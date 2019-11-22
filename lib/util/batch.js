const _ = require('lodash');
const promisify = require('./promisify');
const { isBatch, isQuery } = require('./type-check');

const batchTypes = {
  standard: 0,
  unlogged: 1,
  counter: 2
};

function Batch(db) {
  if (!db) {
    throw new Error('"db" parameter is required');
  }

  this.db = db;
  this.batchType = batchTypes;
  this.context = {
    queries: [],
    options: {},
    errors: [],
    timestamp: null,
    batchType: batchTypes.standard,
    hasChildBatch: false,
    consistencyHierarchy: [
      db.consistencyLevel.all,
      db.consistencyLevel.eachQuorum,
      db.consistencyLevel.quorum,
      db.consistencyLevel.localQuorum,
      db.consistencyLevel.three,
      db.consistencyLevel.two,
      db.consistencyLevel.one,
      db.consistencyLevel.any
    ]
  };
}

Batch.prototype.add = function add(queryOrBatch) {
  if (Array.isArray(queryOrBatch)) {
    queryOrBatch.forEach(this.add.bind(this));
  } else if (isQuery(queryOrBatch)) {
    this.addQuery(queryOrBatch);
  } else if (isBatch(queryOrBatch)) {
    this.addBatch(queryOrBatch);
  } else if (queryOrBatch !== null) {
    this.context.errors.push(`Batch "${JSON.stringify(queryOrBatch)}" is not a valid Priam Batch or Query object. Construct one with #beginBatch() or #beginQuery().`);
  }

  return this;
};

Batch.prototype.addQuery = function addQuery(query) {
  if (!query || !isQuery(query)) {
    this.context.errors.push(`Query "${JSON.stringify(query)}" is not a valid Priam Query object. Construct one with #beginQuery().`);
  } else {
    this.context.queries.push(query);
  }
  return this;
};

Batch.prototype.addBatch = function addBatch(batch) {
  if (!batch || !isBatch(batch)) {
    this.context.errors.push(`Batch "${JSON.stringify(batch)}" is not a valid Priam Batch object. Construct one with #beginBatch().`);
  } else if (!canAddBatch(this, batch)) {
    this.context.errors.push(`Batch "${JSON.stringify(batch)}" contains a child batch that already belongs to the current batch. Unable to add.`);
  } else {
    this.context.hasChildBatch = true;
    this.context.queries.push(batch);
  }
  return this;
};

Batch.prototype.timestamp = function timestamp(clientTimestamp) {
  this.context.timestamp = clientTimestamp || true;
  return this;
};

Batch.prototype.consistency = function consistency(consistencyLevel) {
  if (typeof this.db.consistencyLevel[consistencyLevel] !== 'undefined') {
    this.context.options.consistency = this.db.consistencyLevel[consistencyLevel];
  }
  return this;
};

Batch.prototype.type = function type(batchType) {
  if (typeof batchTypes[batchType] !== 'undefined') {
    this.context.batchType = batchTypes[batchType];
  }
  return this;
};

Batch.prototype.options = function options(optionsDictionary) {
  this.context.options = _.extend(this.context.options, optionsDictionary);
  return this;
};

Batch.prototype.execute = function execute(callback) {
  // If callback not provided, execute as promise
  if (typeof callback !== 'function') {
    return promisify(this.executeBatch, this)();
  }

  // Otherwise, execute callback-style
  return void this.executeBatch(callback);
};

Batch.prototype.executeBatch = function (callback) {
  if (!this.context.queries.length) {
    return void callback(null, []);
  }

  // Build the query batch
  const joined = joinQueries(
    this.context.queries,
    this.context.timestamp,
    this.context.batchType,
    this.context.consistencyHierarchy,
    this.db.config.parsedCqlVersion,
    this.context.hasChildBatch,
    false);

  // Override the detected options with the user-supplied options
  const options = _.extend(joined.options, this.context.options);

  if (!joined.cql) {
    // nothing to execute
    return void callback(null, []);
  }

  this.db.cql(joined.cql, joined.params, options, (err, data) => {
    if (err) {
      this.context.errors.push(err);
    }
    if (this.context.errors.length) {
      return void yieldErrors(this.context.errors, callback);
    }

    callback(null, data);
  });
};

function yieldErrors(errors, callback) {
  if (errors.length === 1) {
    return void callback(errors[0]);
  }
  const e = new Error('Error executing query batch');
  e.inner = errors;
  callback(e);
}

function supportsCql31(version) {
  return version.major > 3 || (version.major === 3 && version.minor >= 1);
}

function containsDeleteOrUpdateStatements(queries) {
  let match = false;
  _.each(queries, function (query) {
    if (match) {
      return;
    }
    if (containsDeleteOrUpdateStatement(query)) {
      match = true;
    }
  });
  return match;
}

const containsDeleteOrUpdateRegex = /^(?:update|delete)\s+/i;
function containsDeleteOrUpdateStatement(query) {
  let match = false;
  if (isBatch(query)) {
    _.each(query.context.queries, function (subQuery) {
      if (match) {
        return;
      }
      if (containsDeleteOrUpdateStatement(subQuery)) {
        match = true;
      }
    });
  } else if (query.context.cql) {
    if (containsDeleteOrUpdateRegex.test(query.context.cql.trimLeft().substring(0, 7))) {
      match = true;
    }
  }
  return match;
}

const endsWithSemicolonRegex = /\s*;\s*$/;
function joinQueries(queries, timestamp, batchType, consistencyHierarchy, cqlVersion, hasChildBatch, isChildBatch) {
  const cql = [];
  const options = {};
  let
    params                  = [],
    consistencyMap          = null,
    hasTimestamp            = false,
    hasQuery                = false,
    supportsNestedTimestamp = typeof cqlVersion === 'boolean' ? cqlVersion : supportsCql31(cqlVersion);
  let queryConsistencyMap;
  if (!isChildBatch) {
    cql.push(`BEGIN ${getBatchTypeString(batchType)}BATCH`);
  }

  if (supportsNestedTimestamp) {
    // Check to see if the batch contains queries that don't support DML-level timestamps
    supportsNestedTimestamp = !containsDeleteOrUpdateStatements(queries);
  }

  if (timestamp !== undefined && timestamp !== null) {
    hasTimestamp = true;
    if (typeof timestamp !== 'number') {
      timestamp = (Date.now() * 1000);
    }

    // If there are any batches in the query, move the USING TIMESTAMP to the end of the individual query.
    // Otherwise, apply at the batch level
    if ((!supportsNestedTimestamp && !isChildBatch) || (!isChildBatch && !hasChildBatch)) {
      cql.push('USING TIMESTAMP ?');
      params.unshift({ value: timestamp, hint: 'bigint' });
    }
  }
  _.each(queries, function (query) {
    let queryIsBatch = false, queryCql;
    if (isBatch(query)) {
      // Add queries from child batch
      queryIsBatch = true;
      const childBatch = joinQueries(
        query.context.queries,
        (hasTimestamp && (typeof query.context.timestamp === 'number' ? query.context.timestamp : (timestamp + 1))) || null,
        query.context.batchType,
        query.context.consistencyHierarchy,
        supportsNestedTimestamp,
        query.context.hasChildBatch,
        true);
      queryCql = childBatch.cql;
      params = params.concat(childBatch.params);
      hasQuery = !!(hasQuery || queryCql);
      query.context.options.consistency = childBatch.options.consistency;
      query.context.options.suppressDebugLog = childBatch.options.suppressDebugLog;
    } else {
      // Add query
      queryCql = query.context.cql;
      const endSemicolonIndex = queryCql && queryCql.lastIndexOf(';');
      if (queryCql && endsWithSemicolonRegex.test(queryCql.substring(endSemicolonIndex))) {
        queryCql = queryCql.substring(0, endSemicolonIndex);
      }
      hasQuery = !!(hasQuery || (queryCql && queryCql.length));
      params = params.concat(query.context.params);
    }

    if (queryCql) {
      if (!queryIsBatch) {
        if (hasTimestamp && supportsNestedTimestamp && (isChildBatch || hasChildBatch)) {
          queryCql += '\nUSING TIMESTAMP ?';
          params.push({ value: timestamp, hint: 'bigint' });
        }
        queryCql += ';';
      }
      cql.push(queryCql);
    }

    // If any of the queries suppresses debug, the batch should also
    if (query.context.options.suppressDebugLog === true) {
      options.suppressDebugLog = true;
    }

    // Apply the most-strict consistency
    if (query.context.options.consistency) {
      queryConsistencyMap = consistencyHierarchy.indexOf(query.context.options.consistency);
      if (consistencyMap === null || (queryConsistencyMap < consistencyMap)) {
        consistencyMap = queryConsistencyMap;
        options.consistency = query.context.options.consistency;
      }
    }
  });
  if (!isChildBatch) {
    cql.push('APPLY BATCH;\n');
  }

  return {
    cql: hasQuery ? cql.join('\n') : null,
    params: params,
    options: options
  };
}

function getBatchTypeString(value) {
  if (value === 0) {
    return '';
  }
  for (const [prop, batchTypeValue] of Object.entries(batchTypes)) {
    if (batchTypeValue === value) {
      return prop.toUpperCase() + ' ';
    }
  }

  return '';
}

function canAddBatch(parentBatch, childBatch) {
  return !containsBatch(parentBatch, childBatch) && !containsBatch(childBatch, parentBatch);
}

function containsBatch(parentBatch, batchToFind) {
  if (parentBatch === batchToFind) {
    return true;
  }
  return _.some(parentBatch.context.queries, function (query) {
    if (isBatch(query)) {
      return containsBatch(query, batchToFind);
    }
    return false;
  });
}

module.exports = Batch;

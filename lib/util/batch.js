"use strict";

var _ = require("lodash")
  , promisify = require("./promisify")
  , Query = require("./query")
  , batchTypes = {
    standard: 0,
    unlogged: 1,
    counter: 2
  };

function Batch(db) {
  if (!db) {
    throw new Error("'db' parameter is required");
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

Batch.prototype.addQuery = function addQuery(query) {
  if (!query || !(query instanceof Query)) {
    this.context.errors.push("Query '" + JSON.stringify(query) + "' is not a valid Priam Query object. Construct one with #beginQuery().");
  }
  else {
    this.context.queries.push(query);
  }
  return this;
};

Batch.prototype.addBatch = function addBatch(batch) {
  if (!batch || !(batch instanceof Batch)) {
    this.context.errors.push("Batch '" + JSON.stringify(batch) + "' is not a valid Priam Batch object. Construct one with #beginBatch().");
  }
  else if (!canAddBatch(this, batch)) {
    this.context.errors.push("Batch '" + JSON.stringify(batch) + "' contains a child batch that already belongs to the current batch. Unable to add.");
  }
  else {
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
  if (typeof this.db.consistencyLevel[consistencyLevel] !== "undefined") {
    this.context.options.consistency = this.db.consistencyLevel[consistencyLevel];
  }
  return this;
};

Batch.prototype.type = function type(batchType) {
  if (typeof batchTypes[batchType] !== "undefined") {
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
  if (typeof callback !== "function") {
    return promisify(executeBatch, this)();
  }

  // Otherwise, execute callback-style
  return void executeBatch.call(this, callback);
};

function yieldErrors(errors, callback) {
  if (errors.length === 1) {
    return void callback(errors[0]);
  }
  var e = new Error("Error executing query batch");
  e.inner = errors;
  callback(e);
}

var endsWithSemicolonRegex = new RegExp(/\s*;\s*$/);
function joinQueries(queries, timestamp, batchType, consistencyHierarchy, hasChildBatch, isChildBatch) {
  var cql = []
    , params = []
    , options = {}
    , consistencyMap = null
    , hasTimestamp = false
    , queryConsistencyMap;

  if (!isChildBatch) {
    cql.push("BEGIN " + getBatchTypeString(batchType) + "BATCH");
  }
  if (timestamp !== undefined && timestamp !== null) {
    hasTimestamp = true;
    if (typeof timestamp !== "number") {
      timestamp = (Date.now() * 1000);
    }
    // If there are any batches in the query, move the USING TIMESTAMP to the end of the individual query
    if (!isChildBatch && !hasChildBatch) {
      cql.push("USING TIMESTAMP " + timestamp.toString());
    }
  }
  _.each(queries, function (query) {
    var isBatch = false
      , queryCql;
    if (query instanceof Batch) {
      // Add queries from child batch
      isBatch = true;
      var childBatch = joinQueries(
        query.context.queries,
        (hasTimestamp && (typeof query.context.timestamp === "number" ? query.context.timestamp : (timestamp + 1))) || null,
        query.context.batchType,
        query.context.consistencyHierarchy,
        query.context.hasChildBatch,
        true);
      queryCql = childBatch.cql;
      params = childBatch.params.concat(params);
      query.context.options.consistency = childBatch.options.consistency;
      query.context.options.suppressDebugLog = childBatch.options.suppressDebugLog;
    }
    else {
      // Add query
      queryCql = query.context.cql;
      if (endsWithSemicolonRegex.test(queryCql)) {
        queryCql = queryCql.substring(0, queryCql.lastIndexOf(";"));
      }
      params = params.concat(query.context.params);
    }

    if (!isBatch) {
      if (hasTimestamp && (isChildBatch || hasChildBatch)) {
        queryCql += " USING TIMESTAMP " + timestamp.toString() + ";";
      }
      else {
        queryCql += ";";
      }
    }
    cql.push(queryCql);

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
    cql.push("APPLY BATCH;\n");
  }

  return {
    cql: cql.join("\n"),
    params: params,
    options: options
  };
}

function getBatchTypeString(value) {
  if (value === 0) {
    return "";
  }
  var prop;
  for (prop in batchTypes) {
    /* istanbul ignore else: not easily tested and no real benefit to doing so */
    if (batchTypes.hasOwnProperty(prop)) {
      if (batchTypes[prop] === value) {
        return prop.toUpperCase() + " ";
      }
    }
  }
  return "";
}

function executeBatch(callback) {
  /*jshint validthis: true */
  var self = this,
    joined, options;

  if (!self.context.queries.length) {
    self.context.errors.push("#statement() should be called prior to #execute()");
    return void yieldErrors(self.context.errors, callback);
  }

  // Build the query batch
  joined = joinQueries(
    self.context.queries,
    self.context.timestamp,
    self.context.batchType,
    self.context.consistencyHierarchy,
    self.context.hasChildBatch,
    false);

  // Override the detected options with the user-supplied options
  options = _.extend(joined.options, self.context.options);

  self.db.cql(
    joined.cql, joined.params, options,
    function executeQueryCallback(err, data) {
      if (err) {
        self.context.errors.push(err);
      }
      if (self.context.errors.length) {
        return void yieldErrors(self.context.errors, callback);
      }

      callback(null, data);
    });
}

function canAddBatch(parentBatch, childBatch) {
  return !containsBatch(parentBatch, childBatch) && !containsBatch(childBatch, parentBatch);
}

function containsBatch(parentBatch, batchToFind) {
  if (parentBatch === batchToFind) { return true; }
  return _.any(parentBatch.context.queries, function (query) {
    if (query instanceof Batch) { return containsBatch(query, batchToFind); }
    return false;
  });
}

exports = module.exports = Batch;
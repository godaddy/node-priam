"use strict";

var _ = require("lodash"),
  promisify = require("./promisify"),
  Query = require("./query"),
  batchTypes = {
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

Batch.prototype.timestamp = function timestamp(clientTimestamp) {
  this.context.timestamp = clientTimestamp || (Date.now() * 1000);
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
function joinQueries(queries, timestamp, batchType, consistencyHierarchy) {
  // TODO: Support batching prepared statements when helenus/node-data-cassandra support them
  //       http://www.datastax.com/dev/blog/client-side-improvements-in-cassandra-2-0.

  var cql = [],
    params = [],
    options = {},
    consistencyMap = null,
    queryConsistencyMap;

  cql.push("BEGIN " + getBatchTypeString(batchType) + "BATCH");
  if (timestamp) {
    cql.push("USING TIMESTAMP " + timestamp.toString());
  }
  _.each(queries, function (query) {
    if (!endsWithSemicolonRegex.test(query.context.cql)) {
      cql.push(query.context.cql + ";");
    }
    else {
      cql.push(query.context.cql);
    }
    params = params.concat(query.context.params);

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
  cql.push("APPLY BATCH;\n");

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
    self.context.consistencyHierarchy);

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

exports = module.exports = Batch;
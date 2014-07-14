"use strict";

var _ = require("lodash"),
  promisify = require("./promisify");

function Query(db) {
  if (!db) {
    throw new Error("'db' parameter is required");
  }

  this.db = db;
  this.context = {
    cql: null,
    params: [],
    options: {},
    errors: []
  };
}

Query.prototype.query = function query(cql) {
  this.context.cql = cql;
  return this;
};

Query.prototype.namedQuery = function namedQuery(queryName) {
  var self = this;

  if (!self.db.queryCache) {
    self.context.errors.push(new Error("'queryDirectory' driver option must be set in order to use #namedQuery()"));
  }
  else {
    self.db.queryCache.readQuery(
      queryName,
      function (err, queryText) {
        if (err) {
          return void self.context.errors.push(err);
        }

        addIfNotExists(self.context.options, "queryName", queryName);
        if (self.db.poolConfig.supportsPreparedStatements) {
          addIfNotExists(self.context.options, "executeAsPrepared", true);
        }

        self.context.cql = queryText;
      });
  }

  return self;
};

Query.prototype.param = function param(value, hint) {
  this.context.params.push(this.db.param(value, hint));
  return this;
};

Query.prototype.params = function params(parameters) {
  var self = this;
  parameters.forEach(function (parameter) {
    self.context.params.push(parameter);
  });
  return self;
};

Query.prototype.consistency = function consistency(consistencyLevel) {
  if (typeof this.db.consistencyLevel[consistencyLevel] !== "undefined") {
    this.context.options.consistency = this.db.consistencyLevel[consistencyLevel];
  }
  return this;
};

Query.prototype.options = function options(optionsDictionary) {
  this.context.options = _.extend(this.context.options, optionsDictionary);
  return this;
};

Query.prototype.execute = function execute(callback) {
  // If callback not provided, execute as promise
  if (typeof callback !== "function") {
    return promisify(executeQuery, this)();
  }

  // Otherwise, execute callback-style
  return void executeQuery.call(this, callback);
};

function yieldErrors(errors, callback) {
  if (errors.length === 1) {
    return void callback(errors[0]);
  }
  var e = new Error("Error executing query");
  e.inner = errors;
  callback(e);
}

function executeQuery(callback) {
  /*jshint validthis: true */
  var self = this;

  if (!self.context.cql) {
    self.context.errors.push("either #query() or #namedQuery() should be called prior to #execute()");
    return void yieldErrors(self.context.errors, callback);
  }

  self.db.cql(
    self.context.cql, self.context.params, self.context.options,
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

function addIfNotExists(dictionary, key, value) {
  if (typeof dictionary[key] === "undefined" || dictionary[key] === null) {
    dictionary[key] = value;
  }
}

exports = module.exports = Query;
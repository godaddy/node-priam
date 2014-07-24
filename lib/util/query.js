'use strict';

var _ = require('lodash'),
  promisify = require("./promisify");

function Query(db, context) {
  if (!db) {
    throw new Error('"db" parameter is required');
  }

  this.db = db;
  this.context = _.assign({}, {
    cql: null,
    params: [],
    options: {},
    errors: [],
    single: false,
    first: false,
    resultTransformers: []
  }, context);
}

Query.prototype.single = function single() {
  this.context.single = true;
  this.context.first = false;
  return this;
};

Query.prototype.first = function single() {
  this.context.first = true;
  this.context.single = false;
  return this;
};

Query.prototype.all = function all() {
  this.context.single = false;
  this.context.first = false;
  return this;
};

Query.prototype.query = function query(cql) {
  this.context.cql = cql;
  return this;
};

Query.prototype.namedQuery = function namedQuery(queryName) {
  var self = this;

  if (!self.db.queryCache) {
    self.context.errors.push(new Error('"queryDirectory" driver option must be set in order to use #namedQuery()'));
  }
  else {
    self.db.queryCache.readQuery(
      queryName,
      function (err, queryText) {
        if (err) {
          return void self.context.errors.push(err);
        }

        addIfNotExists(self.context.options, 'queryName', queryName);
        if (self.db.poolConfig.supportsPreparedStatements) {
          addIfNotExists(self.context.options, 'executeAsPrepared', true);
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
  Array.prototype.push.apply(this.context.params, parameters);
  return this;
};

Query.prototype.consistency = function consistency(consistencyLevel) {
  if (typeof this.db.consistencyLevel[consistencyLevel] !== 'undefined') {
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
  if (typeof callback !== 'function') {
    return promisify(executeQuery, this)();
  }

  // Otherwise, execute callback-style
  return void executeQuery.call(this, callback);
};

Query.prototype.addResultTransformer = function addResultTransformer(fn){
  this.context.resultTransformers.push(fn);
  return this;
};

Query.prototype.clearResultTransformers = function clearResultTransformers(){
  this.context.resultTransformers = [];
  return this;
};

function yieldErrors(errors, callback) {
  if (errors.length === 1) {
    return void callback(errors[0]);
  }
  var e = new Error('Error executing query');
  e.inner = errors;
  callback(e);
}

function executeQuery(callback) {
  /*jshint validthis: true */
  var self = this;

  if (!self.context.cql) {
    self.context.errors.push('either #query() or #namedQuery() should be called prior to #execute()');
    return void yieldErrors(self.context.errors, callback);
  }

  self.context.options.resultTransformers = self.context.resultTransformers;

  self.db.cql(
    self.context.cql, self.context.params, self.context.options,
    function executeQueryCallback(err, data) {
      if (err) {
        self.context.errors.push(err);
      } else if (data && data.length) {
        if (self.context.single && data.length > 1) {
          self.context.errors.push(new Error('More than one result returned'));
        }
      }
      if (self.context.errors.length) {
        return void yieldErrors(self.context.errors, callback);
      }
      if (data && data.length && (self.context.single || self.context.first)) {
        return void callback(null, data[0]);
      }
      callback(null, data);
    });
}

function addIfNotExists(dictionary, key, value) {
  if (typeof dictionary[key] === 'undefined' || dictionary[key] === null) {
    dictionary[key] = value;
  }
}

module.exports = Query;

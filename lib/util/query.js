const { PassThrough } = require('stream');
const _               = require('lodash');
const isStream        = require('isstream');
const promisify       = require('./promisify');

class Query {
  constructor(db, context) {
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

  single() {
    this.context.single = true;
    this.context.first = false;
    return this;
  }

  first() {
    this.context.first = true;
    this.context.single = false;
    return this;
  }

  all() {
    this.context.single = false;
    this.context.first = false;
    return this;
  }

  query(cql) {
    this.context.cql = cql;
    return this;
  }

  namedQuery(queryName) {
    var self = this;

    if (!self.db.queryCache) {
      self.context.errors.push(new Error('"queryDirectory" driver option must be set in order to use #namedQuery()'));
    } else {
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
  }

  param(value, hint, isRoutingKey) {
    var p = this.db.param(value, hint, isRoutingKey);
    this.context.params.push(p);
    return this;
  }

  params(parameters) {
    Array.prototype.push.apply(this.context.params, parameters);
    return this;
  }

  consistency(consistencyLevel) {
    if (typeof this.db.consistencyLevel[consistencyLevel] !== 'undefined') {
      this.context.options.consistency = this.db.consistencyLevel[consistencyLevel];
    }
    return this;
  }

  options(optionsDictionary) {
    this.context.options = _.extend(this.context.options, optionsDictionary);
    return this;
  }

  execute(callback) {
    // If callback not provided, execute as promise
    if (typeof callback !== 'function') {
      return promisify(this.executeQuery, this)();
    }

    // Otherwise, execute callback-style
    return void this.executeQuery(callback);
  }

  // Return a stream from the underlying driver for the given query
  stream() {
    // Create a stream that we will write to when we
    // have access to the connection pool
    const stream = new PassThrough({ objectMode: true });

    // Pass the stream to have data added to it when it exists
    this.executeQuery(stream);

    return stream;
  }

  addResultTransformer(fn) {
    this.context.resultTransformers.push(fn);
    return this;
  }

  clearResultTransformers() {
    this.context.resultTransformers = [];
    return this;
  }

  executeQuery(callback) {
    var isStreaming = isStream(callback);

    if (!this.context.cql) {
      this.context.errors.push('either #query() or #namedQuery() should be called prior to #execute()');
      return void yieldErrors(this.context.errors, callback);
    }

    this.context.options.resultTransformers = this.context.resultTransformers;

    // Pass the stream back to the function in this case
    return isStreaming
      ? this.db.cql(this.context.cql, this.context.params, this.context.options, callback)
      : this.db.cql(this.context.cql, this.context.params, this.context.options, (err, data) => {
        if (err) {
          this.context.errors.push(err);
        } else if (data && data.length) {
          if (this.context.single && data.length > 1) {
            this.context.errors.push(new Error('More than one result returned'));
          }
        }
        if (this.context.errors.length) {
          return void yieldErrors(this.context.errors, callback);
        }
        if (Array.isArray(data) && (this.context.single || this.context.first)) {
          if (data.length) {
            return void callback(null, data[0]);
          }
          return void callback(null, null);
        }

        callback(null, data);
      });
  }
}

function yieldErrors(errors, callback) {
  var e;
  if (errors.length === 1) {
    e = errors[0];
  }

  if (!e) {
    e = new Error('Error executing query');
    e.inner = errors;
  }

  return process.nextTick(function () {
    return isStream(callback)
      ? callback.emit('error', e)
      : callback(e);
  });
}

function addIfNotExists(dictionary, key, value) {
  if (typeof dictionary[key] === 'undefined' || dictionary[key] === null) {
    dictionary[key] = value;
  }
}

module.exports = Query;

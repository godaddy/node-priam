const { EventEmitter }      = require('events');
const util                  = require('util');
const fs                    = require('fs');
const path                  = require('path');
const DEFAULT_POLL_INTERVAL = 30000; // 30 sec

function SampleResolver() {
  if (!(this instanceof SampleResolver)) {
    return new SampleResolver();
  }
  this.localCache = {};
  this.pollHandle = null;
  EventEmitter.call(this);
}
util.inherits(SampleResolver, EventEmitter);

SampleResolver.prototype.resolveConnection = function resolveConnection(config, callback) {
  if (!this.localCache || !this.localCache.cache || !this.pollHandle) {

    // clear any old polling
    this.stopPolling();

    // initial call to resolve connection
    this.localCache.config = {
      /* Resolver configuration options here .. e.g. web service url */
      pollInterval: config.pollInterval
    };
    config = this.localCache.config;

    // set up polling
    let pollInterval = config.pollInterval || DEFAULT_POLL_INTERVAL;
    if (pollInterval <= 0) {
      pollInterval = DEFAULT_POLL_INTERVAL;
    }
    config.pollInterval = pollInterval;
    this.pollHandle = setInterval(() => {
      this.fetch(this.localCache.config, null);
    }, pollInterval);
    this.pollHandle.unref();

    // retrieve credentials
    this.fetch(config, callback);
  } else {
    // retrieve from cache
    callback(null, this.localCache.cache);
  }
};

SampleResolver.prototype.fetch = function fetch(config, callback) {
  this.emit('fetching', config);

  /* Make some web service call or something similar here .. for demo purposes load from file */
  fs.readFile(path.join(__dirname, 'credentials.json'), this.resolveConnectionCallback.bind(this, callback));
};

SampleResolver.prototype.resolveConnectionCallback = function resolveConnectionCallback(callback, err, record) {
  if (!record && !err) {
    err = new Error('no credential record returned from credential store');
  }
  if (err) {
    this.emit('fetch', err);
    if (typeof callback === 'function') {
      callback(err);
    }
    return;
  }

  let parseError = null, configData;
  try {
    configData = JSON.parse(record.toString('utf8'));
    this.localCache.cache = configData;
  } catch (e) {
    parseError = e;
    configData = undefined;
  }

  this.emit('fetch', parseError, configData);
  if (typeof callback === 'function') {
    callback(parseError, configData);
  }
};

SampleResolver.prototype.stopPolling = function stopPolling() {
  if (this.pollHandle) {
    clearInterval(this.pollHandle);
    this.localCache.config = null;
    this.pollHandle = null;
  }
};

module.exports = SampleResolver;

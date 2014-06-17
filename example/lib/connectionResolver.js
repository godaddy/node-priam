"use strict";

var EventEmitter = require("events").EventEmitter,
  util = require("util"),
  fs = require("fs"),
  path = require("path"),
  DEFAULT_POLL_INTERVAL = 30000; // 30 sec

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
  var self = this;

  if (!self.localCache || !self.localCache.cache || !self.pollHandle) {

    // clear any old polling
    self.stopPolling();

    // initial call to resolve connection
    self.localCache.config = {
      /* Resolver configuration options here .. e.g. web service url */
      pollInterval: config.pollInterval
    };
    config = self.localCache.config;

    // set up polling
    var pollInterval = config.pollInterval || DEFAULT_POLL_INTERVAL;
    if (pollInterval <= 0) {
      pollInterval = DEFAULT_POLL_INTERVAL;
    }
    config.pollInterval = pollInterval;
    self.pollHandle = setInterval(function pollResolver() {
      self.fetch(self.localCache.config, null);
    }, pollInterval);
    self.pollHandle.unref();

    // retrieve credentials
    self.fetch(config, callback);
  }
  else {
    // retrieve from cache
    callback(null, self.localCache.cache);
  }
};

SampleResolver.prototype.fetch = function fetch(config, callback) {
  var self = this;

  self.emit('fetching', config);

  /* Make some web service call or something similar here .. for demo purposes load from file */
  fs.readFile(path.join(__dirname, "credentials.json"), self.resolveConnectionCallback.bind(self, callback));
};

SampleResolver.prototype.resolveConnectionCallback = function resolveConnectionCallback(callback, err, record) {
  var self = this;
  if (!record && !err) {
    err = new Error("no credential record returned from credential store");
  }
  if (err) {
    self.emit('fetch', err);
    if (typeof callback === "function") {
      callback(err);
    }
    return;
  }

  var parseError = null,
    configData;
  try {
    configData = JSON.parse(record.toString("utf8"));
    self.localCache.cache = configData;
  }
  catch (e) {
    parseError = e;
    configData = undefined;
  }

  self.emit('fetch', parseError, configData);
  if (typeof callback === "function") {
    callback(parseError, configData);
  }
};

SampleResolver.prototype.stopPolling = function stopPolling() {
  var self = this;
  if (self.pollHandle) {
    clearInterval(self.pollHandle);
    self.localCache.config = null;
    self.pollHandle = null;
  }
};

module.exports = SampleResolver;
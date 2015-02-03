'use strict';

var fs = require('fs')
  , path = require('path')
  , fileCache = {};

function QueryCache(options) {
  if (!options || !options.queryDirectory) {
    throw new Error('QueryCache#ctor(): "queryDirectory" option is required.');
  }

  this.queryDirectory = options.queryDirectory;

  var requiresInit = false;
  if (fileCache[this.queryDirectory]) {
    this.fileCache = fileCache[this.queryDirectory];
  }
  else {
    this.fileCache = {};
    fileCache[this.queryDirectory] = this.fileCache;
    requiresInit = true;
  }

  if (requiresInit || options.forceQueryInit) {
    this.initQueries();
  }
}

QueryCache.prototype.initQueries = function initQueries() {
  var queryDir = this.queryDirectory
    , fileCache = this.fileCache
    , files = fs.readdirSync(queryDir);
  files.forEach(function (filePath) {
    var fileContent = fs.readFileSync(path.join(queryDir, filePath), "utf8").replace(/^\uFEFF/, ''); // remove BOM
    fileCache[path.basename(filePath, ".cql")] = fileContent;
  });
};

QueryCache.prototype.readQuery = function readQuery(queryName, callback) {
  var queryDir = this.queryDirectory
    , fileCache = this.fileCache;

  if (typeof callback !== 'function') {
    throw new Error('QueryCache#readQuery(): "callback" is required');
  }
  if (fileCache[queryName] && (typeof callback === 'function')) {
    return void callback(null, fileCache[queryName]);
  }

  callback(new Error('QueryCache#readQuery(): Unable to find named query: ' + queryName));
};

exports = module.exports = QueryCache;
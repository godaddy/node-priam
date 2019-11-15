const fs        = require('fs');
const path      = require('path');

const globalFileCache = {};

class QueryCache {
  constructor(options) {
    if (!options || !options.queryDirectory) {
      throw new Error('QueryCache#ctor(): "queryDirectory" option is required.');
    }

    this.queryDirectory = options.queryDirectory;

    let requiresInit = false;
    if (globalFileCache[this.queryDirectory]) {
      this.fileCache = globalFileCache[this.queryDirectory];
    } else {
      this.fileCache = {};
      globalFileCache[this.queryDirectory] = this.fileCache;
      requiresInit = true;
    }

    if (requiresInit || options.forceQueryInit) {
      this.initQueries();
    }
  }

  initQueries() {
    const queryDir  = this.queryDirectory;
    const files     = fs.readdirSync(queryDir);
    files.forEach(filePath => {
      const fileContent = fs.readFileSync(path.join(queryDir, filePath), 'utf8').replace(/^\uFEFF/, ''); // remove BOM
      this.fileCache[path.basename(filePath, '.cql')] = fileContent;
    });
  }

  readQuery(queryName) {
    const fileCache = this.fileCache;

    if (fileCache[queryName]) {
      return fileCache[queryName];
    }

    throw new Error('QueryCache#readQuery(): Unable to find named query: ' + queryName);
  }
}

module.exports = QueryCache;

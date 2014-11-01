'use strict';

/**
  Migrated from https://github.com/jorgebay/node-cassandra-cql/blob/protocol1/lib/types.js
  for backwards-compatibility with Jorge Gondra's node-cassandra-cql driver.
 */

var stringifier = require('./string-encoder').stringifyValue;

var queryParser = {
  /**
   * Replaced the query place holders with the stringified value
   * @param {String} query
   * @param {Array} params
   * @param {Function} stringifier
   */
  parse: function (query, params) {
    if (!query || !query.length || !params) {
      return query;
    }
    var parts = [];
    var isLiteral = false;
    var lastIndex = 0;
    var paramsCounter = 0;
    for (var i = 0; i < query.length; i++) {
      var char = query.charAt(i);
      if (char === "'" && query.charAt(i-1) !== '\\') {
        //opening or closing quotes in a literal value of the query
        isLiteral = !isLiteral;
      }
      if (!isLiteral && char === '?') {
        //is a placeholder
        parts.push(query.substring(lastIndex, i));
        parts.push(stringifier(params[paramsCounter++]));
        lastIndex = i+1;
      }
    }
    parts.push(query.substring(lastIndex));
    return parts.join('');
  }
};

module.exports = queryParser;
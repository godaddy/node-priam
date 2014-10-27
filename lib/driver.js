'use strict';

var parseVersion = require('./util/parse-version');

function Driver(context) {

  context = context || {};
  context.config = context.config || {};
  context.config.version = context.config.cqlVersion = context.config.cqlVersion || context.config.version || '3.1.0';
  var version = context.config.parsedCqlVersion = parseVersion(context.config.cqlVersion);
  var driver = context.config.driver || 'datastax';

  var protocol = 'binary';
  if (driver === 'helenus' || driver === 'thrift' || version.major < 3) {
    // thrift is only supported by Helenus driver
    protocol = 'thrift';
  }
  context.config.protocol = protocol;

  if (protocol === 'thrift') {
    context.config.driver = 'helenus';
    return new (require('./drivers/helenus'))(context);
  }
  else {
    return new (require('./drivers/datastax'))(context);
  }
}

module.exports = function (context) {
  return Driver(context);
};

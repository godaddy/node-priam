'use strict';

var parseVersion = require('./util/parse-version');

function Driver(context) {

  context = context || {};
  context.config = context.config || {};
  context.config.version = context.config.cqlVersion = context.config.cqlVersion || context.config.version || '3.1.0';
  context.config.parsedCqlVersion = parseVersion(context.config.cqlVersion);
//  var driver = context.config.driver || 'datastax';

  var protocol = context.config.protocol || 'binary';

  context.config.protocol = protocol;


  return new (require('./drivers/datastax'))(context);
}

module.exports = function (context) {
  return Driver(context);
};
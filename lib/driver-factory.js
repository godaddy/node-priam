'use strict';

var parseVersion = require('./util/parse-version');
var Driver = require('./driver');
var cqlTypes = require('./util/cql-types');

function DriverFactory(context) {
  context = context || {};
  context.config = context.config || {};
  context.config.version = context.config.cqlVersion = context.config.cqlVersion || context.config.version || '3.1.0';
  context.config.parsedCqlVersion = parseVersion(context.config.cqlVersion);

  var protocol = context.config.protocol || 'binary';
  context.config.protocol = protocol;

  return new Driver(context);
}

module.exports = function (context) {
  return DriverFactory(context);
};

module.exports.valueTypes = cqlTypes.valueTypes;
module.exports.dataTypes = cqlTypes.dataTypes;
module.exports.consistencies = cqlTypes.consistencies;

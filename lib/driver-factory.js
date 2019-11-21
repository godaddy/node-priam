const parseVersion = require('./util/parse-version');
const Driver = require('./driver');
const cqlTypes = require('./util/cql-types');

function driverFactory(context) {
  context = context || {};
  context.config = context.config || {};
  const {
    protocolOptions: {
      maxVersion = '3.1.0'
    } = {}
  } = context.config;
  context.config.version = maxVersion;
  context.config.parsedCqlVersion = parseVersion(maxVersion);

  return new Driver(context);
}

module.exports = function (context) {
  return driverFactory(context);
};

module.exports.valueTypes = cqlTypes.valueTypes;
module.exports.dataTypes = cqlTypes.dataTypes;
module.exports.consistencies = cqlTypes.consistencies;

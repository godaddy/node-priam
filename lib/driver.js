'use strict';

var parseVersion = require('./util/parse-version');

function Driver(context) {

  context = context || {};
  context.config = context.config || {};
  context.config.version = context.config.cqlVersion = context.config.cqlVersion || context.config.version || '3.1.0';
  var version = context.config.parsedCqlVersion = parseVersion(context.config.cqlVersion);
  var driver = context.config.driver || 'node-cassandra-cql';

  var protocol = 'binaryV2';
  if (driver === 'helenus' || driver === 'thrift' || version.major < 3) {
    // thrift is only supported by Helenus driver, and binary is not supported until Cassandra 1.2 (CQL3)
    protocol = 'thrift';
  }
  else if (version.major < 3 || (version.major === 3 && version.minor < 1)) {
    // binaryV2 protocol supported in latest node-cassandra-cql did not exist until Cassandra 2.0 (CQL3.1)
    protocol = 'binaryV1';
  }
  context.config.protocol = protocol;

  if (protocol === 'thrift') {
    context.config.driver = 'helenus';
    return new (require('./drivers/helenus'))(context);
  }
  else {
    context.config.driver = (protocol === 'binaryV2' ? 'node-cassandra-cql' : 'priam-cassandra-cql');
    return new (require('./drivers/node-cassandra-cql'))(context);
  }
}

module.exports = function (context) {
  return Driver(context);
};

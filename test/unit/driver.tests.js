'use strict';

var sinon = require('sinon')
  , chai = require('chai')
  , util = require('util')
  , assert = chai.assert
  , expect = chai.expect
  , Driver = require('../../lib/driver')
  , helenus = require('../../lib/drivers/helenus')
  , cassandraCql = require('../../lib/drivers/node-cassandra-cql')
  , parseVersion = require('../../lib/util/parse-version');

describe('lib/driver.js', function () {

  describe('interface', function () {

    it('is a constructor function', function () {
      assert.strictEqual(typeof Driver, 'function', 'exports a constructor function');
    });

    it('returns latest node-cassandra-cql driver by default', function () {
      // arrange
      var context = null;
      var parsed = parseVersion('3.1.0');

      // act
      var driver = Driver(context);

      // assert
      assert.strictEqual(driver.config.version, '3.1.0');
      assert.deepEqual(driver.config.parsedCqlVersion, parsed);
      assert.strictEqual(driver.config.driver, 'node-cassandra-cql');
      assert.strictEqual(driver.config.protocol, 'binaryV2');
      assert.instanceOf(driver, cassandraCql.NodeCassandraDriver);
    });

    it('returns helenus driver if specified as "helenus" and null cqlVersion', function () {
      testInstance('helenus', 'cqlVersion', null, 'helenus', '3.1.0', 'thrift', helenus.HelenusDriver);
    });

    it('returns helenus driver if specified as "thrift" and null cqlVersion', function () {
      testInstance('thrift', 'cqlVersion', null, 'helenus', '3.1.0', 'thrift', helenus.HelenusDriver);
    });

    it('returns helenus driver if cqlVersion is less than 3', function () {
      testInstance('node-cassandra-cql', 'cqlVersion', '2.0.0', 'helenus', '2.0.0', 'thrift', helenus.HelenusDriver);
    });

    it('returns priam-cassandra-cql driver if cqlVersion is less than 3.1', function () {
      testInstance('node-cassandra-cql', 'cqlVersion', '3.0.0', 'priam-cassandra-cql', '3.0.0', 'binaryV1', cassandraCql.NodeCassandraDriver);
    });

    it('returns node-cassandra-cql driver if cqlVersion is greater than 3.1', function () {
      testInstance('node-cassandra-cql', 'cqlVersion', '3.2.0', 'node-cassandra-cql', '3.2.0', 'binaryV2', cassandraCql.NodeCassandraDriver);
    });

    it('returns helenus driver if specified as "helenus" and null version', function () {
      testInstance('helenus', 'version', null, 'helenus', '3.1.0', 'thrift', helenus.HelenusDriver);
    });

    it('returns helenus driver if specified as "thrift" and null version', function () {
      testInstance('thrift', 'version', null, 'helenus', '3.1.0', 'thrift', helenus.HelenusDriver);
    });

    it('returns helenus driver if version is less than 3', function () {
      testInstance('node-cassandra-cql', 'version', '2.0.0', 'helenus', '2.0.0', 'thrift', helenus.HelenusDriver);
    });

    it('returns priam-cassandra-cql driver if version is greater than 3.1', function () {
      testInstance('node-cassandra-cql', 'version', '3.0.0', 'priam-cassandra-cql', '3.0.0', 'binaryV1', cassandraCql.NodeCassandraDriver);
    });

    it('returns node-cassandra-cql driver if version is greater than 3.1', function () {
      testInstance('node-cassandra-cql', 'version', '3.2.0', 'node-cassandra-cql', '3.2.0', 'binaryV2', cassandraCql.NodeCassandraDriver);
    });

    function testInstance(driver, versionPath, cqlVersion, expectedDriver, expectedVersion, expectedProtocol, expectedInstance) {
      // arrange
      var context = {
        config: {
          driver: driver
        }
      };
      context.config[versionPath] = cqlVersion;
      var parsed = parseVersion(expectedVersion);

      // act
      var instance = Driver(context);
      var config = instance.config;

      // assert
      assert.strictEqual(config.cqlVersion || config.version, expectedVersion); /* helenus is cqlVersion, node-cass-cql is version */
      assert.deepEqual(config.parsedCqlVersion, parsed);
      assert.strictEqual(config.driver, expectedDriver);
      assert.strictEqual(config.protocol, expectedProtocol);
      assert.instanceOf(instance, expectedInstance);
    }

  });

});

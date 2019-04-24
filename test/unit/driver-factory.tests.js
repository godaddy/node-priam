var
  chai         = require('chai'),
  assert       = chai.assert,
  expect       = chai.expect,
  DriverFactory       = require('../../lib/driver-factory'),
  Driver     = require('../../lib/driver'),
  parseVersion = require('../../lib/util/parse-version');

describe('lib/driver-factory.js', function () {

  describe('interface', function () {

    it('is a constructor function', function () {
      assert.strictEqual(typeof DriverFactory, 'function', 'exports a constructor function');
    });

    it('returns latest datastax driver by default', function () {
      // arrange
      var context = null;
      var parsed = parseVersion('3.1.0');

      // act
      var driver = DriverFactory(context);

      // assert
      assert.strictEqual(driver.config.version, '3.1.0');
      assert.deepEqual(driver.config.parsedCqlVersion, parsed);
      assert.strictEqual(driver.config.protocol, 'binary');
      assert.instanceOf(driver, Driver.DatastaxDriver);
    });

    it('returns datastax driver set to version 3.0 if cqlVersion is equal to 3.0', function () {
      testInstance('datastax', 'cqlVersion', '3.0.0', 'datastax', '3.0.0', 'binary', Driver.DatastaxDriver);
    });

    it('exposes DataStax types and consistencies', function () {
      expect(DriverFactory.consistencies).to.be.an('object');
      expect(DriverFactory.dataTypes).to.be.an('object');
      expect(DriverFactory.valueTypes).to.be.an('object');
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
      var instance = DriverFactory(context);
      var config = instance.config;

      // assert
      assert.strictEqual(config.cqlVersion || config.version, expectedVersion);
      /* helenus is cqlVersion, node-cass-cql is version */
      assert.deepEqual(config.parsedCqlVersion, parsed);
      assert.strictEqual(config.protocol, expectedProtocol);
      assert.instanceOf(instance, expectedInstance);
    }

  });

});

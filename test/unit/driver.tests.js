'use strict';

var sinon = require('sinon'),
  chai = require('chai'),
  util = require('util'),
  assert = chai.assert,
  expect = chai.expect;

var Driver = require('../../lib/driver'),
  helenus = require('../../lib/drivers/helenus'),
  cassandraCql = require('../../lib/drivers/node-cassandra-cql');

describe('lib/driver.js', function () {

  describe('interface', function () {

    it('is a constructor function', function () {
      assert.strictEqual(typeof Driver, 'function', 'exports a constructor function');
    });

    it('returns cassandra-cql driver by default', function () {
      // arrange
      var context = {
        config: {
        }
      };

      // act
      var driver = Driver(context);

      // assert
      assert.ok(driver instanceof cassandraCql.NodeCassandraDriver);
    });

    it('returns helenus driver if specified', function () {
      // arrange
      var context = {
        config: {
          driver: 'helenus'
        }
      };

      // act
      var driver = Driver(context);

      // assert
      assert.ok(driver instanceof helenus.HelenusDriver);
    });

  });

});

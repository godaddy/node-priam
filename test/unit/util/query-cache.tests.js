'use strict';

var sinon = require('sinon'),
  chai = require('chai'),
  assert = chai.assert,
  expect = chai.expect,
  path = require('path');

var QueryCache = require('../../../lib/util/query-cache'),
  queryDir = path.join(__dirname, '../../stubs/cql');

describe('lib/util/query-cache.js', function () {

  describe('interface', function () {

    it('is a constructor function', function () {
      assert.strictEqual(typeof QueryCache, 'function', 'is a constructor function');
    });

    it('throws error if options not provided', function (done) {
      // act
      expect(function () {
        var qc = new QueryCache();
      }).to.throw(Error);

      done();
    });

    it('throws error if queryDirectory not provided', function (done) {
      // act
      expect(function () {
        var qc = new QueryCache({});
      }).to.throw(Error);

      done();
    });
  });

  describe('constructed instance', function () {

    var queries;

    beforeEach(function () {
      queries = new QueryCache({ queryDirectory: queryDir });
    });

    function validateFunctionExists(name, argCount) {
      // assert
      assert.strictEqual(typeof queries[name], 'function');
      assert.strictEqual(queries[name].length, argCount, name + ' takes ' + argCount + ' arguments');
    }

    it('provides a readQuery function', function () {
      validateFunctionExists('readQuery', 2);
    });

    it('pre-initializes the query cache', function () {
      assert.typeOf(queries.fileCache, 'object');
      assert.isDefined(queries.fileCache.myFakeCql, 'myFakeCql is loaded');
    });

  });

  describe('#readQuery()', function () {

    it('returns query text represented by query name', function (done) {
      // arrange
      var queryName = 'myQueryName',
        queryText = 'SELECT * FROM users LIMIT 10;';
      var queries = new QueryCache({ queryDirectory: queryDir });
      queries.fileCache[queryName] = queryText;

      // act
      queries.readQuery(queryName, function (err, data) {
        // assert
        assert.strictEqual(data, queryText, 'returned contents of file');
        assert.isNull(err, 'error should be null');

        done();
      });
    });

    it('returns error if named query does not exist', function (done) {
      // arrange
      var queryName = 'myQueryNameDoesntExist';
      var queries = new QueryCache({ queryDirectory: queryDir });

      // act
      queries.readQuery(queryName, function (err, data) {
        // assert
        assert.isUndefined(data, 'file contents are undefined');
        assert.isObject(err, 'error should be populated');

        done();
      });
    });

    it('throws error if callback not provided', function (done) {
      // arrange
      var queryName = 'myQueryNameDoesntExist';
      var queries = new QueryCache({ queryDirectory: queryDir });

      // act
      expect(function () {
        queries.readQuery(queryName);
      }).to.throw(Error);

      done();
    });

  });
});

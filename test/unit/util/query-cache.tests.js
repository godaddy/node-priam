const { assert, expect } = require('chai');
const path = require('path');
const QueryCache = require('../../../lib/util/query-cache');

const queryDir = path.join(__dirname, '../../stubs/cql');

describe('lib/util/query-cache.js', function () {

  describe('interface', function () {

    it('is a constructor function', function () {
      assert.strictEqual(typeof QueryCache, 'function', 'is a constructor function');
    });

    it('throws error if options not provided', function (done) {
      // act
      expect(function () { new QueryCache(); }).to.throw(Error);

      done();
    });

    it('throws error if queryDirectory not provided', function (done) {
      // act
      expect(function () { new QueryCache({}); }).to.throw(Error);

      done();
    });
  });

  describe('constructed instance', function () {

    let queries;

    beforeEach(function () {
      queries = new QueryCache({ queryDirectory: queryDir });
    });

    function validateFunctionExists(name, argCount) {
      // assert
      assert.strictEqual(typeof queries[name], 'function');
      assert.strictEqual(queries[name].length, argCount, `${name} takes ${argCount} arguments`);
    }

    it('provides a readQuery function', function () {
      validateFunctionExists('readQuery', 1);
    });

    it('pre-initializes the query cache', function () {
      assert.typeOf(queries.fileCache, 'object');
      assert.isDefined(queries.fileCache.myFakeCql, 'myFakeCql is loaded');
    });

  });

  describe('#readQuery()', function () {

    it('returns query text represented by query name', function () {
      // arrange
      const
        queryName = 'myQueryName',
        queryText = 'SELECT * FROM users LIMIT 10;';
      const queries = new QueryCache({ queryDirectory: queryDir });
      queries.fileCache[queryName] = queryText;

      // act
      const data = queries.readQuery(queryName);

      // assert
      assert.strictEqual(data, queryText, 'returned contents of file');
    });

    it('returns error if named query does not exist', function () {
      // arrange
      const queryName = 'myQueryNameDoesntExist';
      const queries = new QueryCache({ queryDirectory: queryDir });

      // act/assert
      expect(() => queries.readQuery(queryName)).to.throw(Error);
    });

    it('throws error if callback not provided', function (done) {
      // arrange
      const queryName = 'myQueryNameDoesntExist';
      const queries = new QueryCache({ queryDirectory: queryDir });

      // act
      expect(function () {
        queries.readQuery(queryName);
      }).to.throw(Error);

      done();
    });

  });
});

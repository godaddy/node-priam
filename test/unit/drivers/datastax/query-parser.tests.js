'use strict';

var sinon = require('sinon')
  , chai = require('chai')
  , assert = chai.assert;

var parser = require('../../../../lib/drivers/datastax/query-parser');

describe('lib/drivers/datastax/query-parser.js', function () {

  describe('#parse()', function () {
    var parse = parser.parse;
    it('returns original CQL if there are no params', function () {
      var query = 'SELECT * FROM my_column_family';
      assert.strictEqual(parse(query), query, 'Did not return the appropriate CQL');
    });
    it('returns empty if there is no query', function () {
      var query = '';
      assert.strictEqual(parse(query, [1,2,3]), query, 'Did not return the appropriate CQL');
    });
    it('generates appropriate CQL with stringified params', function() {
      var query = 'UPDATE "myColumnFamily" SET field1 = ?, field2 = ? WHERE key1=\'something\' AND key2=? USING TIMESTAMP ?';
      var params = ['field1', 'field\'2', 'key2', ((new Date()).getTime() * 1000)];
      var expected = 'UPDATE "myColumnFamily" SET field1 = \'field1\', field2 = \'field\'\'2\' WHERE key1=\'something\' AND key2=\'key2\' USING TIMESTAMP ' + params[params.length - 1].toString();

      assert.strictEqual(parse(query, params), expected, 'Did not generate the appropriate stringified CQL');
    });
  });

});
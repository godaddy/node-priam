'use strict';

var sinon = require('sinon')
  , chai = require('chai')
  , assert = chai.assert
  , expect = chai.expect;

var ds = require('cassandra-driver');
var uuid = require('uuid');
var types = ds.types;
var dataTypes = types.dataTypes;
var Long = types.Long;
var encoder = require('../../../../lib/drivers/datastax/string-encoder');

describe('lib/drivers/datastax/string-encoder.js', function () {

  describe('#stringifyValue()', function () {
    var stringify = encoder.stringifyValue;
    it('returns "NULL" for null values', function() {
      assert.strictEqual(stringify(null), 'NULL', 'null value should be "NULL"');
      assert.strictEqual(stringify(undefined), 'NULL', 'undefined value should be "NULL"');
      assert.strictEqual(stringify({ value: null, hint: dataTypes.ascii }), 'NULL', 'null value should be "NULL"');
      assert.strictEqual(stringify({ value: undefined, hint: dataTypes.ascii }), 'NULL', 'null value should be "NULL"');
    });
    it('guesses data type if type hint is not provided', function() {
      assert.strictEqual(stringify(12345), '12345', 'stringify for guessed int value failed');
      var fail = stringify.bind(encoder, { my: 'object'});
      expect(fail).to.throw(TypeError);
    });
    it('returns simple numeric values as strings', function() {
      assert.strictEqual(stringify({ value: 12345, hint: dataTypes.int }), '12345', 'stringify for int value failed');
      assert.strictEqual(stringify({ value: 12345.1234, hint: dataTypes.float }), '12345.1234', 'stringify for float value failed');
      assert.strictEqual(stringify({ value: 123451234512345.12, hint: dataTypes.double }), '123451234512345.12', 'stringify for double value failed');
    });
    it('returns boolean values as strings', function() {
      assert.strictEqual(stringify({ value: true, hint: dataTypes.boolean }), 'true', 'stringify for boolean value failed');
      assert.strictEqual(stringify({ value: false, hint: dataTypes.boolean }), 'false', 'stringify for boolean value failed');
    });
    it('returns uuid values as strings', function() {
      var id = uuid.v4();
      assert.strictEqual(stringify({ value: id, hint: dataTypes.uuid }), id.toString(), 'stringify for uuid value failed');
      assert.strictEqual(stringify({ value: id, hint: dataTypes.timeuuid }), id.toString(), 'stringify for timeuuid value failed');
    });
    it('quotes string values', function() {
      assert.strictEqual(stringify({ value: 'ab\'cd', hint: dataTypes.ascii }), '\'ab\'\'cd\'', 'stringify for ascii value failed');
      assert.strictEqual(stringify({ value: 'ab\'cd', hint: dataTypes.text }), '\'ab\'\'cd\'', 'stringify for text value failed');
      assert.strictEqual(stringify({ value: 'ab\'cd', hint: dataTypes.varchar }), '\'ab\'\'cd\'', 'stringify for varchar value failed');
      expect(stringify.bind(encoder, { value: 12345, hint: dataTypes.ascii })).to.throw(TypeError);
    });
    it('returns buffer values as strings', function() {
      var buffer = new Buffer('foobar');
      var expected = '0x' + buffer.toString('hex');
      assert.strictEqual(stringify({ value: buffer, hint: dataTypes.custom }), expected, 'stringify for custom value failed');
      assert.strictEqual(stringify({ value: buffer, hint: dataTypes.decimal }), expected, 'stringify for decimal value failed');
      assert.strictEqual(stringify({ value: buffer, hint: dataTypes.inet }), expected, 'stringify for inet value failed');
      assert.strictEqual(stringify({ value: buffer, hint: dataTypes.varint }), expected, 'stringify for varint value failed');
      assert.strictEqual(stringify({ value: buffer, hint: dataTypes.blob }), expected, 'stringify for blob value failed');
    });
    it('returns int64 number values as strings', function () {
      var l = (new Date()).getTime()*1000;
      var expected = l.toString();
      assert.strictEqual(stringify({ value: l, hint: dataTypes.bigint }), expected, 'stringify for int64 value failed');
    });
    it('returns big number values as blob strings', function () {
      var n = 1234512345123451234512345;
      var l = Long.fromNumber(n);
      var expected = 'blobAsBigint(0x' + Long.toBuffer(l).toString('hex') + ')';
      assert.strictEqual(stringify({ value: l, hint: dataTypes.bigint }), expected, 'stringify for bigint Long value failed');
      assert.strictEqual(stringify({ value: n, hint: dataTypes.bigint }), expected, 'stringify for bigint numeric value failed');
      assert.strictEqual(stringify({ value: Long.toBuffer(l), hint: dataTypes.bigint }), expected, 'stringify for bigint Buffer value failed');
      expect(stringify.bind(encoder, { value: 'notaninteger', hint: dataTypes.bigint })).to.throw(TypeError);
    });
    it('returns timestamp values as strings of ticks', function () {
      var d = new Date();
      var expected = d.getTime().toString();
      assert.strictEqual(stringify({ value: d, hint: dataTypes.timestamp }), expected, 'stringify for timestamp value failed');
    });
    it('returns list values as stringified array', function () {
      var list = [1, 2, 3, 4, 5];
      var expected = '[1,2,3,4,5]';
      assert.strictEqual(stringify({ value: list, hint: 'list<int>' }), expected, 'stringify for list<int> value failed');
      assert.strictEqual(stringify({ value: list, hint: dataTypes.list }), expected, 'stringify for list value failed');
    });
    it('returns set values as stringified object', function () {
      var set = [1, 2, 3, 4, 5];
      var expected = '{1,2,3,4,5}';
      assert.strictEqual(stringify({ value: set, hint: 'set<int>' }), expected, 'stringify for list<int> value failed');
      assert.strictEqual(stringify({ value: set, hint: dataTypes.set }), expected, 'stringify for list value failed');
    });
    it('returns map values as stringified object', function () {
      var map = {
        a: 123,
        b: 234
      };
      var expected = '{\'a\':123,\'b\':234}';
      assert.strictEqual(stringify({ value: map, hint: 'map<text,int>' }), expected, 'stringify for map<text,int> value failed');
      assert.strictEqual(stringify({ value: map, hint: dataTypes.map }), expected, 'stringify for map value failed');
      assert.strictEqual(stringify({ value: {}, hint: dataTypes.map }), '{}', 'stringify for map empty object value failed');
    });
    it('throws for unsupported types', function () {
      expect(stringify.bind(encoder, { value: 'foobar', hint: 12345 })).to.throw(TypeError);
    });
  });

  describe('#guessDataType()', function () {
    var guessDataType = encoder.guessDataType;
    it('should guess the native types', function () {
      assert.strictEqual(guessDataType(12345), dataTypes.int, 'Guess type for an integer number failed');
      assert.strictEqual(guessDataType(1.01), dataTypes.double, 'Guess type for a double number failed');
      assert.strictEqual(guessDataType(true), dataTypes.boolean, 'Guess type for a boolean value failed');
      assert.strictEqual(guessDataType(false), dataTypes.boolean, 'Guess type for a boolean value failed');
      assert.strictEqual(guessDataType([1,2,3]), dataTypes.list, 'Guess type for an Array value failed');
      assert.strictEqual(guessDataType('a string'), dataTypes.text, 'Guess type for an string value failed');
      assert.strictEqual(guessDataType(new Buffer('bip bop')), dataTypes.blob, 'Guess type for a buffer value failed');
      assert.strictEqual(guessDataType(new Date()), dataTypes.timestamp, 'Guess type for a Date value failed');
      assert.strictEqual(guessDataType(new types.Long(10)), dataTypes.bigint, 'Guess type for a Int 64 value failed');
      assert.strictEqual(guessDataType(uuid.v4()), dataTypes.uuid, 'Guess type for a UUID value failed');
      assert.strictEqual(guessDataType(types.uuid()), dataTypes.uuid, 'Guess type for a UUID value failed');
      assert.strictEqual(guessDataType(types.timeuuid()), dataTypes.uuid, 'Guess type for a Timeuuid value failed');
    });
  });

});
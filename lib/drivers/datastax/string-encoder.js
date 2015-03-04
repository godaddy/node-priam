'use strict';

/**
  Migrated from https://github.com/jorgebay/node-cassandra-cql/blob/protocol1/lib/encoder.js
  for backwards-compatibility with Jorge Gondra's node-cassandra-cql driver.
 */

var util = require('util');
var uuid = require('uuid');
var ds = require('cassandra-driver');
var types = ds.types;
var dataTypes = types.dataTypes;
var Long = types.Long;

function stringifyValue (item) {
  if (item === null || item === undefined) {
    return 'NULL';
  }
  var value = item;
  var type = null;
  var subtypes = null;
  if (item.hint !== null && item.hint !== undefined) {
    value = item.value;
    type = item.hint;
    if (typeof type === 'string') {
      var typeInfo = dataTypes.getByName(type);
      type = typeInfo.type;
      subtypes = typeInfo.subtypes;
    }
  }
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (type === null) {
    type = guessDataType(value);
    if (!type) {
      throw new TypeError('Target data type could not be guessed, you must specify a hint.', value);
    }
  }
  switch (type) {
    case dataTypes.int:
    case dataTypes.float:
    case dataTypes.double:
    case dataTypes.boolean:
    case dataTypes.uuid:
    case dataTypes.timeuuid:
      return value.toString();
    case dataTypes.text:
    case dataTypes.varchar:
    case dataTypes.ascii:
      return quote(value);
    case dataTypes.custom:
    case dataTypes.decimal:
    case dataTypes.inet:
    case dataTypes.varint:
    case dataTypes.blob:
      return stringifyBuffer(value);
    case dataTypes.bigint:
    case dataTypes.counter:
      return stringifyBigNumber(value);
    case dataTypes.timestamp:
      return stringifyDate(value);
    case dataTypes.list:
      return stringifyArray(value, subtypes && subtypes[0]);
    case dataTypes.set:
      return stringifyArray(value, subtypes && subtypes[0], '{', '}');
    case dataTypes.map:
      return stringifyMap(value, subtypes && subtypes[0], subtypes && subtypes[1]);
    default:
      throw new TypeError('Type not supported ' + type, value);
  }
}

function stringifyBuffer(value) {
  return '0x' + value.toString('hex');
}

function stringifyDate (value) {
  return value.getTime().toString();
}

function quote(value) {
  if (typeof value !== 'string') {
    throw new TypeError(null, value, 'string');
  }
  value = value.replace(/'/g, "''"); // escape strings with double single-quotes
  return "'" + value + "'";
}

function stringifyBigNumber (value) {
  if (typeof value === 'number') {
    var s = value.toString();
    if (s.indexOf('e+') === -1) { return s; }
  }
  var buf = getBigNumberBuffer(value);
  if (buf === null) {
    throw new TypeError(null, value, Long);
  }
  return 'blobAsBigint(' + stringifyBuffer(buf) + ')';
}

function getBigNumberBuffer (value) {
  var buf = null;
  if (Buffer.isBuffer(value)) {
    buf = value;
  } else if (Long.isLong(value)) {
    buf = Long.toBuffer(value);
  } else if (typeof value === 'number') {
    buf = Long.toBuffer(Long.fromNumber(value));
  }
  return buf;
}

function stringifyArray (value, subtype, openChar, closeChar) {
  if (!openChar) {
    openChar = '[';
    closeChar = ']';
  }
  var stringValues = [];
  for (var i = 0; i < value.length; i++) {
    var item = value[i];
    if (subtype) {
      item = {hint: subtype, value: item};
    }
    stringValues.push(stringifyValue(item));
  }
  return openChar + stringValues.join() + closeChar;
}

function stringifyMap (value, keyType, valueType) {
    var stringValues = [];
    var keys = Object.keys(value);
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var mapKey = key;
      if (keyType) {
        mapKey = {hint: keyType, value: mapKey};
      }
      var mapValue = value[key];
      if (valueType) {
        mapValue = {hint: valueType, value: mapValue};
      }
      stringValues.push(stringifyValue(mapKey) + ':' + stringifyValue(mapValue));
    }
    return '{' + stringValues.join() + '}';
  }


/**
 * Try to guess the Cassandra type to be stored, based on the javascript value type
 */
function guessDataType (value) {
  var dataType = null;
  if (typeof value === 'number') {
    dataType = dataTypes.int;
    if (value % 1 !== 0) {
      dataType = dataTypes.double;
    }
  }
  else if(value instanceof Date) {
    dataType = dataTypes.timestamp;
  }
  else if(Long.isLong(value)) {
    dataType = dataTypes.bigint;
  }
  else if (typeof value === 'string') {
    dataType = dataTypes.text;
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)){
      dataType = dataTypes.uuid;
    }
  }
  else if (Buffer.isBuffer(value)) {
    dataType = dataTypes.blob;
  }
  else if (util.isArray(value)) {
    dataType = dataTypes.list;
  }
  else if (value === true || value === false) {
    dataType = dataTypes.boolean;
  }
  return dataType;
}

module.exports = {
  stringifyValue: stringifyValue,
  guessDataType: guessDataType
};
'use strict';

var _ = require('lodash');
var cqlDriver = require('cassandra-driver');

var cqlDriverNumericTypeNames = ['BigDecimal', 'Integer', 'Long'];
var cqlDriverNumericTypes = _.pick(cqlDriver.types, cqlDriverNumericTypeNames);
var cqlDriverStringTypeNames = ['TimeUuid', 'Uuid'];
var cqlDriverStringTypes = _.pick(cqlDriver.types, cqlDriverStringTypeNames);
var cqlDriverTypes = _.merge({}, cqlDriverNumericTypes, cqlDriverStringTypes);

module.exports = {
  numericValueTypeNames: cqlDriverNumericTypeNames,
  numericValueTypes: cqlDriverNumericTypes,
  stringValueTypeNames: cqlDriverStringTypeNames,
  stringValueTypes: cqlDriverStringTypes,
  valueTypes: cqlDriverTypes,
  dataTypes: cqlDriver.types.dataTypes,
  consistencies: cqlDriver.types.consistencies
};

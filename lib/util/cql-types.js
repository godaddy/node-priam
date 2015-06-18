'use strict';

var _ = require('lodash');
var cqlDriver = require('cassandra-driver');

var cqlDriverNumericTypeNames = ['BigDecimal', 'Integer', 'Long'];
var cqlDriverNumericTypes = _.pick(cqlDriver.types, cqlDriverNumericTypeNames);
var cqlDriverStringTypeNames = ['TimeUuid', 'Uuid'];
var cqlDriverStringTypes = _.pick(cqlDriver.types, cqlDriverStringTypeNames);
var cqlDriverTypes = _.merge({}, cqlDriverNumericTypes, cqlDriverStringTypes);
var consistencies = cqlDriver.types.consistencies;

module.exports = {
  numericValueTypeNames: cqlDriverNumericTypeNames,
  numericValueTypes: cqlDriverNumericTypes,
  stringValueTypeNames: cqlDriverStringTypeNames,
  stringValueTypes: cqlDriverStringTypes,
  valueTypes: cqlDriverTypes,
  dataTypes: cqlDriver.types.dataTypes,
  consistencies: _.merge({}, consistencies, {
    ONE: consistencies.one,
    TWO: consistencies.two,
    THREE: consistencies.three,
    QUORUM: consistencies.quorum,
    LOCAL_QUORUM: consistencies.localQuorum,
    LOCAL_ONE: consistencies.localOne,
    EACH_QUORUM: consistencies.eachQuorum,
    ALL: consistencies.all,
    ANY: consistencies.any,
  })
};

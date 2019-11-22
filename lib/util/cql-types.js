const _ = require('lodash');
const cqlDriver = require('cassandra-driver');

const cqlDriverNumericTypeNames = ['BigDecimal', 'Integer', 'Long'];
const cqlDriverNumericTypes = _.pick(cqlDriver.types, cqlDriverNumericTypeNames);
const cqlDriverStringTypeNames = ['TimeUuid', 'Uuid', 'InetAddress'];
const cqlDriverStringTypes = _.pick(cqlDriver.types, cqlDriverStringTypeNames);
const cqlDriverTypes = _.merge({}, cqlDriverNumericTypes, cqlDriverStringTypes);
const consistencies = cqlDriver.types.consistencies;

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
    ANY: consistencies.any
  })
};

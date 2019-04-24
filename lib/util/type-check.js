

module.exports = {
  isQuery: isQuery,
  isBatch: isBatch
};

function isDefined(object, property) {
  return typeof object[property] !== 'undefined';
}

function isQuery(object) {
  return !!(object && !isDefined(object, 'batchType') &&
  isDefined(object, 'context') &&
  isDefined(object.context, 'cql'));
}

function isBatch(object) {
  return !!(object &&
  isDefined(object, 'batchType') &&
  isDefined(object, 'context') &&
  Array.isArray(object.context.queries));
}

const q = require('q');

exports = module.exports = function promisify(nodeAsyncFn, context) {
  return function () {
    const defer = q.defer();
    const args  = Array.prototype.slice.call(arguments);

    args.push(function (err, val) {
      if (err !== null) {
        return defer.reject(err);
      }

      return defer.resolve(val);
    });

    nodeAsyncFn.apply(context, args);

    return defer.promise;
  };
};

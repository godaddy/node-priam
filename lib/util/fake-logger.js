

function getCallback(args) {
  if (args.length && typeof args[args.length - 1] === 'function') {
    return args[args.length - 1];
  }
  return null;
}

function logFake() {
  const cb = getCallback(Array.prototype.slice.call(arguments, 0));
  if (typeof cb === 'function') {
    return void cb(null, {});
  }
}

module.exports = {
  info: logFake,
  warn: logFake,
  error: logFake,
  debug: logFake,
  critical: logFake
};

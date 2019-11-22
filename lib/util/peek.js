async function peek(asyncIterable, count = 1) {
  const buffer = [];
  for (let i = 0; i < count; i++) {
    const { value, done } = await asyncIterable.next();
    if (done) {
      break;
    }
    buffer.push(value);
  }

  async function *newIterator() {
    yield* buffer;
    yield* asyncIterable;
  }

  return newIterator();
}

module.exports = peek;

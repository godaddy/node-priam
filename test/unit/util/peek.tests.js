const { expect } = require('chai');
const peek = require('../../../lib/util/peek');

describe('peek', () => {
  it('returns an async iterable containing the original contents', async () => {
    function *iterable() {
      yield 1;
      yield 2;
      yield 3;
    }

    const newIterable = await peek(iterable());

    const items = [];
    for await (const item of newIterable) {
      items.push(item);
    }

    expect(items).to.deep.equal([1, 2, 3]);
  });

  it('throws if the original iterable throws while seeking', async () => {
    const mockError = new Error('Bah!');
    function *iterable() {
      throw mockError;
      // eslint-disable-next-line no-unreachable
      yield 2;
      yield 3;
    }

    let error;
    try {
      await peek(iterable());
    } catch (err) {
      error = err;
    }

    expect(error).to.equal(mockError);
  });
});

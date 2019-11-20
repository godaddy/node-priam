const { Writable } = require('stream');
const { expect } = require('chai');
const iterateIntoStream = require('../../../lib/util/iterate-into-stream');

describe('lib/util/iterate-into-stream.js', () => {
  it('writes all items in an async iterable to a stream', async () => {
    const written = [];
    const stream = new Writable({
      objectMode: true,
      write(item, _, cb) {
        written.push(item);
        cb();
      }
    });

    function *iterable() {
      yield 3;
      yield 2;
      yield 1;
    }

    await iterateIntoStream(iterable(), stream);

    expect(written).to.deep.equal([3, 2, 1]);
  });

  it('emits errors through the stream', done => {
    const error = new Error('Boom!');
    const stream = new Writable({
      objectMode: true,
      write(__, _, cb) { cb(); }
    });
    stream.once('error', assertError);

    function *iterable() {
      yield 3;
      yield 2;
      throw error;
    }

    iterateIntoStream(iterable(), stream);

    function assertError(err) {
      expect(err).to.equal(error);
      done();
    }
  });

  it('handles backpressure', async () => {
    const written = [];
    const stream = new Writable({
      objectMode: true,
      highWaterMark: 1,
      write(item, _, cb) {
        written.push(item);
        setTimeout(() => cb(), 10);
      }
    });

    function *iterable() {
      yield 3;
      yield 2;
      yield 1;
    }

    iterateIntoStream(iterable(), stream);

    await delay(5);
    expect(written).to.deep.equal([3]);

    await delay(25);
    expect(written).to.deep.equal([3, 2, 1]);
  });

  function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
});

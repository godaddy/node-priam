const { once } = require('events');
async function iterateIntoStream(iterable, stream) {
  let queryFinished = false;
  try {
    for await (const chunk of iterable) {
      queryFinished = true;
      if (!stream.write(chunk))
        await once(stream, 'drain');
    }
  } catch (err) {
    if (queryFinished) {
      stream.emit('error', err);
    } else {
      // Ensure error during query causes a retry
      throw err;
    }
  } finally {
    stream.end();
  }
}

module.exports = iterateIntoStream;

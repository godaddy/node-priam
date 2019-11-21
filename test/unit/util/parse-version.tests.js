const sut = require('../../../lib/util/parse-version');
const { assert } = require('chai');

describe('lib/util/parse-version.js', function () {

  describe('module export', function () {
    it('returns a function', function () {
      assert.isFunction(sut);
      assert.strictEqual(sut.length, 1);
    });
  });

  function testParse(value, major, minor, patch) {
    it(`parseVersion#() parses "${value}" correctly`, function () {
      const result = sut(value);
      assert.strictEqual(result.major, major, 'major version matches');
      assert.strictEqual(result.minor, minor, 'minor version matches');
      assert.strictEqual(result.patch, patch, 'patch version matches');
    });
  }

  testParse(null, 0, 0, 0);
  testParse('not a number', 0, 0, 0);
  testParse('3', 3, 0, 0);
  testParse('3.2', 3, 2, 0);
  testParse('3.2.1', 3, 2, 1);
  testParse('.2.1', 0, 2, 1);
  testParse('..1', 0, 0, 1);
  testParse('...', 0, 0, 0);
});

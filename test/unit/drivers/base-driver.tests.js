'use strict';

var sinon = require('sinon')
  , chai = require('chai')
  , assert = chai.assert
  , expect = chai.expect
  , Query = require('../../../lib/util/query')
  , Batch = require('../../../lib/util/batch');

var EventEmitter = require('events').EventEmitter;

var Driver = require('../../../lib/drivers/base-driver');

describe('lib/drivers/base-driver.js', function () {

  function getDefaultLogger() {
    return {
      debug: sinon.stub(),
      info: sinon.stub(),
      warn: sinon.stub(),
      error: sinon.stub(),
      critical: sinon.stub()
    };
  }

  function getDefaultConfig() {
    return {
      hosts: ['123.456.789.012:9160'],
      keyspace: 'myKeySpace',
      timeout: 12345
    };
  }

  function getDefaultInstance() {
    var instance = new Driver();
    var context = {
      config: getDefaultConfig(),
      logger: getDefaultLogger()
    };
    instance.init(context);
    return instance;
  }

  describe('interface', function () {

    var instance = getDefaultInstance();

    function validateFunctionExists(name, argCount) {
      // arrange
      // act
      // assert
      assert.strictEqual(typeof instance[name], 'function');
      assert.strictEqual(instance[name].length, argCount, name + ' takes ' + argCount + ' arguments');
    }

    it('is a constructor function', function () {
      assert.strictEqual(typeof Driver, 'function', 'exports a constructor function');
    });

    describe('instance', function () {
      it('provides a cql function', function () {
        validateFunctionExists('cql', 4);
      });
      it('provides a namedQuery function', function () {
        validateFunctionExists('namedQuery', 4);
      });
      it('provides a select function', function () {
        validateFunctionExists('select', 4);
      });
      it('provides a insert function', function () {
        validateFunctionExists('insert', 4);
      });
      it('provides a update function', function () {
        validateFunctionExists('update', 4);
      });
      it('provides a delete function', function () {
        validateFunctionExists('delete', 4);
      });
      it('provides a close function', function () {
        validateFunctionExists('close', 1);
      });
      it('is an EventEmitter', function () {
        expect(instance).to.be.an.instanceOf(EventEmitter);
      });
    });
  });

  describe('BaseDriver#constructor', function () {
    it('sets default values', function () {
      // arrange
      // act
      var instance = new Driver();

      // assert
      assert.ok(instance.consistencyLevel);
      assert.ok(instance.dataType);
    });

    it('converts consistency levels to DB codes', function () {
      // arrange
      // act
      var instance = new Driver();
      instance.consistencyLevel = {one: 1};
      instance.init({config: {consistency: 'one'}});

      // assert
      assert.equal(instance.consistencyLevel.one, instance.poolConfig.consistencyLevel);
    });

    it('throws an error if given an invalid consistency level', function () {
      // arrange
      // act
      var instance = new Driver();
      instance.consistencyLevel = {one: 1};
      var initWithInvalidConsistency = function() {
        instance.init({config: {consistency: 'invalid consistency level'}});
      };

      // assert
      assert.throw(initWithInvalidConsistency, 'Error: "invalid consistency level" is not a valid consistency level' );
    });
  });

  it('BaseDriver#beginQuery() returns a Query', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    var result = driver.beginQuery();

    // assert
    assert.instanceOf(result, Query, 'result is instance of Query');
    done();
  });

  it('BaseDriver#beginBatch() returns a Batch', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    var result = driver.beginBatch();

    // assert
    assert.instanceOf(result, Batch, 'result is instance of Batch');
    done();
  });

  describe('BaseDriver#connect()', function () {

    var driver;
    beforeEach(function () {
      driver = getDefaultInstance();
      driver.init({ config: {} });
    });

    it('creates a connection for the supplied keyspace', function (done) {
      // arrange
      var keyspace = 'myKeyspace';

      // act
      driver.connect(keyspace, function (err, pool) {
        assert.notOk(err);
        assert.equal(driver.pools[keyspace], pool);
        done();
      });
    });

    it('creates a connection for the default keyspace if keyspace is not supplied', function (done) {
      // arrange
      // act
      driver.connect(function (err, pool) {
        assert.notOk(err);
        assert.equal(driver.pools.default, pool);
        done();
      });
    });

    it('yields error if connection pool fails to initialize', function (done) {
      // arrange
      var error = new Error('connection failed');
      driver.getConnectionPool = sinon.stub().yields(error);

      // act
      driver.connect(function (err, pool) {
        assert.equal(err, error);
        assert.notOk(pool);
        done();
      });
    });
  });

  describe('BaseDriver#cql()', function(){

    it('is expected function', function(){
      var driver = getDefaultInstance();
      assert.isFunction(driver.cql);
      assert.equal(driver.cql.length, 4);
    });

    describe('resultTransformers', function(){

      function testTransformers(transformers, results, cb){
        var driver = getDefaultInstance();
        driver.execCql = sinon.stub().yields(null, results);
        driver.cql('test', [], {
          resultTransformers: transformers
        }, cb);
      }

      it('called if results', function(done){
        var transformer = sinon.stub(),
          results = [{test:true}];
        testTransformers([transformer], results, function(err, results){
          assert.ok(transformer.calledOnce);
          done();
        });
      });

      it('does not call unless results', function(done){
        var transformer = sinon.stub(),
          results;
        testTransformers([transformer], results, function(err, results){
          assert.notOk(transformer.called);
          done();
        });
      });

      it('does not call unless results.length', function(done){
        var transformer = sinon.stub(),
          results = [];
        testTransformers([transformer], results, function(err, results){
          assert.notOk(transformer.called);
          done();
        });
      });
    });
  });

  // NOTE: All of the functions below are stubs for functionality that should be
  //       provided by the inheriting driver classes. These tests are present solely for
  //       code coverage purposes

  it('BaseDriver#initProviderOptions() does nothing', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    driver.initProviderOptions();

    done();
  });

  it('BaseDriver#getNormalizedResults() returns original argument', function (done) {
    // arrange
    var driver = getDefaultInstance();
    var expected = [
      { }
    ];

    // act
    var actual = driver.getNormalizedResults(expected, {});

    assert.deepEqual(expected, actual);
    done();
  });

  it('BaseDriver#dataToCql() returns original argument', function (done) {
    // arrange
    var driver = getDefaultInstance();
    var expected = 'myValue';

    // act
    var actual = driver.dataToCql(expected);

    assert.strictEqual(expected, actual);
    done();
  });

  it('BaseDriver#executeCqlOnDriver() calls callback', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    driver.executeCqlOnDriver(null, null, null, null, null, done);
  });

  it('BaseDriver#canRetryError() returns false', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    var result = driver.canRetryError(null);

    // assert
    assert.isFalse(result);
    done();
  });

  it('BaseDriver#closePool() calls callback', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    driver.closePool(null, done);
  });

  it('BaseDriver#createConnectionPool() calls callback', function (done) {
    // arrange
    var driver = getDefaultInstance();

    // act
    driver.createConnectionPool(null, false, done);
  });

  describe('BaseDriver#param()', function () {
    var driver;

    beforeEach(function () {
      driver = new getDefaultInstance();
      driver.dataType.timestamp = 1;
    });

    it('returns the value parameter if no type hint was provided', function () {
      expect(driver.param('foo')).to.equal('foo');
    });

    it('returns a hinted value wrapper if a type hint was provided', function () {
      var timestamp = new Date();
      var param = driver.param(timestamp, driver.dataType.timestamp);

      expect(param.value).to.equal(timestamp);
      expect(param.hint).to.equal(driver.dataType.timestamp);
    });

    it('returns a hinted value wrapper if a type hint was provided as a string', function () {
      var timestamp = new Date();
      var param = driver.param(timestamp, 'timestamp');

      expect(param.value).to.equal(timestamp);
      expect(param.hint).to.equal(driver.dataType.timestamp);
    });

    it('returns a hinted value wrapper if an unmapped type hint was provided as a string', function () {
      var type = 'map<text,text>';
      var val = {key: 'value'};
      var param = driver.param(val, type);

      expect(param.value).to.equal(val);
      expect(param.hint).to.equal(type);
    });
  });

  describe('BaseDriver#getDriverDataType()', function () {
    var driver;

    beforeEach(function () {
      driver = new getDefaultInstance();
      driver.dataType.ascii = 1;
      driver.dataType.text = 2;
    });

    it('returns "ascii" if "objectAscii" provided', function () {
      var value = '{ "some": "jsonObject" }';
      var param = driver.param(value, 'objectAscii');

      var type = driver.getDriverDataType(param.hint);

      expect(type).to.equal(driver.dataType.ascii);
    });

    it('returns "text" if "text" provided', function () {
      var value = '{ "some": "jsonObject" }';
      var param = driver.param(value, 'text');

      var type = driver.getDriverDataType(param.hint);

      expect(type).to.equal(driver.dataType.text);
    });
  });
});

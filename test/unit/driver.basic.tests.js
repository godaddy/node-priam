const
  Stream = require('stream'),
  { EventEmitter } = require('events'),
  sinon = require('sinon'),
  { assert, expect } = require('chai'),
  _ = require('lodash'),
  cql = require('cassandra-driver'),
  Driver = require('../../lib/driver'),
  Query = require('../../lib/util/query'),
  Batch = require('../../lib/util/batch');

describe('lib/driver.js', function () {

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

  function getDefaultContext() {
    return {
      config: getDefaultConfig(),
      logger: getDefaultLogger()
    };
  }

  function getDefaultInstance() {
    const context = getDefaultContext();
    return new Driver(context);
  }

  beforeEach(function () {
    sinon.stub(cql, 'Client').returns({
      connect: sinon.stub().yieldsAsync(),
      on: sinon.stub(),
      execute: sinon.stub().yieldsAsync(null, [])
    });
  });

  afterEach(function () {
    if (cql.Client.restore) { cql.Client.restore(); }
  });

  describe('interface', function () {

    const instance = getDefaultInstance();

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
      it('instance provides a streamCqlOnDriver function', function () {
        validateFunctionExists('streamCqlOnDriver', 6);
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
    it('throws an error if context not provided', function () {
      // arrange
      // act
      const initWithNoContext = function () {
        new Driver();
      };

      // assert
      expect(initWithNoContext).to.throw(Error, /missing context/);
    });

    it('throws an error if context.config not provided', function () {
      // arrange
      // act
      const initWithNoConfig = function () {
        new Driver({});
      };

      // assert
      expect(initWithNoConfig).to.throw(Error, /missing context\.config/);
    });

    it('sets default values', function () {
      // arrange
      // act
      const instance = new Driver(getDefaultContext());

      // assert
      assert.ok(instance.consistencyLevel);
      assert.ok(instance.dataType);
    });

    it('converts consistency levels to DB codes', function () {
      // arrange
      // act
      const instance = new Driver(getDefaultContext());
      instance.consistencyLevel = { one: 1 };
      instance.init({ config: { consistency: 'one' } });

      // assert
      assert.equal(instance.consistencyLevel.one, instance.poolConfig.consistencyLevel);
    });

    it('throws an error if given an invalid consistency level', function () {
      // arrange
      // act
      const instance = new Driver(getDefaultContext());
      instance.consistencyLevel = { one: 1 };
      const initWithInvalidConsistency = function () {
        instance.init({ config: { consistency: 'invalid consistency level' } });
      };

      // assert
      assert.throw(initWithInvalidConsistency, 'Error: "invalid consistency level" is not a valid consistency level');
    });

    it('sets the name property', function () {
      // arrange
      const config = _.extend({}, getDefaultConfig());

      // act
      const instance = new Driver({ config: config });

      // assert
      assert.strictEqual(instance.name, 'datastax');
    });

    it('sets default pool configuration', function () {
      // arrange
      const config = _.extend({}, getDefaultConfig());
      const configCopy = _.extend({}, config);
      const consistencyLevel = cql.types.consistencies.one;

      // act
      const instance = new Driver({ config: config });

      // assert
      assert.deepEqual(instance.poolConfig.contactPoints, configCopy.hosts, 'hosts should be passed through');
      assert.strictEqual(instance.poolConfig.keyspace, configCopy.keyspace, 'keyspace should be passed through');
      assert.strictEqual(instance.poolConfig.getAConnectionTimeout, configCopy.timeout, 'timeout should be passed through');
      assert.strictEqual(instance.poolConfig.version, configCopy.cqlVersion, 'cqlVersion should be passed through');
      assert.strictEqual(instance.poolConfig.limit, configCopy.limit, 'limit should be passed through');
      assert.strictEqual(instance.poolConfig.consistencyLevel, consistencyLevel, 'consistencyLevel should default to ONE');
    });

    it('should override default pool config with additional store options', function () {
      // arrange
      const config = _.extend({}, getDefaultConfig());
      const configCopy = _.extend({}, config);
      const cqlVersion = '2.0.0';
      const consistencyLevel = cql.types.consistencies.any;
      const limit = 300;
      config.cqlVersion = cqlVersion;
      config.consistencyLevel = consistencyLevel;
      config.limit = limit;

      // act
      const instance = new Driver({ config: config });

      // assert
      assert.deepEqual(instance.poolConfig.contactPoints, configCopy.hosts, 'hosts should be passed through');
      assert.strictEqual(instance.poolConfig.getAConnectionTimeout, configCopy.timeout, 'timeout should be passed through');
      assert.strictEqual(instance.poolConfig.keyspace, configCopy.keyspace, 'keyspace should be passed through');
      assert.strictEqual(instance.poolConfig.version, cqlVersion, 'cqlVersion should be overridden');
      assert.strictEqual(instance.poolConfig.limit, limit, 'limit should be overridden');
      assert.strictEqual(instance.poolConfig.consistencyLevel, consistencyLevel, 'consistencyLevel should be overridden');
    });

    it('uses localDataCenter config in the load balancing policy', done => {
      const driver = new Driver({
        config: {
          localDataCenter: 'some_dc'
        }
      });

      driver.connect('myKeyspace', () => {
        const dsOptions = cql.Client.lastCall.args[0];
        expect(dsOptions.policies).to.be.an('object');
        expect(dsOptions.policies.loadBalancing)
          .to.be.instanceOf(cql.policies.loadBalancing.TokenAwarePolicy);
        expect(dsOptions.policies.loadBalancing.childPolicy)
          .to.be.instanceOf(cql.policies.loadBalancing.DCAwareRoundRobinPolicy);
        expect(dsOptions.policies.loadBalancing.childPolicy.localDc)
          .to.equal('some_dc');
        done();
      });
    });
  });

  it('BaseDriver#beginQuery() returns a Query', function (done) {
    // arrange
    const driver = getDefaultInstance();

    // act
    const result = driver.beginQuery();

    // assert
    assert.instanceOf(result, Query, 'result is instance of Query');
    done();
  });

  it('BaseDriver#beginBatch() returns a Batch', function (done) {
    // arrange
    const driver = getDefaultInstance();

    // act
    const result = driver.beginBatch();

    // assert
    assert.instanceOf(result, Batch, 'result is instance of Batch');
    done();
  });

  describe('BaseDriver#connect()', function () {

    let driver;
    beforeEach(function () {
      driver = getDefaultInstance();
      driver.init({ config: {} });
    });

    it('creates a connection for the supplied keyspace', function (done) {
      // arrange
      const keyspace = 'myKeyspace';

      // act
      driver.connect(keyspace, function (err, pool) {
        assert.notOk(err);
        assert.equal(driver.pools[keyspace], pool);
        done();
      });
    });

    it('creates a connection for the default keyspace if keyspace is not supplied', function (done) {
      // arrange
      driver.keyspace = 'defaultKeyspace';

      // act
      driver.connect(function (err, pool) {
        assert.notOk(err);
        assert.equal(driver.pools[driver.keyspace], pool);
        done();
      });
    });

    it('yields error if connection pool fails to initialize', function (done) {
      // arrange
      const error = new Error('connection failed');
      driver.getConnectionPool = sinon.stub().yields(error);

      // act
      driver.connect(function (err, pool) {
        assert.equal(err, error);
        assert.notOk(pool);
        done();
      });
    });
  });

  describe('BaseDriver#cql()', function () {

    it('is expected function', function () {
      const driver = getDefaultInstance();
      assert.isFunction(driver.cql);
      assert.equal(driver.cql.length, 4);
    });

    it('calls #execCql() if callback function is provided', function () {
      const cql = 'myCqlQuery';
      const params = [];
      const options = { consistency: 'one' };
      const cb = sinon.stub();
      const driver = getDefaultInstance();
      driver.execCql = sinon.stub();
      driver.execCqlStream = sinon.stub();

      driver.cql(cql, params, options, cb);

      expect(driver.execCql.calledOnce).to.be.true;
      expect(driver.execCql.args[0][0]).to.equal(cql);
      expect(driver.execCql.args[0][1]).to.deep.equal(params);
      expect(driver.execCql.args[0][2]).to.deep.equal(options);
      expect(driver.execCql.args[0][3]).to.be.a('string');
      expect(driver.execCql.args[0][4]).to.be.a('function');
      expect(driver.execCqlStream.called).to.be.false;
    });

    it('calls #execCqlStream() if stream is provided', function () {
      const cql = 'myCqlQuery';
      const params = [];
      const options = { consistency: 'one' };
      const stream = new Stream();
      const driver = getDefaultInstance();
      driver.execCql = sinon.stub();
      driver.execCqlStream = sinon.stub();

      driver.cql(cql, params, options, stream);

      expect(driver.execCqlStream.calledOnce).to.be.true;
      expect(driver.execCqlStream.args[0][0]).to.equal(cql);
      expect(driver.execCqlStream.args[0][1]).to.deep.equal(params);
      expect(driver.execCqlStream.args[0][2]).to.deep.equal(options);
      expect(driver.execCqlStream.args[0][3]).to.equal(stream);
      expect(driver.execCql.called).to.be.false;
    });

    describe('resultTransformers', function () {

      function testTransformers(transformers, results, cb) {
        const driver = getDefaultInstance();
        driver.execCql = sinon.stub().yields(null, results);
        driver.cql('test', [], {
          resultTransformers: transformers
        }, cb);
      }

      it('called if results', function (done) {
        const transformer = sinon.stub(),
          results = [{ test: true }];
        testTransformers([transformer], results, function () {
          assert.ok(transformer.calledOnce);
          done();
        });
      });

      it('does not call unless results', function (done) {
        const transformer = sinon.stub(),
          results = [];
        testTransformers([transformer], results, function () {
          assert.notOk(transformer.called);
          done();
        });
      });

      it('does not call unless results.length', function (done) {
        const transformer = sinon.stub(),
          results = [];
        testTransformers([transformer], results, function () {
          assert.notOk(transformer.called);
          done();
        });
      });
    });
  });

  describe('BaseDriver#executeCqlStream()', function () {

    let cql, dataParams, options, stream, driver, pool;
    beforeEach(function () {
      cql = 'myCqlStatement';
      dataParams = [];
      options = { consistency: 'one' };
      stream = {
        emit: sinon.stub()
      };
      driver = getDefaultInstance();
      pool = { isReady: true, waiters: [] };
      driver.streamCqlOnDriver = sinon.stub();
      driver.getConnectionPool = sinon.stub().yields(null, pool);
    });

    it('calls #streamCqlOnDriver() if pool is ready', function () {
      driver.execCqlStream(cql, dataParams, options, stream);
      expect(driver.streamCqlOnDriver.calledOnce).to.be.true;
      expect(driver.streamCqlOnDriver.args[0][0]).to.equal(pool);
      expect(driver.streamCqlOnDriver.args[0][1]).to.equal(cql);
      expect(driver.streamCqlOnDriver.args[0][2]).to.deep.equal(dataParams);
      expect(driver.streamCqlOnDriver.args[0][3]).to.deep.equal(options.consistency);
      expect(driver.streamCqlOnDriver.args[0][4]).to.deep.equal(options);
      expect(driver.streamCqlOnDriver.args[0][5]).to.equal(stream);
      expect(pool.waiters.length).to.equal(0);
      expect(stream.emit.called).to.be.false;
    });

    it('calls #streamCqlOnDriver() after pool is ready if pool is not yet ready', function () {
      pool.isReady = false;
      driver.execCqlStream(cql, dataParams, options, stream);
      expect(driver.streamCqlOnDriver.called).to.be.false;
      expect(pool.waiters.length).to.equal(1);
      pool.isReady = true;
      pool.waiters[0]();
      expect(driver.streamCqlOnDriver.calledOnce).to.be.true;
      expect(stream.emit.called).to.be.false;
    });

    it('emits error to stream if pool resolution fails', function () {
      const error = new Error('uh-oh');
      driver.getConnectionPool = sinon.stub().yields(error);
      driver.execCqlStream(cql, dataParams, options, stream);
      expect(driver.streamCqlOnDriver.called).to.be.false;
      expect(stream.emit.calledOnce).to.be.true;
      expect(stream.emit.args[0][0]).to.equal(error);
    });

    it('emits error to stream if pool connection fails', function () {
      pool.isReady = false;
      driver.execCqlStream(cql, dataParams, options, stream);
      expect(driver.streamCqlOnDriver.called).to.be.false;
      expect(pool.waiters.length).to.equal(1);
      pool.isReady = true;
      const error = new Error('uh-oh');
      pool.waiters[0](error);
      expect(driver.streamCqlOnDriver.called).to.be.false;
      expect(stream.emit.calledOnce).to.be.true;
      expect(stream.emit.args[0][0]).to.equal(error);
    });

  });

  it('BaseDriver#canRetryError() returns false', function (done) {
    // arrange
    const driver = getDefaultInstance();

    // act
    const result = driver.canRetryError(null);

    // assert
    assert.isFalse(result);
    done();
  });

  describe('BaseDriver#param()', function () {
    let driver;

    beforeEach(function () {
      driver = new getDefaultInstance();
      driver.dataType.timestamp = 1;
    });

    it('returns the value parameter if no type hint was provided', function () {
      expect(driver.param('foo')).to.equal('foo');
    });

    it('returns a hinted value wrapper if a type hint was provided', function () {
      const timestamp = new Date();
      const param = driver.param(timestamp, driver.dataType.timestamp);

      expect(param.value).to.equal(timestamp);
      expect(param.hint).to.equal(driver.dataType.timestamp);
      expect(param.isRoutingKey).to.equal(false);
    });

    it('returns a hinted value wrapper marked as routing key if a type hint was provided and isRoutingKey is true', function () {
      const timestamp = new Date();
      const param = driver.param(timestamp, driver.dataType.timestamp, true);

      expect(param.value).to.equal(timestamp);
      expect(param.hint).to.equal(driver.dataType.timestamp);
      expect(param.isRoutingKey).to.equal(true);
    });

    it('returns a hinted value wrapper if a type hint was provided as a string', function () {
      const timestamp = new Date();
      const param = driver.param(timestamp, 'timestamp');

      expect(param.value).to.equal(timestamp);
      expect(param.hint).to.equal(driver.dataType.timestamp);
      expect(param.isRoutingKey).to.equal(false);
    });

    it('returns a hinted value wrapper if an unmapped type hint was provided as a string', function () {
      const type = 'map<text,text>';
      const val = { key: 'value' };
      const param = driver.param(val, type);

      expect(param.value).to.equal(val);
      expect(param.hint).to.equal(type);
      expect(param.isRoutingKey).to.equal(false);
    });
  });

  describe('BaseDriver#getDriverDataType()', function () {
    let driver;

    beforeEach(function () {
      driver = new getDefaultInstance();
      driver.dataType.ascii = 1;
      driver.dataType.text = 2;
    });

    it('returns "ascii" if "objectAscii" provided', function () {
      const value = '{ "some": "jsonObject" }';
      const param = driver.param(value, 'objectAscii');

      const type = driver.getDriverDataType(param.hint);

      expect(type).to.equal(driver.dataType.ascii);
    });

    it('returns "text" if "text" provided', function () {
      const value = '{ "some": "jsonObject" }';
      const param = driver.param(value, 'text');

      const type = driver.getDriverDataType(param.hint);

      expect(type).to.equal(driver.dataType.text);
    });
  });

  describe('BaseDriver#isBatch()', function () {
    let driver;

    beforeEach(function () {
      driver = new getDefaultInstance();
    });

    it('returns true if passed a batch', function () {
      const batch = driver.beginBatch();
      expect(driver.isBatch(batch)).to.be.true;
    });

    it('returns false if passed a non-batch', function () {
      const notBatch = {
        add: function () {
        },
        foo: 'bar'
      };
      expect(driver.isBatch(notBatch)).to.be.false;
      expect(driver.isBatch(new Query(driver))).to.be.false;
    });

    it('handles weird values', function () {
      expect(driver.isBatch()).to.be.false;
      expect(driver.isBatch(null)).to.be.false;
      expect(driver.isBatch(undefined)).to.be.false;
      expect(driver.isBatch({})).to.be.false;
      expect(driver.isBatch(true)).to.be.false;
      expect(driver.isBatch(false)).to.be.false;
      expect(driver.isBatch([])).to.be.false;
    });
  });

  describe('#BaseDriverisQuery()', function () {
    let driver;

    beforeEach(function () {
      driver = new getDefaultInstance();
    });

    it('returns true if passed a query', function () {
      expect(driver.isQuery(new Query(driver))).to.be.true;
    });

    it('returns false if passed a non-query', function () {
      const notQuery = {
        execute: function () {
        },
        foo: 'bar'
      };
      expect(driver.isQuery(notQuery)).to.be.false;
      expect(driver.isQuery(driver.beginBatch())).to.be.false;
    });

    it('handles weird values', function () {
      expect(driver.isQuery()).to.be.false;
      expect(driver.isQuery(null)).to.be.false;
      expect(driver.isQuery(undefined)).to.be.false;
      expect(driver.isQuery({})).to.be.false;
      expect(driver.isQuery(true)).to.be.false;
      expect(driver.isQuery(false)).to.be.false;
      expect(driver.isQuery([])).to.be.false;
    });
  });

});

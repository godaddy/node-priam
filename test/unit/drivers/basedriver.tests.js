"use strict";

var sinon = require("sinon"),
    chai = require("chai"),
    assert = chai.assert,
    expect = chai.expect,
    Query = require("../../../lib/util/query"),
    Batch = require("../../../lib/util/batch");

var Driver = require("../../../lib/drivers/basedriver");

describe("lib/drivers/basedriver.js", function () {

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
            hosts: ["123.456.789.012:9160"],
            keyspace: "myKeySpace",
            timeout: 12345
        };
    }
    function getDefaultInstance() {
        var instance = new Driver({
            config: getDefaultConfig(),
            logger: getDefaultLogger()
        });
        return instance;
    }

    describe("interface", function () {

        var instance = getDefaultInstance();

        function validateFunctionExists(name, argCount) {
            // arrange
            // act
            // assert
            assert.strictEqual(typeof instance[name], "function");
            assert.strictEqual(instance[name].length, argCount, name + " takes " + argCount + " arguments");
        }

        it("is a constructor function", function () {
            assert.strictEqual(typeof Driver, "function", "exports a constructor function");
        });
        it("instance provides a cql function", function () {
            validateFunctionExists("cql", 4);
        });
        it("instance provides a namedQuery function", function () {
            validateFunctionExists("namedQuery", 4);
        });
        it("instance provides a select function", function () {
            validateFunctionExists("select", 4);
        });
        it("instance provides a insert function", function () {
            validateFunctionExists("insert", 4);
        });
        it("instance provides a update function", function () {
            validateFunctionExists("update", 4);
        });
        it("instance provides a delete function", function () {
            validateFunctionExists("delete", 4);
        });
        it("instance provides a close function", function () {
            validateFunctionExists("close", 1);
        });
    });

    describe("BaseDriver#constructor", function () {
        it ("sets default values", function () {
            // arrange
            // act
            var instance = new Driver();

            // assert
            assert.ok(instance.consistencyLevel);
            assert.ok(instance.dataType);
        });
    });

    it("BaseDriver#beginQuery() returns a Query", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        var result = driver.beginQuery();

        // assert
        assert.instanceOf(result, Query, "result is instance of Query");
        done();
    });

    it("BaseDriver#beginBatch() returns a Batch", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        var result = driver.beginBatch();

        // assert
        assert.instanceOf(result, Batch, "result is instance of Batch");
        done();
    });

    // NOTE: All of the functions below are stubs for functionality that should be
    //       provided by the inheriting driver classes. These tests are present solely for
    //       code coverage purposes

    it("BaseDriver#initProviderOptions() does nothing", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        driver.initProviderOptions();

        done();
    });

    it("BaseDriver#initProviderOptions() returns original argument", function (done) {
        // arrange
        var driver = getDefaultInstance();
        var expected = [{ }];

        // act
        var actual = driver.getNormalizedResults(expected);

        assert.deepEqual(expected, actual);
        done();
    });

    it("BaseDriver#dataToCql() returns original argument", function (done) {
        // arrange
        var driver = getDefaultInstance();
        var expected = "myValue";

        // act
        var actual = driver.dataToCql(expected);

        assert.strictEqual(expected, actual);
        done();
    });

    it("BaseDriver#executeCqlOnDriver() calls callback", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        driver.executeCqlOnDriver(null, null, null, null, null, done);
    });

    it("BaseDriver#canRetryError() returns false", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        var result = driver.canRetryError(null);

        // assert
        assert.isFalse(result);
        done();
    });

    it("BaseDriver#closePool() calls callback", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        driver.closePool(null, done);
    });

    it("BaseDriver#createConnectionPool() calls callback", function (done) {
        // arrange
        var driver = getDefaultInstance();

        // act
        driver.createConnectionPool(null, done);
    });

    describe("BaseDriver#param()", function() {
      var driver;

      beforeEach(function() {
        driver = new getDefaultInstance();
        driver.dataType.timestamp = 1;
      });

      it('returns the value parameter if no type hint was provided', function() {
        expect(driver.param('foo')).to.equal('foo');
      });

      it('returns a hinted value wrapper if a type hint was provided', function() {
        var timestamp = new Date();
        var param = driver.param(timestamp, driver.dataType.timestamp);

        expect(param.value).to.equal(timestamp);
        expect(param.hint).to.equal(driver.dataType.timestamp);
      });

      it('returns a hinted value wrapper if a type hint was provided as a string', function() {
        var timestamp = new Date();
        var param = driver.param(timestamp, 'timestamp');

        expect(param.value).to.equal(timestamp);
        expect(param.hint).to.equal(driver.dataType.timestamp);
      });
    });
});

"use strict";

var logger = require("../../../lib/util/fakeLogger"),
    sinon = require('sinon'),
    chai = require('chai'),
    assert = chai.assert,
    expect = chai.expect;

describe("lib/util/fakeLogger.js", function(){

    beforeEach(function () {
        sinon.spy(console, "log");
        sinon.spy(console, "info");
        sinon.spy(console, "warn");
        sinon.spy(console, "error");
    });

    afterEach(function () {
        console.log.restore();
        console.info.restore();
        console.warn.restore();
        console.error.restore();
    });

    describe("module export", function () {
        it("returns required", function () {
            assert.isFunction(logger.info);
            assert.isFunction(logger.warn);
            assert.isFunction(logger.error);
            assert.isFunction(logger.debug);
            assert.isFunction(logger.critical);
        });

        it("does nothing", function () {
            logger.info("Test");
            assert.ok(console.info.notCalled);
            logger.warn("Test");
            assert.ok(console.warn.notCalled);
            logger.error("Test");
            assert.ok(console.error.notCalled);
            logger.debug("Test");
            assert.ok(console.log.notCalled);
            logger.critical("Test");
            assert.ok(console.log.notCalled);
        });
    });

    function testLogLevel(level) {
        describe("FakeLogger#" + level + "()", function () {

            it ("does nothing if callback not provided", function (done) {
                logger[level]("Test");
                assert.ok(console.info.notCalled);
                done();
            });

            it ("calls callback if provided", function (done) {
                logger[level]("Test", done);
            });

        });
    }
    testLogLevel("debug");
    testLogLevel("info");
    testLogLevel("warn");
    testLogLevel("error");
    testLogLevel("critical");
});
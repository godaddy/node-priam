"use strict";

var sinon = require('sinon'),
    chai = require("chai"),
    assert = chai.assert,
    expect = chai.expect,
    path = require("path"),
    q = require("q");

var Query = require("../../../lib/util/query");

describe("lib/util/query.js", function () {

    var query,
        db;

    beforeEach(function () {
        db = {
            poolConfig: {},
            param: function (value, hint) {
                return { value: value, hint: hint};
            },
            consistencyLevel: {
                one: 1,
                localQuorum: 2
            }
        };
        query = new Query(db);
    });

    describe("interface", function () {

        it("is a constructor function", function () {
            assert.strictEqual(typeof Query, "function", "is a constructor function");
        });

        it("throws error if db not provided", function (done) {
            // act
            expect(function () {
                var q = new Query();
            }).to.throw(Error);

            done();
        });
    });

    describe("constructed instance", function () {

        function validateFunctionExists(name, argCount) {
            // assert
            assert.strictEqual(typeof query[name], "function");
            assert.strictEqual(query[name].length, argCount, name + " takes " + argCount + " arguments");
        }

        it("provides a query function", function () {
            validateFunctionExists("query", 1);
        });

        it("provides a namedQuery function", function () {
            validateFunctionExists("namedQuery", 1);
        });

        it("provides a param function", function () {
            validateFunctionExists("param", 2);
        });

        it("provides a consistency function", function () {
            validateFunctionExists("consistency", 1);
        });

        it("provides an options function", function () {
            validateFunctionExists("options", 1);
        });

        it("provides an execute function", function () {
            validateFunctionExists("execute", 1);
        });

    });

    describe("#query()", function () {
        it("populates cql in context", function (done) {
            // arrange
            var cql = "SELECT * FROM mycolumnfamily";

            // act
            query.query(cql);

            // assert
            assert.strictEqual(query.context.cql, cql, "cql is populated");
            done();
        });

        it("returns self", function (done) {
            // arrange
            var cql = "SELECT * FROM mycolumnfamily";

            // act
            var result = query.query(cql);

            // assert
            assert.equal(result, query, "returns self");
            done();
        });
    });

    describe("#namedQuery()", function () {

        it("adds error to context if 'queryCache' does not exist on db", function (done) {
            // arrange
            query.db.queryCache = null;

            // act
            query.namedQuery("myQueryName");

            // assert
            assert.strictEqual(query.context.errors.length, 1, "errors is populated");
            done();
        });

        it("adds error to context if 'queryCache' yields error", function (done) {
            // arrange
            var qcError = new Error("queryCache blew up");
            query.db.queryCache = {
                readQuery: sinon.stub().yields(qcError)
            };

            // act
            query.namedQuery("myQueryName");

            // assert
            assert.strictEqual(query.context.errors.length, 1, "errors is populated");
            assert.equal(query.context.errors[0], qcError, "queryCache error is in errors list");
            done();
        });

        it("populates cql in context if 'queryCache' yields data", function (done) {
            // arrange
            var cql = "SELECT * FROM mycolumnfamily";
            query.db.queryCache = {
                readQuery: sinon.stub().yields(null, cql)
            };

            // act
            query.namedQuery("myQueryName");

            // assert
            assert.strictEqual(query.context.errors.length, 0, "errors is not populated");
            assert.strictEqual(query.context.cql, cql, "cql is populated");
            done();
        });

        it("populates queryName and executeAsPrepared in options if not already supplied", function (done) {
            // arrange
            var cql = "SELECT * FROM mycolumnfamily";
            db.poolConfig.supportsPreparedStatements = true;
            query.db.queryCache = {
                readQuery: sinon.stub().yields(null, cql)
            };

            // act
            query.namedQuery("myQueryName");

            // assert
            assert.strictEqual(query.context.errors.length, 0, "errors is not populated");
            assert.strictEqual(query.context.cql, cql, "cql is populated");
            assert.strictEqual(query.context.options.queryName, "myQueryName");
            assert.strictEqual(query.context.options.executeAsPrepared, true);
            done();
        });

        it("does not populate queryName and executeAsPrepared in options if already supplied", function (done) {
            // arrange
            var cql = "SELECT * FROM mycolumnfamily";
            db.poolConfig.supportsPreparedStatements = true;
            query.db.queryCache = {
                readQuery: sinon.stub().yields(null, cql)
            };
            query.options({
                queryName: "someOtherQueryName",
                executeAsPrepared: false
            });

            // act
            query.namedQuery("myQueryName");

            // assert
            assert.strictEqual(query.context.errors.length, 0, "errors is not populated");
            assert.strictEqual(query.context.cql, cql, "cql is populated");
            assert.strictEqual(query.context.options.queryName, "someOtherQueryName");
            assert.strictEqual(query.context.options.executeAsPrepared, false);
            done();
        });

        it("returns self", function (done) {
            // arrange
            query.db.queryCache = null;

            // act
            var result = query.namedQuery("myQueryName");

            // assert
            assert.equal(result, query, "returns self");
            done();
        });
    });

    describe("#param()", function () {
        it("adds a parameter to the context", function (done) {
            // arrange
            var param1 = { value: "myVal1", hint: "ascii"};
            var param2 = { value: 12345, hint: "int"};

            // act
            query.param(param1.value, param1.hint);
            query.param(param2.value, param2.hint);

            // assert
            assert.strictEqual(query.context.params.length, 2, "params is populated");
            assert.strictEqual(query.context.params[0].value, param1.value, "param1 value is populated");
            assert.strictEqual(query.context.params[0].hint, param1.hint, "param1 hint is populated");
            assert.strictEqual(query.context.params[1].value, param2.value, "param2 value is populated");
            assert.strictEqual(query.context.params[1].hint, param2.hint, "param2 hint is populated");
            done();
        });

        it("returns self", function (done) {
            // arrange
            var param1 = { value: "myVal1", hint: "ascii"};

            // act
            var result = query.param(param1.value, param1.hint);

            // assert
            assert.equal(result, query, "returns self");
            done();
        });
    });

    describe("#consistency()", function () {
        it("adds consistency level to the options context if valid consistency is given", function (done) {
            // arrange
            // act
            query.consistency("one");

            // assert
            assert.strictEqual(query.context.options.consistency, db.consistencyLevel.one, "consistency is populated");
            done();
        });

        it("does not add consistency level to the options context if invalid consistency is given", function (done) {
            // arrange
            // act
            query.consistency("someInvalidConsistency");

            // assert
            assert.notOk(query.context.options.consistency, "consistency is not populated");
            done();
        });

        it("returns self", function (done) {
            // arrange
            // act
            var result = query.consistency("one");

            // assert
            assert.equal(result, query, "returns self");
            done();
        });
    });

    describe("#options()", function () {
        it("adds extends the options context", function (done) {
            // arrange
            // act
            query.options({ one: "one" });
            query.options({ two: 2 });

            // assert
            assert.deepEqual(query.context.options, { one: "one", two: 2}, "options is populated");
            done();
        });

        it("returns self", function (done) {
            // arrange
            // act
            var result = query.options({ one: "one" });

            // assert
            assert.equal(result, query, "returns self");
            done();
        });
    });

    describe("#execute()", function () {

        beforeEach(function () {
            db.cql = sinon.stub().yields(null, [{}]);
        });

        it("returns void if callback supplied", function (done) {
            // arrange
            query.context.cql = "myCqlQuery";

            // act
            var result = query.execute(function (err, data) {});

            // assert
            assert.equal(result, void 0, "returns void");
            done();
        });

        it("returns promise if callback not supplied", function (done) {
            // arrange
            query.context.cql = "myCqlQuery";

            // act
            var result = query.execute({ one: "one" });

            // assert
            assert.ok(q.isPromise(result), "returns promise");
            done();
        });

        function testCallbacks(isPromise) {
            describe(isPromise ? "with promise" : "with callback", function () {
                it("yields error if cql is not populated", function (done) {
                    // arrange
                    query.context.cql = null;

                    // act
                    if (isPromise) {
                        var e = null,
                            result = null;
                        query
                            .execute()
                            .fail(function (error) {
                                e = error;
                            })
                            .done(function (data) {
                                if (e) { asserts (e); }
                                else { asserts(null, data); }
                            });
                    }
                    else {
                        query.execute(asserts);
                    }

                    // assert
                    function asserts (err, data) {
                        assert.ok(err, "error is populated");
                        assert.notOk(data, "data is not populated");
                        done();
                    }
                });

                it("yields error if db yields error", function (done) {
                    // arrange
                    query.context.cql = "myCqlStatement";
                    db.cql = sinon.stub().yields(new Error("Cassandra error"));

                    // act
                    if (isPromise) {
                        var e = null,
                            result = null;
                        query
                            .execute()
                            .fail(function (error) {
                                e = error;
                            })
                            .done(function (data) {
                                if (e) { asserts (e); }
                                else { asserts(null, data); }
                            });
                    }
                    else {
                        query.execute(asserts);
                    }

                    // assert
                    function asserts (err, data) {
                        assert.ok(err, "error is populated");
                        assert.notOk(data, "data is not populated");
                        done();
                    }
                });

                it("yields multiple errors if triggered", function (done) {
                    // arrange
                    db.cql = sinon.stub().yields(new Error("Cassandra error"));
                    query.db.queryCache = {
                        readQuery: sinon.stub().yields(new Error("query cache blew up"))
                    };
                    query.namedQuery("myQueryName"); // fails
                    query.context.cql = "myCqlStatement"; // set CQL so we can execute

                    // act
                    if (isPromise) {
                        var e = null,
                            result = null;
                        query
                            .execute()
                            .fail(function (error) {
                                e = error;
                            })
                            .done(function (data) {
                                if (e) { asserts (e); }
                                else { asserts(null, data); }
                            });
                    }
                    else {
                        query.execute(asserts);
                    }

                    // assert
                    function asserts (err, data) {
                        assert.ok(err, "error is populated");
                        assert.ok(Array.isArray(err.inner), "error inner array is populated");
                        assert.strictEqual(err.inner.length, 2, "error inner array is populated with 2 errors");
                        assert.notOk(data, "data is not populated");
                        done();
                    }
                });

                it("yields data if db yields data", function (done) {
                    // arrange
                    var data = [{}];
                    query.context.cql = "myCqlStatement";
                    db.cql = sinon.stub().yields(null, data);

                    // act
                    if (isPromise) {
                        var e = null,
                            result = null;
                        query
                            .execute()
                            .fail(function (error) {
                                e = error;
                            })
                            .done(function (data) {
                                if (e) { asserts (e); }
                                else { asserts(null, data); }
                            });
                    }
                    else {
                        query.execute(asserts);
                    }

                    // assert
                    function asserts (err, data) {
                        assert.notOk(err, "error is not populated");
                        assert.equal(data, data, "data is populated");
                        done();
                    }
                });
            });
        }

        testCallbacks(false);
        testCallbacks(true);
    });
});

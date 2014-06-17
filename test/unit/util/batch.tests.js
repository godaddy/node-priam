"use strict";

var sinon = require('sinon'),
  chai = require("chai"),
  assert = chai.assert,
  expect = chai.expect,
  q = require("q"),
  Query = require("../../../lib/util/query");

var Batch = require("../../../lib/util/batch");

describe("lib/util/batch.js", function () {

  var batch,
    db;

  beforeEach(function () {
    db = {
      poolConfig: {},
      param: function (value, hint) {
        return { value: value, hint: hint};
      },
      consistencyLevel: { // from helenus: cassandra_types.js
        one: 1,
        quorum: 2,
        localQuorum: 3,
        eachQuorum: 4,
        all: 5,
        any: 6,
        two: 7,
        three: 8
      }
    };
    batch = new Batch(db);
  });

  describe("interface", function () {

    it("is a constructor function", function () {
      assert.strictEqual(typeof Query, "function", "is a constructor function");
    });

    it("throws error if db not provided", function (done) {
      // act
      expect(function () {
        var b = new Batch();
      }).to.throw(Error);

      done();
    });
  });

  describe("constructed instance", function () {

    function validateFunctionExists(name, argCount) {
      // assert
      assert.strictEqual(typeof batch[name], "function");
      assert.strictEqual(batch[name].length, argCount, name + " takes " + argCount + " arguments");
    }

    it("provides an addQuery function", function () {
      validateFunctionExists("addQuery", 1);
    });

    it("provides a timestamp function", function () {
      validateFunctionExists("timestamp", 1);
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

  describe("#addQuery()", function () {
    it("adds the query to the query list", function (done) {
      // arrange
      var query = new Query(db);

      // act
      batch.addQuery(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, "query is added to list");
      assert.strictEqual(batch.context.queries[0], query, "query is added to list");
      assert.strictEqual(batch.context.errors.length, 0, "no error is generated");
      done();
    });

    it("adds error if query is not a valid Query object", function (done) {
      // arrange
      var query = { cql: "mySql", queryName: "myQueryName" };

      // act
      batch.addQuery(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, "query is NOT added to list");
      assert.strictEqual(batch.context.errors.length, 1, "error is added to list");
      done();
    });

    it("adds error if query is null", function (done) {
      // arrange
      var query = null;

      // act
      batch.addQuery(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, "query is NOT added to list");
      assert.strictEqual(batch.context.errors.length, 1, "error is added to list");
      done();
    });

    it("returns self", function (done) {
      // arrange
      var query = new Query(db);

      // act
      var result = batch.addQuery(query);

      // assert
      assert.equal(result, batch, "returns self");
      done();
    });
  });

  describe("#consistency()", function () {
    it("adds consistency level to the options context if valid consistency is given", function (done) {
      // arrange
      // act
      batch.consistency("one");

      // assert
      assert.strictEqual(batch.context.options.consistency, db.consistencyLevel.one, "consistency is populated");
      done();
    });

    it("does not add consistency level to the options context if invalid consistency is given", function (done) {
      // arrange
      // act
      batch.consistency("someInvalidConsistency");

      // assert
      assert.notOk(batch.context.options.consistency, "consistency is not populated");
      done();
    });

    it("returns self", function (done) {
      // arrange
      // act
      var result = batch.consistency("one");

      // assert
      assert.equal(result, batch, "returns self");
      done();
    });
  });

  describe("#timestamp()", function () {
    it("adds timestamp to context if given", function (done) {
      // arrange
      var timestamp = 1234567;

      // act
      batch.timestamp(timestamp);

      // assert
      assert.strictEqual(batch.context.timestamp, timestamp, "timestamp is populated");
      done();
    });

    it("adds default timestamp if not given", function (done) {
      // arrange
      // act
      batch.timestamp();

      // assert
      assert.ok(batch.context.timestamp, "timestamp is populated");
      done();
    });

    it("returns self", function (done) {
      // arrange
      // act
      var result = batch.timestamp();

      // assert
      assert.equal(result, batch, "returns self");
      done();
    });
  });

  describe("#options()", function () {
    it("extends the options context", function (done) {
      // arrange
      // act
      batch.options({ one: "one" });
      batch.options({ two: 2 });

      // assert
      assert.deepEqual(batch.context.options, { one: "one", two: 2}, "options is populated");
      done();
    });

    it("returns self", function (done) {
      // arrange
      // act
      var result = batch.options({ one: "one" });

      // assert
      assert.equal(result, batch, "returns self");
      done();
    });
  });

  describe("#type()", function () {
    it("defaults to standard", function (done) {
      // arrange
      // act
      // assert
      assert.strictEqual(batch.context.batchType, batch.batchType.standard, "batch type should be standard");
      done();
    });

    it("changes the batch type if valid type given", function (done) {
      // arrange
      // act
      batch.type('unlogged');

      // assert
      assert.strictEqual(batch.context.batchType, batch.batchType.unlogged, "batch type should be unlogged");
      done();
    });

    it("does not change the batch type if invalid type given", function (done) {
      // arrange
      // act
      batch.type("someInvalidBatchType");

      // assert
      assert.notOk(batch.context.options.batchType, "batchType is not populated");
      done();
    });

    it("returns self", function (done) {
      // arrange
      // act
      var result = batch.type('counter');

      // assert
      assert.equal(result, batch, "returns self");
      done();
    });
  });

  describe("#execute()", function () {

    beforeEach(function () {
      db.cql = sinon.stub().yields(null, [
        {}
      ]);
    });

    it("returns void if callback supplied", function (done) {
      // arrange
      var query1 = new Query(db),
        query2 = new Query(db);
      query1.context.cql = "myCqlQuery1";
      query2.context.cql = "myCqlQuery2";
      batch.context.queries = [query1, query2];

      // act
      var result = batch.execute(function (err, data) {
      });

      // assert
      assert.equal(result, void 0, "returns void");
      done();
    });

    it("returns promise if callback not supplied", function (done) {
      // arrange
      var query1 = new Query(db),
        query2 = new Query(db);
      query1.context.cql = "myCqlQuery1";
      query2.context.cql = "myCqlQuery2";
      batch.context.queries = [query1, query2];

      // act
      var result = batch.execute({ one: "one" });

      // assert
      assert.ok(q.isPromise(result), "returns promise");
      done();
    });

    function testCallbacks(isPromise) {
      describe(isPromise ? "with promise" : "with callback", function () {
        it("yields error if query list is not populated", function (done) {
          // arrange
          batch.context.queries = [];

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.ok(err, "error is populated");
            assert.notOk(data, "data is not populated");
            done();
          }
        });

        it("yields error if db yields error", function (done) {
          // arrange
          var query1 = new Query(db),
            query2 = new Query(db);
          query1.context.cql = "myCqlQuery1";
          query2.context.cql = "myCqlQuery2";
          batch.context.queries = [query1, query2];
          db.cql = sinon.stub().yields(new Error("Cassandra error"));

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.ok(err, "error is populated");
            assert.notOk(data, "data is not populated");
            done();
          }
        });

        it("yields multiple errors if triggered", function (done) {
          // arrange
          var query1 = new Query(db),
            query2 = { not: "a query"};
          query1.context.cql = "myCqlQuery1";
          db.cql = sinon.stub().yields(new Error("Cassandra error"));
          batch.addQuery(query1);
          batch.addQuery(query2); // fails

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.ok(err, "error is populated");
            assert.ok(Array.isArray(err.inner), "error inner array is populated");
            assert.strictEqual(err.inner.length, 2, "error inner array is populated with 2 errors");
            assert.notOk(data, "data is not populated");
            done();
          }
        });

        it("yields data if db yields data", function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db),
            query2 = new Query(db);
          query1.context.cql = "myCqlQuery1";
          query2.context.cql = "myCqlQuery2";
          batch.context.queries = [query1, query2];
          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.notOk(err, "error is not populated");
            assert.equal(data, data, "data is populated");
            done();
          }
        });

        it("joins queries correctly", function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query("myCqlQuery1")
              .consistency("localQuorum")
              .param("param1", "ascii")
              .param("param2", "ascii"),
            query2 = new Query(db)
              .query("myCqlQuery2;\n")
              .consistency("eachQuorum")
              .param("param3", "ascii")
              .param("param4", "ascii"),
            query3 = new Query(db)
              .query("myCqlQuery3")
              .options({ suppressDebugLog: true })
              .consistency("one")
              .param("param5", "ascii")
              .param("param6", "ascii");

          batch.context.queries = [query1, query2, query3];
          batch.timestamp(1234567);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.strictEqual(db.cql.callCount, 1, "cql is only called once");
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], "BEGIN BATCH\nUSING TIMESTAMP 1234567\nmyCqlQuery1;\nmyCqlQuery2;\n\nmyCqlQuery3;\nAPPLY BATCH;\n", "Query text is joined");
            assert.strictEqual(callArgs[1].length, 6, "Query params are joined");
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, "Strictest consistency is set");
            assert.strictEqual(callArgs[2].suppressDebugLog, true, "Debug log is suppressed");
            assert.notOk(err, "error is not populated");
            assert.equal(data, data, "data is populated");
            done();
          }
        });

        it("sets batch type appropriately", function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
            .query("myCqlQuery1")
            .consistency("localQuorum")
            .param("param1", "ascii")
            .param("param2", "ascii");

          batch.context.queries = [query1];
          batch.type('counter');

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.strictEqual(db.cql.callCount, 1, "cql is only called once");
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], "BEGIN COUNTER BATCH\nmyCqlQuery1;\nAPPLY BATCH;\n", "Query text is joined");
            assert.notOk(err, "error is not populated");
            assert.equal(data, data, "data is populated");
            done();
          }
        });

        it("ignores bad batch types", function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
            .query("myCqlQuery1")
            .consistency("localQuorum")
            .param("param1", "ascii")
            .param("param2", "ascii");

          batch.context.queries = [query1];
          batch.context.batchType = 123456789; //invalid

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null,
              result = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                }
                else {
                  asserts(null, data);
                }
              });
          }
          else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.strictEqual(db.cql.callCount, 1, "cql is only called once");
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], "BEGIN BATCH\nmyCqlQuery1;\nAPPLY BATCH;\n", "Query text is joined");
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



var sinon = require('sinon'),
  chai = require('chai'),
  assert = chai.assert,
  expect = chai.expect,
  q = require('q'),
  Query = require('../../../lib/util/query');

var Batch = require('../../../lib/util/batch');

describe('lib/util/batch.js', function () {

  var batch,
    db;

  beforeEach(function () {
    db = {
      poolConfig: {},
      config: { parsedCqlVersion: { major: 3, minor: 1, patch: 0 } },
      param: function (value, hint) {
        return { value: value, hint: hint };
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

  describe('interface', function () {

    it('is a constructor function', function () {
      assert.strictEqual(typeof Query, 'function', 'is a constructor function');
    });

    it('throws error if db not provided', function (done) {
      // act
      expect(function () { new Batch(); }).to.throw(Error);

      done();
    });
  });

  describe('constructed instance', function () {

    function validateFunctionExists(name, argCount) {
      // assert
      assert.strictEqual(typeof batch[name], 'function');
      assert.strictEqual(batch[name].length, argCount, `${name} takes ${argCount} arguments`);
    }

    it('provides an add function', function () {
      validateFunctionExists('add', 1);
    });

    it('provides an addQuery function', function () {
      validateFunctionExists('addQuery', 1);
    });

    it('provides an addBatch function', function () {
      validateFunctionExists('addBatch', 1);
    });

    it('provides a timestamp function', function () {
      validateFunctionExists('timestamp', 1);
    });

    it('provides a consistency function', function () {
      validateFunctionExists('consistency', 1);
    });

    it('provides an options function', function () {
      validateFunctionExists('options', 1);
    });

    it('provides an execute function', function () {
      validateFunctionExists('execute', 1);
    });

  });

  describe('#add()', function () {

    it('iterates an array', function () {
      // arrange
      var query1 = new Query(db);
      var query2 = new Query(db);
      var batch1 = new Batch(db);

      // act
      batch.add([query1, query2, batch1]);

      // assert
      assert.strictEqual(batch.context.queries.length, 3, 'queries are added to list');
      assert.strictEqual(batch.context.queries[0], query1, 'query1 is added to list');
      assert.strictEqual(batch.context.queries[1], query2, 'query2 is added to list');
      assert.strictEqual(batch.context.queries[2], batch1, 'batch1 is added to list of queries');
      assert.strictEqual(batch.context.errors.length, 0, 'no error is generated');
    });

    it('adds the query to the query list', function (done) {
      // arrange
      var query = new Query(db);

      // act
      batch.add(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, 'query is added to list');
      assert.strictEqual(batch.context.queries[0], query, 'query is added to list');
      assert.strictEqual(batch.context.errors.length, 0, 'no error is generated');
      done();
    });

    it('adds the batch to the query list', function (done) {
      // arrange
      var newBatch = new Batch(db);

      // act
      batch.add(newBatch);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, 'batch is added to list');
      assert.strictEqual(batch.context.queries[0], newBatch, 'batch is added to list of queries');
      assert.strictEqual(batch.context.errors.length, 0, 'no error is generated');
      done();
    });

    it('adds error if query is not a valid Query or Batch object', function (done) {
      // arrange
      var query = { cql: 'mySql', queryName: 'myQueryName' };

      // act
      batch.add(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'query is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('does not add error if query is null', function (done) {
      // arrange
      var query = null;

      // act
      batch.add(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'query is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 0, 'no error is generated');
      done();
    });

    it('adds error if query is undefined', function (done) {
      // arrange
      var query;

      // act
      batch.add(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'query is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('returns self', function (done) {
      // arrange
      var query = new Query(db);

      // act
      var result = batch.add(query);

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#addQuery()', function () {
    it('adds the query to the query list', function (done) {
      // arrange
      var query = new Query(db);

      // act
      batch.addQuery(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, 'query is added to list');
      assert.strictEqual(batch.context.queries[0], query, 'query is added to list');
      assert.strictEqual(batch.context.errors.length, 0, 'no error is generated');
      done();
    });

    it('adds error if query is not a valid Query object', function (done) {
      // arrange
      var query = { cql: 'mySql', queryName: 'myQueryName' };

      // act
      batch.addQuery(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'query is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('adds error if query is null', function (done) {
      // arrange
      var query = null;

      // act
      batch.addQuery(query);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'query is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('returns self', function (done) {
      // arrange
      var query = new Query(db);

      // act
      var result = batch.addQuery(query);

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#addBatch()', function () {
    it('adds the batch to the query list', function (done) {
      // arrange
      var newBatch = new Batch(db);

      // act
      batch.addBatch(newBatch);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, 'batch is added to list');
      assert.strictEqual(batch.context.queries[0], newBatch, 'batch is added to list of queries');
      assert.strictEqual(batch.context.errors.length, 0, 'no error is generated');
      done();
    });

    it('adds error if the added batch contains the batch it is being added to', function (done) {
      // arrange
      var newBatch = new Batch(db);
      newBatch.addBatch(batch);

      // act
      batch.addBatch(newBatch);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'batch is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('adds error if the added batch contains the batch it is being added to (nested)', function (done) {
      // arrange
      var newBatch1 = new Batch(db);
      var newBatch2 = new Batch(db);
      newBatch1.addBatch(newBatch2);
      newBatch2.addBatch(batch);

      // act
      batch.addBatch(newBatch1);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'batch is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('adds error if the added batch is already in the tree', function (done) {
      // arrange
      var newBatch = new Batch(db);
      batch.addBatch(newBatch);

      // act
      batch.addBatch(newBatch);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, 'batch is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('adds error if the added batch is already in the tree (nseted)', function (done) {
      // arrange
      var newBatch1 = new Batch(db);
      var newBatch2 = new Batch(db);
      newBatch1.addBatch(newBatch2);
      batch.addBatch(newBatch1);

      // act
      batch.addBatch(newBatch2);

      // assert
      assert.strictEqual(batch.context.queries.length, 1, 'batch is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('adds error if batch is not a valid Batch object', function (done) {
      // arrange
      var newBatch = { context: { queries: [] } };

      // act
      batch.addBatch(newBatch);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'batch is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('adds error if batch is null', function (done) {
      // arrange
      var newBatch = null;

      // act
      batch.addQuery(newBatch);

      // assert
      assert.strictEqual(batch.context.queries.length, 0, 'batch is NOT added to list');
      assert.strictEqual(batch.context.errors.length, 1, 'error is added to list');
      done();
    });

    it('returns self', function (done) {
      // arrange
      var newBatch = new Batch(db);

      // act
      var result = batch.addBatch(newBatch);

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#consistency()', function () {
    it('adds consistency level to the options context if valid consistency is given', function (done) {
      // arrange
      // act
      batch.consistency('one');

      // assert
      assert.strictEqual(batch.context.options.consistency, db.consistencyLevel.one, 'consistency is populated');
      done();
    });

    it('does not add consistency level to the options context if invalid consistency is given', function (done) {
      // arrange
      // act
      batch.consistency('someInvalidConsistency');

      // assert
      assert.notOk(batch.context.options.consistency, 'consistency is not populated');
      done();
    });

    it('returns self', function (done) {
      // arrange
      // act
      var result = batch.consistency('one');

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#timestamp()', function () {
    it('adds timestamp to context if given', function (done) {
      // arrange
      var timestamp = 1234567;

      // act
      batch.timestamp(timestamp);

      // assert
      assert.strictEqual(batch.context.timestamp, timestamp, 'timestamp is populated');
      done();
    });

    it('adds default timestamp if not given', function (done) {
      // arrange
      // act
      batch.timestamp();

      // assert
      assert.ok(batch.context.timestamp, 'timestamp is populated');
      done();
    });

    it('returns self', function (done) {
      // arrange
      // act
      var result = batch.timestamp();

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#options()', function () {
    it('extends the options context', function (done) {
      // arrange
      // act
      batch.options({ one: 'one' });
      batch.options({ two: 2 });

      // assert
      assert.deepEqual(batch.context.options, { one: 'one', two: 2 }, 'options is populated');
      done();
    });

    it('returns self', function (done) {
      // arrange
      // act
      var result = batch.options({ one: 'one' });

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#type()', function () {
    it('defaults to standard', function (done) {
      // arrange
      // act
      // assert
      assert.strictEqual(batch.context.batchType, batch.batchType.standard, 'batch type should be standard');
      done();
    });

    it('changes the batch type if valid type given', function (done) {
      // arrange
      // act
      batch.type('unlogged');

      // assert
      assert.strictEqual(batch.context.batchType, batch.batchType.unlogged, 'batch type should be unlogged');
      done();
    });

    it('does not change the batch type if invalid type given', function (done) {
      // arrange
      // act
      batch.type('someInvalidBatchType');

      // assert
      assert.notOk(batch.context.options.batchType, 'batchType is not populated');
      done();
    });

    it('returns self', function (done) {
      // arrange
      // act
      var result = batch.type('counter');

      // assert
      assert.equal(result, batch, 'returns self');
      done();
    });
  });

  describe('#execute()', function () {

    beforeEach(function () {
      db.cql = sinon.stub().yields(null, [
        {}
      ]);
    });

    it('returns void if callback supplied', function (done) {
      // arrange
      var query1 = new Query(db),
        query2 = new Query(db);
      query1.context.cql = 'myCqlQuery1';
      query2.context.cql = 'myCqlQuery2';
      batch.context.queries = [query1, query2];

      // act
      var result = batch.execute(function () { });

      // assert
      assert.equal(result, void 0, 'returns void');
      done();
    });

    it('returns promise if callback not supplied', function (done) {
      // arrange
      var query1 = new Query(db),
        query2 = new Query(db);
      query1.context.cql = 'myCqlQuery1';
      query2.context.cql = 'myCqlQuery2';
      batch.context.queries = [query1, query2];

      // act
      var result = batch.execute({ one: 'one' });

      // assert
      assert.ok(q.isPromise(result), 'returns promise');
      done();
    });

    function testCallbacks(isPromise) {
      describe(isPromise ? 'with promise' : 'with callback', function () {

        it('yields [] if query list is not populated', function (done) {
          // arrange
          batch.context.queries = [];

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                asserts(e, data);
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            expect(err).to.not.exist;
            expect(data).to.deep.equal([]);
            done();
          }
        });

        it('yields error if db yields error', function (done) {
          // arrange
          var query1 = new Query(db),
            query2 = new Query(db);
          query1.context.cql = 'myCqlQuery1';
          query2.context.cql = 'myCqlQuery2';
          batch.context.queries = [query1, query2];
          db.cql = sinon.stub().yields(new Error('Cassandra error'));

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.ok(err, 'error is populated');
            assert.notOk(data, 'data is not populated');
            done();
          }
        });

        it('yields multiple errors if triggered', function (done) {
          // arrange
          var query1 = new Query(db),
            query2 = { not: 'a query' };
          query1.context.cql = 'myCqlQuery1';
          db.cql = sinon.stub().yields(new Error('Cassandra error'));
          batch.addQuery(query1);
          batch.addQuery(query2); // fails

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.ok(err, 'error is populated');
            assert.ok(Array.isArray(err.inner), 'error inner array is populated');
            assert.strictEqual(err.inner.length, 2, 'error inner array is populated with 2 errors');
            assert.notOk(data, 'data is not populated');
            done();
          }
        });

        it('yields empty result set if no query text is available', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db),
            query2 = new Query(db);
          query1.context.cql = '';
          query2.context.cql = '';
          batch.context.queries = [query1, query2];
          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.notOk(err, 'error is not populated');
            assert.deepEqual(data, [], 'data is populated with empty array');
            done();
          }
        });

        it('yields data if db yields data', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db),
            query2 = new Query(db);
          query1.context.cql = 'myCqlQuery1';
          query2.context.cql = 'myCqlQuery2';
          batch.context.queries = [query1, query2];
          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.notOk(err, 'error is not populated');
            assert.equal(data, data, 'data is populated');
            done();
          }
        });

        it('joins queries correctly', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii');

          batch.context.queries = [query1, query2, query3];
          batch.timestamp(1234567);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nUSING TIMESTAMP ?\nmyCqlQuery1;\nmyCqlQuery2;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 7, 'Query params are joined with timestamp');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(data, result, 'result is populated');
            assert.equal(queryParams[0].value, 1234567, 'timestamp param is populated');
            for (var i = 1; i < queryParams.length; i++) {
              assert.equal(queryParams[i].value, `param${i}`, `query param ${i} is populated`);
            }
            done();
          }
        });

        it('can join a query with an empty batch', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query = new Query(db)
            .query('myCqlQuery1')
            .consistency('localQuorum')
            .param('param1', 'ascii')
            .param('param2', 'ascii');

          batch.add(query);
          batch.add(new Batch(db));
          batch.timestamp(1234567);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1\nUSING TIMESTAMP ?;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 3, 'Query params are joined with timestamp');
            assert.notOk(err, 'error is not populated');
            assert.equal(data, result, 'result is populated');
            assert.equal(queryParams[2].value, 1234567, 'timestamp param is populated');
            for (var i = 0; i < queryParams.length - 1; i++) {
              assert.equal(queryParams[i].value, `param${i + 1}`, `query param ${i + 1} is populated`);
            }
            done();
          }
        });

        it('can join a query with an empty query', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query = new Query(db)
            .query('myCqlQuery1')
            .consistency('localQuorum')
            .param('param1', 'ascii')
            .param('param2', 'ascii');

          batch.add(query);
          batch.add(new Query(db));
          batch.timestamp(1234567);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nUSING TIMESTAMP ?\nmyCqlQuery1;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 3, 'Query params are joined with timestamp');
            assert.notOk(err, 'error is not populated');
            assert.equal(data, result, 'result is populated');
            assert.equal(queryParams[0].value, 1234567, 'timestamp param is populated');
            for (var i = 1; i < queryParams.length; i++) {
              assert.equal(queryParams[i].value, `param${i}`, `query param ${i} is populated`);
            }
            done();
          }
        });

        it('can join a query with an empty string query', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query = new Query(db)
            .query('myCqlQuery1')
            .consistency('localQuorum')
            .param('param1', 'ascii')
            .param('param2', 'ascii');

          batch.add(query);
          batch.add(new Query(db).query(''));
          batch.timestamp(1234567);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nUSING TIMESTAMP ?\nmyCqlQuery1;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 3, 'Query params are joined with timestamp');
            assert.notOk(err, 'error is not populated');
            assert.equal(data, result, 'result is populated');
            assert.equal(queryParams[0].value, 1234567, 'timestamp param is populated');
            for (var i = 1; i < queryParams.length; i++) {
              assert.equal(queryParams[i].value, `param${i}`, `query param ${i} is populated`);
            }
            done();
          }
        });

        it('sets batch type appropriately', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
            .query('myCqlQuery1')
            .consistency('localQuorum')
            .param('param1', 'ascii')
            .param('param2', 'ascii');

          batch.context.queries = [query1];
          batch.type('counter');

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN COUNTER BATCH\nmyCqlQuery1;\nAPPLY BATCH;\n', 'Query text is joined');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            done();
          }
        });

        it('ignores bad batch types', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
            .query('myCqlQuery1')
            .consistency('localQuorum')
            .param('param1', 'ascii')
            .param('param2', 'ascii');

          batch.context.queries = [query1];
          batch.context.batchType = 123456789; // invalid

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1;\nAPPLY BATCH;\n', 'Query text is joined');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            done();
          }
        });

        it('joins nested batches correctly', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch = new Batch(db);

          childBatch.addQuery(query2);
          childBatch.addQuery(query3);
          batch.addQuery(query1);
          batch.addBatch(childBatch);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1;\nmyCqlQuery2;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 6, 'Query params are joined');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            for (var i = 0; i < queryParams.length; i++) {
              var num = i + 1;
              assert.equal(queryParams[i].value, `param${num}`, `query param ${num} is populated`);
            }
            done();
          }
        });

        it('joins nested batches correctly through multiple levels', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch1 = new Batch(db),
            childBatch2 = new Batch(db);

          batch.addQuery(query1);
          batch.addBatch(childBatch1);
          childBatch1.addQuery(query2);
          childBatch1.addBatch(childBatch2);
          childBatch2.addQuery(query3);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1;\nmyCqlQuery2;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 6, 'Query params are joined');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            for (var i = 0; i < queryParams.length; i++) {
              var num = i + 1;
              assert.equal(queryParams[i].value, `param${num}`, `query param ${num} is populated`);
            }
            done();
          }
        });

        it('joins nested batches correctly with empty batches through multiple levels', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch1 = new Batch(db),
            childBatch2 = new Batch(db);

          batch.addQuery(query1);
          batch.addBatch(childBatch1);
          batch.addBatch(new Batch(db));
          childBatch1.addBatch(new Batch(db));
          childBatch1.addQuery(query2);
          childBatch1.addBatch(childBatch2);
          childBatch2.addQuery(query3);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1;\nmyCqlQuery2;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 6, 'Query params are joined');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            for (var i = 0; i < queryParams.length; i++) {
              var num = i + 1;
              assert.equal(queryParams[i].value, `param${num}`, `query param ${num} is populated`);
            }
            done();
          }
        });

        it('joins empty nested batches correctly through multiple levels', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch1 = new Batch(db),
            childBatch2 = new Batch(db);

          batch.addQuery(query1);
          batch.addBatch(childBatch2);
          childBatch1.addQuery(query2);
          childBatch1.addQuery(query3);
          childBatch2.addBatch(childBatch1);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.notOk(db.cql.called, 'cql is NOT called');
            assert.notOk(err, 'error is not populated');
            assert.deepEqual(result, [], 'result is populated with empty array');
            done();
          }
        });

        it('joins nested batches correctly when the parent has a timestamp and child does not', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch = new Batch(db);

          childBatch.addQuery(query2);
          childBatch.addQuery(query3);
          batch.addQuery(query1);
          batch.addBatch(childBatch);
          batch.timestamp(1234567);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, data) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1\nUSING TIMESTAMP ?;\nmyCqlQuery2\nUSING TIMESTAMP ?;\nmyCqlQuery3\nUSING TIMESTAMP ?;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 9, 'Query params are joined with timestamps');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(data, data, 'data is populated');
            assert.equal(queryParams[0].value, 'param1', 'query param 1 is populated');
            assert.equal(queryParams[1].value, 'param2', 'query param 2 is populated');
            assert.equal(queryParams[2].value, 1234567, 'query timestamp 1 is populated');
            assert.equal(queryParams[3].value, 'param3', 'query param 3 is populated');
            assert.equal(queryParams[4].value, 'param4', 'query param 4 is populated');
            assert.equal(queryParams[5].value, 1234568, 'query timestamp 2 is populated');
            assert.equal(queryParams[6].value, 'param5', 'query param 5 is populated');
            assert.equal(queryParams[7].value, 'param6', 'query param 6 is populated');
            assert.equal(queryParams[8].value, 1234568, 'query timestamp 3 is populated');
            done();
          }
        });

        it('sets nested batch timestamps correctly', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch = new Batch(db);

          childBatch.addQuery(query2);
          childBatch.addQuery(query3);
          childBatch.timestamp(1234567);
          batch.addQuery(query1);
          batch.addBatch(childBatch);
          batch.timestamp(7654321);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1\nUSING TIMESTAMP ?;\nmyCqlQuery2\nUSING TIMESTAMP ?;\nmyCqlQuery3\nUSING TIMESTAMP ?;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 9, 'Query params are joined with timestamps');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            assert.equal(queryParams[0].value, 'param1', 'query param 1 is populated');
            assert.equal(queryParams[1].value, 'param2', 'query param 2 is populated');
            assert.equal(queryParams[2].value, 7654321, 'query timestamp 1 is populated');
            assert.equal(queryParams[3].value, 'param3', 'query param 3 is populated');
            assert.equal(queryParams[4].value, 'param4', 'query param 4 is populated');
            assert.equal(queryParams[5].value, 1234567, 'query timestamp 2 is populated');
            assert.equal(queryParams[6].value, 'param5', 'query param 5 is populated');
            assert.equal(queryParams[7].value, 'param6', 'query param 6 is populated');
            assert.equal(queryParams[8].value, 1234567, 'query timestamp 3 is populated');
            done();
          }
        });

        it('sets nested batch timestamps correctly for CQL versions that don\'t support DML timestamps inside of a batch', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('myCqlQuery2;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch = new Batch(db);

          childBatch.addQuery(query2);
          childBatch.addQuery(query3);
          childBatch.timestamp(1234567);
          batch.addQuery(query1);
          batch.addBatch(childBatch);
          batch.timestamp(7654321);

          db.config.parsedCqlVersion = { major: 3, minor: 0, patch: 0 };
          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nUSING TIMESTAMP ?\nmyCqlQuery1;\nmyCqlQuery2;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 7, 'Query params are joined with timestamps');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            assert.equal(queryParams[0].value, 7654321, 'query timestamp is populated');
            assert.equal(queryParams[1].value, 'param1', 'query param 1 is populated');
            assert.equal(queryParams[2].value, 'param2', 'query param 2 is populated');
            assert.equal(queryParams[3].value, 'param3', 'query param 3 is populated');
            assert.equal(queryParams[4].value, 'param4', 'query param 4 is populated');
            assert.equal(queryParams[5].value, 'param5', 'query param 5 is populated');
            assert.equal(queryParams[6].value, 'param6', 'query param 6 is populated');
            done();
          }
        });

        it('sets nested batch timestamps correctly for queries that don\'t support DML timestamps inside of a batch', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            query2 = new Query(db)
              .query('UPDATE something;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            childBatch = new Batch(db);

          childBatch.addQuery(query2);
          childBatch.addQuery(query3);
          childBatch.timestamp(1234567);
          batch.addQuery(query1);
          batch.addBatch(childBatch);
          batch.timestamp(7654321);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nUSING TIMESTAMP ?\nmyCqlQuery1;\nUPDATE something;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 7, 'Query params are joined with timestamps');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            assert.equal(queryParams[0].value, 7654321, 'query timestamp is populated');
            assert.equal(queryParams[1].value, 'param1', 'query param 1 is populated');
            assert.equal(queryParams[2].value, 'param2', 'query param 2 is populated');
            assert.equal(queryParams[3].value, 'param3', 'query param 3 is populated');
            assert.equal(queryParams[4].value, 'param4', 'query param 4 is populated');
            assert.equal(queryParams[5].value, 'param5', 'query param 5 is populated');
            assert.equal(queryParams[6].value, 'param6', 'query param 6 is populated');
            done();
          }
        });

        it('sets nested batch timestamps correctly for child batches that don\'t support DML timestamps inside of a batch', function (done) {
          // arrange
          var data = [
            {}
          ];
          var query1 = new Query(db)
              .query('myCqlQuery1')
              .consistency('localQuorum')
              .param('param1', 'ascii')
              .param('param2', 'ascii'),
            insertQuery1 = new Query(db)
              .query('INSERT something;\n')
              .consistency('eachQuorum')
              .param('param3', 'ascii')
              .param('param4', 'ascii'),
            deleteQuery = new Query(db)
              .query('DELETE something;\n')
              .consistency('eachQuorum')
              .param('param5', 'ascii')
              .param('param6', 'ascii'),
            insertQuery2 = new Query(db)
              .query('INSERT something;\n')
              .consistency('eachQuorum')
              .param('param7', 'ascii')
              .param('param8', 'ascii'),
            query3 = new Query(db)
              .query('myCqlQuery3')
              .options({ suppressDebugLog: true })
              .consistency('one')
              .param('param9', 'ascii')
              .param('param10', 'ascii'),
            childBatch = new Batch(db);

          childBatch.addQuery(insertQuery1);
          childBatch.addQuery(deleteQuery);
          childBatch.addQuery(insertQuery2);
          childBatch.timestamp(1234567);
          batch.addQuery(query1);
          batch.addBatch(childBatch);
          batch.addQuery(query3);
          batch.timestamp(7654321);

          db.cql = sinon.stub().yields(null, data);

          // act
          if (isPromise) {
            var e = null;
            batch
              .execute()
              .catch(function (error) {
                e = error;
              })
              .done(function (data) {
                if (e) {
                  asserts(e);
                } else {
                  asserts(null, data);
                }
              });
          } else {
            batch.execute(asserts);
          }

          // assert
          function asserts(err, result) {
            assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
            var callArgs = db.cql.getCall(0).args;
            assert.strictEqual(callArgs[0], 'BEGIN BATCH\nUSING TIMESTAMP ?\nmyCqlQuery1;\nINSERT something;\nDELETE something;\nINSERT something;\nmyCqlQuery3;\nAPPLY BATCH;\n', 'Query text is joined');
            var queryParams = callArgs[1];
            assert.strictEqual(queryParams.length, 11, 'Query params are joined with timestamps');
            assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
            assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
            assert.notOk(err, 'error is not populated');
            assert.equal(result, data, 'result is populated');
            assert.equal(queryParams[0].value, 7654321, 'query timestamp is populated');
            assert.equal(queryParams[1].value, 'param1', 'query param 1 is populated');
            assert.equal(queryParams[2].value, 'param2', 'query param 2 is populated');
            assert.equal(queryParams[3].value, 'param3', 'query param 3 is populated');
            assert.equal(queryParams[4].value, 'param4', 'query param 4 is populated');
            assert.equal(queryParams[5].value, 'param5', 'query param 5 is populated');
            assert.equal(queryParams[6].value, 'param6', 'query param 6 is populated');
            assert.equal(queryParams[7].value, 'param7', 'query param 7 is populated');
            assert.equal(queryParams[8].value, 'param8', 'query param 8 is populated');
            assert.equal(queryParams[9].value, 'param9', 'query param 9 is populated');
            assert.equal(queryParams[10].value, 'param10', 'query param 10 is populated');
            done();
          }
        });
      });
    }

    testCallbacks(false);
    testCallbacks(true);

    describe('sets timestamp to current time', function () {
      var clock;
      beforeEach(function () {
        clock = this.clock = sinon.useFakeTimers();
      });

      afterEach(function () {
        clock.restore();
      });

      // NOTE: Fake timers break Promises, so can only test coverage on this for standard execute.

      it('and propagates to child batches appropriately', function (done) {
        // arrange
        var data = [
          {}
        ];
        clock.tick(123456789);
        var nowTs = (Date.now() * 1000);
        var query1 = new Query(db)
            .query('myCqlQuery1')
            .consistency('localQuorum')
            .param('param1', 'ascii')
            .param('param2', 'ascii'),
          query2 = new Query(db)
            .query('myCqlQuery2;\n')
            .consistency('eachQuorum')
            .param('param3', 'ascii')
            .param('param4', 'ascii'),
          query3 = new Query(db)
            .query('myCqlQuery3')
            .options({ suppressDebugLog: true })
            .consistency('one')
            .param('param5', 'ascii')
            .param('param6', 'ascii'),
          childBatch = new Batch(db);

        childBatch.addQuery(query3);
        childBatch.timestamp();
        batch.addQuery(query1);
        batch.addQuery(query2);
        batch.addBatch(childBatch);
        batch.timestamp();

        db.cql = sinon.stub().yields(null, data);

        // act
        batch.execute(asserts);

        // assert
        function asserts(err, data) {
          assert.strictEqual(db.cql.callCount, 1, 'cql is only called once');
          var callArgs = db.cql.getCall(0).args;
          assert.strictEqual(callArgs[0], 'BEGIN BATCH\nmyCqlQuery1\nUSING TIMESTAMP ?;\nmyCqlQuery2\nUSING TIMESTAMP ?;\nmyCqlQuery3\nUSING TIMESTAMP ?;\nAPPLY BATCH;\n', 'Query text is joined');
          var queryParams = callArgs[1];
          assert.strictEqual(queryParams.length, 9, 'Query params are joined with timestamps');
          assert.strictEqual(callArgs[2].consistency, db.consistencyLevel.eachQuorum, 'Strictest consistency is set');
          assert.strictEqual(callArgs[2].suppressDebugLog, true, 'Debug log is suppressed');
          assert.notOk(err, 'error is not populated');
          assert.equal(data, data, 'data is populated');
          assert.equal(queryParams[0].value, 'param1', 'query param 1 is populated');
          assert.equal(queryParams[1].value, 'param2', 'query param 2 is populated');
          assert.equal(queryParams[2].value, nowTs, 'query timestamp 1 is populated');
          assert.equal(queryParams[3].value, 'param3', 'query param 3 is populated');
          assert.equal(queryParams[4].value, 'param4', 'query param 4 is populated');
          assert.equal(queryParams[5].value, nowTs, 'query timestamp 2 is populated');
          assert.equal(queryParams[6].value, 'param5', 'query param 5 is populated');
          assert.equal(queryParams[7].value, 'param6', 'query param 6 is populated');
          assert.equal(queryParams[8].value, nowTs + 1, 'query timestamp 3 is populated');
          done();
        }
      });
    });

  });
});

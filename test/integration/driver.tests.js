'use strict';

var assert = require('assert');

var Driver = require('../../lib/drivers/datastax');

describe('integration tests', function () {

  function bootCassandra(cb) {
    console.log('cassandra booted');
    cb();
  }


  function shutdownCassandra(cb) {
    console.log('cassandra shut down');
    cb();
  }

  before(function (done) {
    bootCassandra(done);
  });


  after(function (done) {
    shutdownCassandra(done);
  });

  describe('dummy test', function () {
    it('should assert something silly', function (done) {

      var config = {
        keyspace: 'myKeySpace',
        limit: 5000,
        getAConnectionTimeout: 12345,
        version: '3.1.0',
        contactPoints: ['127.0.0.1'],
        supportsPreparedStatements: true
      };

      var instance = new Driver({ config: config });
      assert(instance);
      done();
    });
  });
});

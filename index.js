'use strict';

/*
// wire up zones if available
if (global.zone) {
    var BaseDriver = require("./lib/drivers/base-driver"),
        zone = global.zone;

    var origCql = BaseDriver.prototype.cql;
    BaseDriver.prototype.cql = function executeCqlQueryWithZone(cqlQuery, dataParams, options, callback) {
        zone.create(function PriamExecuteCql() {
            if (typeof options !== "function") {
                zone.data.options = options;
            }
            origCql.call(this, cqlQuery, dataParams, options, zone.complete);
        }).setCallback(callback);
    };

    var origGetConnection = BaseDriver.prototype.getConnectionPool;
    BaseDriver.prototype.getConnectionPool = function getConnectionPoolWithZone(keyspace, callback) {
        zone.create(function PriamGetConnection() {
            origGetConnection.call(this, keyspace, zone.complete);
        }).setCallback(callback);
    };
}
*/

var driver = require('./lib/driver');
module.exports = driver;
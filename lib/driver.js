"use strict";

function Driver(context) {
    if (context && context.config && context.config.driver === "helenus") {
        return new (require("./drivers/helenus"))(context);
    }
    else {
        return new (require("./drivers/node-cassandra-cql"))(context);
    }
}

exports = module.exports = function (context) {
    return Driver(context);
};

"use strict";

var express = require("express"),
    path = require("path"),
    ConnectionResolver = require("./connectionResolver"),
    MetricsClient = require("./metricsClient"),
    winston = require("winston"),
    logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({ level: 'debug' })
        ]
    }),
    metrics = new MetricsClient({ logger: logger }),
    db = require("../index" /*"priam"*/)({
        config: {
            driver: "helenus",
            queryDirectory: path.join(__dirname, "cql"),

            // If using config-based connection, use these options
            user: "<your_username>",
            password: "<your_password>",
            keyspace: "<your_keyspace>",
            hosts: [
                "123.456.789.010", // your host IP's should be here
                "123.456.789.011",
                "123.456.789.012",
                "123.456.789.013"
            ]
        },
        logger: logger, // optional
        metrics: metrics, // optional

        // If using resolver-based connection, use this option
        connectionResolver: new ConnectionResolver({ pollInterval: 3000 }) // this will override any matching config options
    }),
    app = express(),
    port = 8080;

app.get("/", function (req, res) {
    var params = [
        "value1", // maps to 'param1' in 'helloWorld.cql'
        "value2"  // maps to 'param2' in 'helloWorld.cql'
    ];
    db.namedQuery("helloWorld", params, function (err, data) {
        if (err) {
            res.statusCode = 500;
            res.send(err.stack);
        }
        else {
            res.send(data);
        }
    });
});

app.listen(port, function () {
    logger.info("Express server started on port %s", port);
});

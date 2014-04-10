"use strict";

var http = require("http"),
    path = require("path"),
    ConnectionResolver = require("./lib/connectionResolver"),
    MetricsClient = require("./lib/metricsClient"),
    winston = require("winston"),
    logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({ level: 'debug' })
        ]
    }),
    metrics = new MetricsClient({ logger: logger }),
    db = require("../index" /*"priam"*/)({
        config: {
            driver: "node-cassandra-cql", //"helenus"
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
    port = 8080;

http.createServer(function (req, res) {
    db.beginQuery()
        .param("hello", "ascii") // maps to 'column1' placeholder in 'helloWorld.cql'
        .param("world", "ascii") // maps to 'column2' placeholder in 'helloWorld.cql'
        .namedQuery("helloWorld")
        .execute(function (err, data) {
            var statusCode = 200,
                message = null;
            if (err) {
                statusCode = 500;
                message = "If you're getting this error message, please ensure the following:\n\n" +
                    " - The data in '/example/lib/credentials.json' is updated with your connection information.\n" +
                    " - You have executed the '/example/cql/create_db.cql' in your keyspace.\n\n";
                if (Array.isArray(err.inner)) {

                }
                else {
                    message += JSON.stringify({ message: err.name, info: err.info, stack: err.stack });
                }
            }
            else {
                message = (Array.isArray(data) && data.length) ?
                    (data[0].column1 + " " + data[0].column2 + "!") :
                    "NO DATA FOUND! Please execute '/example/cql/create_db.cql' in your keyspace."
            }
            res.writeHead(statusCode, {"Content-Type": "text/plain"});
            res.end(message);
        });
}).listen(port);

logger.info("Node HTTP server listening at port %s", port);
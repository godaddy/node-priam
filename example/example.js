/* eslint-disable no-console */
const http               = require('http');
const util               = require('util');
const url                = require('url');
const path               = require('path');
const ConnectionResolver = require('./lib/connection-resolver');
const MetricsClient      = require('./lib/metrics-client');
const winston            = require('winston');
const _                  = require('lodash');
const logger             = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({ level: 'debug' })
  ]
});
const metrics            = new MetricsClient({ logger: logger });
const db                 = require('../index' /* "priam"*/)({
  config: {
    protocolOptions: {
      maxVersion: '3.1'
    },
    queryDirectory: path.join(__dirname, 'cql'),

    // If using config-based connection, use these options
    credentials: {
      username: 'admin',
      password: 'admin'
    },
    // keyspace: 'priam_test_db',
    contactPoints: [
      '127.0.0.1', // your host IP's should be here
      '127.0.0.2',
      '127.0.0.3',
      '127.0.0.4',
      '127.0.0.5'
    ]
  },
  logger: logger, // optional
  metrics: metrics, // optional

  // If using resolver-based connection, use this option
  connectionResolver: new ConnectionResolver({ pollInterval: 3000 }) // this will override any matching config options
});
const port               = 8080;

console.log(util.inspect(db, { showHidden: true, depth: null, colors: true }));

http.createServer(function (req, res) {
  var parsed = url.parse(req.url, true);
  var shouldStreamData = (parsed.query.stream && parsed.query.stream.toLowerCase() === 'true');

  function errorHandler(err) {
    if (err) {
      var statusCode = 500;
      var message = `If you're getting this error message, please ensure the following:
- The data in "/example/lib/credentials.json" is updated with your connection information.
- You have executed the "/example/cql/create-db.cql" in your keyspace.
`;
      if (Array.isArray(err.inner)) {
        _.each(err.inner, function (innerErr) {
          message += '------------------------\n';
          message += JSON.stringify({ message: innerErr.name, info: innerErr.info, stack: innerErr.stack });
          message += '\n';
        });
      } else {
        message += '------------------------\n';
        message += JSON.stringify({ message: err.name, info: err.info, stack: err.stack });
      }
      res.writeHead(statusCode, { 'Content-Type': 'text/plain' });
      res.end(message);
    }
  }

  // Batching inserts/updates demo
  db.beginBatch()
    .addQuery(db.beginQuery()
      .param('hello from Priam - batch query 1', 'ascii') // maps to 'column1' placeholder in 'addTimestamp.cql'
      .param((new Date()).toISOString(), 'ascii') // maps to 'column2' placeholder in 'addTimestamp.cql'
      .namedQuery('add-timestamp')
    ).addQuery(db.beginQuery()
      .param('hello from Priam - batch query 2', 'ascii') // maps to 'column1' placeholder in 'addTimestamp.cql'
      .param((new Date()).toISOString(), 'ascii') // maps to 'column2' placeholder in 'addTimestamp.cql'
      .namedQuery('add-timestamp')
    ).addQuery(db.beginQuery()
      .param({ key3: (new Date()).toISOString() }, 'map<text, text>') // maps to 'column3' placeholder in 'updateWorld.cql'
      .param('hello', 'ascii') // maps to 'column1' placeholder in 'updateWorld.cql'
      .param('world', 'ascii') // maps to 'column2' placeholder in 'updateWorld.cql'
      .namedQuery('update-world')
    ).timestamp()
    .options({ queryName: 'hello-world-writes' })
    .execute() // This will execute the two inserts above in a single batch
    .fail(errorHandler)
    .done(function () {
      // When insert batch completes, execute select

      if (shouldStreamData) {
        // Read the data from a stream!
        var data = [];
        db.beginQuery()
          .param('hello', 'ascii', true) // maps to 'column1' placeholder in 'helloWorld.cql'
          .param('world', 'ascii') // maps to 'column2' placeholder in 'helloWorld.cql'
          .namedQuery('hello-world')
          .stream()
          .on('error', errorHandler)
          .on('data', data.push.bind(data))
          .on('end', function () {
            if (res.headersSent) {
              return;
            }
            var message = data.length ?
              `${data[0].column1} ${data[0].column2} - from stream! Map: ${JSON.stringify(data[0].column3)}` :
              'NO DATA FOUND! Please execute "/example/cql/create-db.cql" in your keyspace.';
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end(message);
          });
      } else {
        // Read the data from a Promise!
        db.beginQuery()
          .param('hello', 'ascii', true) // maps to 'column1' placeholder in 'helloWorld.cql'
          .param('world', 'ascii') // maps to 'column2' placeholder in 'helloWorld.cql'
          .namedQuery('hello-world')
          .execute()
          .fail(errorHandler)
          .done(function (data) {
            var message = (Array.isArray(data) && data.length) ?
              `${data[0].column1} ${data[0].column2} - from Promise! Map: ${JSON.stringify(data[0].column3)}` :
              'NO DATA FOUND! Please execute "/example/cql/create-db.cql" in your keyspace.';
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end(message);
          });
      }
    });
}).listen(port);

logger.info('Node HTTP server listening at port %s', port);

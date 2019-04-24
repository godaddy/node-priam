

function MetricsClient(context) {
  context = context || {};
  this.logger = context.logger;
}

MetricsClient.prototype.measurement = function measure(queryName, duration, unit) {
  if (!this.logger) {
    return;
  }
  this.logger.debug('Metrics: %s took %s %s to execute.', queryName, duration, unit);
};

exports = module.exports = MetricsClient;

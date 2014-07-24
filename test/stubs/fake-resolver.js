"use strict";

var sinon = require('sinon');

function FakeResolver() {
  if (!(this instanceof FakeResolver)) {
    return new FakeResolver();
  }
  this.resolveConnection = sinon.stub().yields(null, {});
  this.on = sinon.stub();
}

module.exports = FakeResolver;
/**
 * Created by scommisso on 1/3/14.
 */

var sinon = require("sinon");

function FakeResolver() {
    if (!(this instanceof FakeResolver)) {
        return new FakeResolver();
    }
    this.resolveConnection = sinon.stub().yields(null, {});
    this.on = sinon.stub();
}

module.exports = FakeResolver;
REPORTER = spec

lint:
	./node_modules/.bin/jshint ./lib ./test ./example ./index.js

test:
	$(MAKE) lint
	@NODE_ENV=test ./node_modules/.bin/mocha -b --reporter $(REPORTER)

test-cov:
	$(MAKE) lint
	@NODE_ENV=test ./node_modules/.bin/nyc ./node_modules/mocha/bin/_mocha -R spec

test-coveralls:
	echo TRAVIS_JOB_ID $(TRAVIS_JOB_ID)
	$(MAKE) test
	@NODE_ENV=test ./node_modules/.bin/nyc --report lcovonly ./node_modules/mocha/bin/_mocha -R spec && \
		cat ./coverage/lcov.info | ./node_modules/coveralls/bin/coveralls.js || true

.PHONY: test

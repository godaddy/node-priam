var
  path        = require('path'),
  gulp        = require('gulp'),
  eslint      = require('gulp-eslint'),
  mocha       = require('gulp-mocha'),
  util        = require('gulp-util'),
  del         = require('del'),
  jsdoc       = require('gulp-jsdoc3'),
  sonar       = require('gulp-sonar2'),
  istanbul    = require('gulp-istanbul'),
  runSequence = require('run-sequence'),
  minimist    = require('minimist');

var MOCHA_REPORTER = 'spec';
var SRC = 'lib';
var TEST = 'test';
var BUILD = 'build';
var API_DOCS = 'build' + path.sep + 'api';

var knownOptions = {
  string: 'sonarUrl',
  default: { sonarUrl: 'http://localhost:9000' }
};

var options = minimist(process.argv.slice(2), knownOptions);
var pkg = require('./package.json');

gulp.task('clean', function (cb) {
  del([BUILD, '.sonar'], cb);
});

gulp.task('unit', function () {
  return gulp.src(TEST + path.sep + 'unit/**/*.js')
    .pipe(mocha({
      reporter: MOCHA_REPORTER
    }))
    .on('error', util.log);
});

gulp.task('mocha', function () {
  return gulp.src(TEST + '/**/*.js')
    .pipe(mocha({
      reporter: MOCHA_REPORTER
    }))
    .on('error', util.log);
});

gulp.task('watch', function () {
  gulp.watch([SRC + path.sep + '**', TEST + path.sep + '**'], ['mocha']);

});

gulp.task('lint', function () {
  return gulp.src(SRC + '/**/*.js')
    .pipe(eslint({ fix: true }));
});

gulp.task('apidocs', function (cb) {
  gulp
    .src(SRC + '/**/*.js')
    .pipe(jsdoc({
      opts: {
        destination: API_DOCS
      }
    }, cb));
});

gulp.task('test', function (cb) {
  gulp.src([SRC + '/**/*.js'])
    .pipe(istanbul())
    .pipe(istanbul.hookRequire())
    .on('finish', function () {
      gulp.src([TEST + path.sep + '**' + path.sep + '*.js'])
        .pipe(mocha())
        .pipe(istanbul.writeReports({
          dir: BUILD + path.sep + 'coverage',
          reporters: ['lcov', 'json', 'text', 'text-summary'],
          reportOpts: { dir: BUILD + path.sep + 'coverage' }
        })).on('end', cb);
    });
});

gulp.task('sonar', function () {
  var sonarOptions = {
    sonar: {
      host: {
        url: options.sonarUrl
      },
      projectKey: pkg.name,
      projectName: pkg.name,
      projectVersion: pkg.version,
      sources: SRC,
      language: 'js',
      sourceEncoding: 'UTF-8',
      javascript: {
        lcov: {
          reportPath: BUILD + path.sep + 'coverage' + path.sep + 'lcov.info'
        }
      }
    }
  };

  return gulp.src('thisFileDoesNotExist.js', { read: false })
    .pipe(sonar(sonarOptions))
    .on('error', util.log);
});

gulp.task('default', function (cb) {
  runSequence('build', cb);
});

gulp.task('build', function (cb) {
  runSequence('clean', 'lint', 'test', cb);
});

gulp.task('full', function (cb) {
  runSequence('clean', 'lint', 'test', 'apidocs', 'sonar', cb);
});


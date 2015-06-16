var path        = require('path'),
    gulp        = require('gulp'),
    mocha       = require('gulp-mocha'),
    util        = require('gulp-util'),
    jshint      = require('gulp-jshint'),
    map         = require('map-stream'),
    del         = require('del'),
    jsdoc       = require('gulp-jsdoc'),
    sonar       = require('gulp-sonar'),
    istanbul    = require('gulp-istanbul'),
    runSequence = require('run-sequence'),
    minimist    = require('minimist'),
    jscs        = require('gulp-jscs');

var MOCHA_REPORTER = 'spec';
var SRC = 'lib';
var TEST = 'test';
var BUILD = 'build'
var API_DOCS = 'build' + path.sep + 'api';

var knownOptions = {
  string: 'sonarUrl',
  default: { sonarUrl: 'http://localhost:9000' }
};

var options = minimist(process.argv.slice(2), knownOptions);
var pkg = require('./package.json');

var jshintReporter = map(function (file, cb) {
  if (!file.jshint.success) {
    console.error('JSHINT fail in ' + file.path);
    file.jshint.results.forEach(function (err) {
      if (err) {
        console.error(' ' + file.path + ': line ' + err.line + ', col ' + err.character + ', code ' + err.code + ', ' + err.reason);
      }
    });
  }
  cb(null, file);
});


gulp.task('clean', function (cb) {
  del([BUILD, '.sonar'], cb);
});

gulp.task('unit', function () {
  return gulp.src(TEST + path.sep + 'unit' + '/**/*.js')
    .pipe(mocha({
      reporter: MOCHA_REPORTER
    }))
    .on('error', util.log);
});

// gulp.task('integration', function () {
//   return gulp.src(TEST + path.sep + 'integration' + '/**/*.js')
//     .pipe(mocha({
//       reporter: MOCHA_REPORTER
//     }))
//     .on('error', util.log);
// });

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
    .pipe(jshint())
    .pipe(jshint.reporter('jshint-stylish'))
//    .pipe(jshint.reporter('fail'))
    .pipe(jshintReporter);
});

gulp.task('apidocs', function () {
  gulp.src(SRC + '/**/*.js')
    .pipe(jsdoc.parser())
    .pipe(jsdoc.generator(API_DOCS));
});

gulp.task('jscs', function () {
  return gulp.src(SRC + '/**/*.js')
    .pipe(jscs());
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

gulp.task('sonar', function (cb) {
  console.log(options);
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
  runSequence('clean', 'lint', 'jscs', 'test', cb);
});

gulp.task('full', function (cb) {
  runSequence('clean', 'lint', 'jscs', 'test', 'apidocs', 'sonar', cb);
});


var util = require('util'),
  fs = require('fs');

var levels = ["trace", "info", "notice", "debug", "warn", "error"];
var capitalizedLevels = {
  trace: "TRACE", 
  info: "INFO", 
  notice: "NOTICE", 
  debug: "DEBUG", 
  warn: "WARN", 
  error: "ERROR"
}

var verifyOpts = function (opts) {
  opts = opts || {};
  if (opts.ignore) {
    if (!Array.isArray) throw new Error('BAD_ARGS');
    var badLevel = opts.ignore.some( function (level) {
      return opts.ignore.indexOf(level) < 0;
    });
    if (badLevel) throw new Error('BAD_ARGS');
  }
}

var Logger = function (opts) {

  var self = this;

  verifyOpts(opts);
  opts = opts || {};

  var stream = opts.stream || process.stdout;

  var ignoreLevels = (function () {
    var retval = {};
    if (opts.ignore) {
      opts.ignore.forEach( function (level) {
        reval[level] = true;
      });
    }
    return retval;
  }());

  levels.forEach(function (level) {
    self[level] = function (msg) {
      if (ignoreLevels[level]) return;
      var data = {
        time: new Date(),
        level: level,
        msg: msg
      }
      stream.write(
        data.time.toString() + ': ' +
        capitalizedLevels[data.level] + '\t' +
        data.msg + '\n'
      );
    }
  });
}

module.exports = Logger;

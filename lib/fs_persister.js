var fs = require('fs'),
  path_lib = require('path');

var Persister = function (filepath) {

  this.save = function (state, cb) {

    var genError = function () {
      return new PersisterError(errorCodes.PERSISTER_FAULT);
    }

    fs.writeFile(filepath, JSON.stringify(state, null, '  '), 'utf8', function (err) {
    if (err) { cb(genError()); return; }
    fs.open(filepath, 'a', function (err, fd) {
    if (err) { cb(genError()); return; }
    fs.fsync(fd, function (err) {
    if (err) { cb(genError()); return; }
    fs.close(fd, function (err) {
    if (err) { cb(genError()); return; }
    cb();
    })})})});
  }

  this.load = function (cb) {
    fs.readFile(filepath, 'utf8', function (err, data) {
      var state;
      if (err) {
        if (err.code === 'ENOENT') {
          cb(new PersisterError(errorCodes.NO_SUCH_INC));
        }
        else {
          cb(new PersisterError(errorCodes.PERSISTER_FAULT));
        }
        return;
      }
      try {
        state = JSON.parse(data);
      }
      catch (e) {
        cb(new PersisterError(errorCodes.PERSISTER_FAULT));
        return;
      }
      cb(null, state);
    });
  }

  this.rm = function (cb) {
    fs.unlink(filepath, function (err) {
      if (err) {
        if (err.code === 'ENOENT') {
          cb(new PersisterError(errorCodes.NO_SUCH_INC));
        }
        else {
          cb(new PersisterError(errorCodes.RM_INC));
        }
      }
      else {
        cb();
      }
    });
  }
}

var errorCodes = {
  NO_SUCH_INC: 0,
  RM_INC: 1,
  PERSISTER_FAULT: 2
}

var PersisterError = function (errCode) {
  Error.apply(this);
  this.code = errCode;
}

Persister.errorCodes = errorCodes;
module.exports = Persister;

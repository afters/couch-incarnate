var fs = require('fs'),
  path_lib = require('path');

var Persister = function (filepath) {

  this.save = function (state, cb) {
    fs.writeFile(filepath, JSON.stringify(state, null, ' '), 'utf8', function (err) {
      cb( err ? new PersisterError(errorCodes.PERSISTER_FAULT) : null);
    });
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

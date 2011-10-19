var util = require('util'),
  Incarnator = require('./incarnator'),
  Logger = require('./logger');


var nameIncarnate = function (id, reName, gl) {
  return 'incarnate_' + id + '___' + name + '_' + gl;
}

// opts:
//  id
//  couchUrl
//  persister
//  [log]
var IncarnatorHandler = function (opts) {
  var incarnator;
  var incarnatorId = opts.id;
  var couchUrl = opts.couchUrl;
  var persister = opts.persister;
  var log = opts.log || new Logger();

  var status = 'uninitialized';
  var incarnator;
  var busy = false;
  var requests = [];
  var dbReqBatch = [];

  this.init = function (cb) {
    if (status === "initialized") {
      cb(new HandlerError(errorCodes.ALREADY_INITIALIZED));
      return;
    }
    newIncarnator = new Incarnator({
      id: incarnatorId,
      couchUrl: couchUrl,
      persister: persister,
      log: log
    });
    newIncarnator.loadPreviousState(function (err) {
      if (err && err.code === Incarnator.errorCodes.LOAD) {
        cb(new HandlerError(errorCodes.INCARNATOR_INIT));
        return;
      }
      if (!err) {
        incarnator = newIncarnator;
      }
      status = "initialized";
      cb();
    });
  }

  this.setup = function (incConf, reqId, cb) {
    if (status !== "initialized") {
      cb(new HandlerError(errorCodes.NOT));
      return;
    }
    requests.push({
      type: 'setup',
      incConf: incConf,
      id: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  this.getState = function (reqId, cb) {
    if (status !== "initialized") {
      cb(new HandlerError(errorCodes.NOT_INITIALIZED));
      return;
    }
    requests.push({
      type: 'state',
      id: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  this.destroy = function (reqId, cb) {
    if (status !== "initialized") {
      cb(new HandlerError(errorCodes.NOT_INITIALIZED));
      return;
    }
    requests.push({
      type: 'destroy',
      id: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  // opts:
  //  reName
  //  gl
  //  req
  this.dbRequest = function (opts, cb) {
    if (status !== "initialized") {
      cb(new HandlerError(errorCodes.NOT_INITIALIZED));
      return;
    }

    var dbReqData = {
      type: 'dbRequest',
      gl: opts.groupLevel,
      reName: opts.reName,
      req: opts.req,
      id: opts.reqId,
      cb: cb
    }

    // if appropriate, latch on to running sync
    if (incarnator && !requests.length) {
      try {
        incarnator.addToRunningSync(opts.reName, opts.gl);
        dbReqBatch.push(dbReqData);
      }
      catch (e) {
        log.trace(dbReqData.id + '\t' + 'failed add to running sync of ' + incarnatorId);
        requests.push(dbReqData);
      }
    }
    else {
      requests.push(dbReqData);
    }
    if (!busy) getBusy();
  }

  var getBusy = function () {
    log.trace('incarnatorHandler ' + incarnatorId + ' getting busy');
    busy = true;
    
    var done = function () {
      log.trace('incarnatorHandler ' + incarnatorId+ ' no longer busy');
      busy = false;
      if (requests.length) {
        getBusy();
      }
    }

    var next = requests.pop();

    (function (next) {
      if (next.type === 'dbRequest') {

        var clearAndDone = function (err) {
          dbReqBatch = [];
          done();
        }

        dbReqBatch = [];
        while (next && next.type === 'dbRequest') {
          dbReqBatch.push(next);
          next = requests.pop();
        }

        if (!incarnator) {
          log.trace('no such incarnator: ' + incarnatorId);
          sendErrTo(dbReqBatch, 
            new HandlerError(errorCodes.NO_SUCH_INCARNATOR),
            clearAndDone);
          return;
        }
        var incarnations = {};
        dbReqBatch.forEach( function (dbReq) {
          if (incarnator.incarnationExists(dbReq.reName, dbReq.gl)) {
            incarnations[dbReq.reName] = incarnations[dbReq.reName] || {};
            incarnations[dbReq.reName][dbReq.gl] = true;
          }
        });
        if (Object.keys(incarnations).length === 0) {
          log.trace('no incarnations to sync for incarnator: ' + incarnatorId);
          sendErrTo(dbReqBatch, new HandlerError(errorCodes.NO_SUCH_INCARNATION), clearAndDone);
          return;
        }
        incarnator.sync(incarnations, function (err) {
          if (err) {
            log.warn('sync error for incarnator: ' + incarnatorId);
            sendErrTo(dbReqBatch, new HandlerError(errorCodes.SYNC), clearAndDone);
          }
          else {
            forwardTo(dbReqBatch, clearAndDone);
          }
        });
      }
      else if (next.type === 'state') {
        if (!incarnator) {
          log.trace(next.id + '\t' + 'no such incarnator: ' + incarnatorId);
          next.cb(new HandlerError(errorCodes.NO_SUCH_INCARNATOR));
          done();
          return;
        }
        var state = incarnator.getState();
        if (!state) {
          next.cb(new HandlerError());
          done();
          return;
        }
        next.cb(null, state);
        done();
      }
      else if (next.type === 'destroy') {
        log.trace(next.id + '\t' + 'launching destroy for incarnator: ' + incarnatorId);
        if (!incarnator) {
          log.trace(next.id + '\t' + 'no such incarnator: ' + incarnatorId);
          next.cb(new HandlerError(errorCodes.NO_SUCH_INCARNATOR));
          done();
          return;
        }
        incarnator.destroy( function (err) {
          if (err) {
            next.cb(new HandlerError(errorCodes.SERVER_ERROR));
            done();
            return;
          }
          incarnator = null;
          next.cb();
          done();
        });
      }
      else if (next.type === 'setup') {
        var createNewIncarnator = function (cb) {
          log.info('creating new incarnator: ' + incarnatorId);
          var newIncarnator = new Incarnator({
            id: incarnatorId, 
            couchUrl: couchUrl, 
            persister: persister,
            log: log
          });
          newIncarnator.initFromConf(next.incConf, function (err) {
            if (err) {
              log.trace(next.id + '\t' + 'failed to init incarnator: ' + incarnatorId);
              next.cb(new HandlerError(errorCodes.RESET));
              done();
              return;
            }
            incarnator = newIncarnator;
            next.cb();
            done();
          });
        }

        if (incarnator) {
          incarnator.destroy(function (err) {
            if (err) {
              log.trace(next.id + '\t' + 'no such incarnator: ' + incarnatorId);
              next.cb(new HandlerError(errorCodes.NO_SUCH_INCARNATOR));
              done();
              return;
            }
            createNewIncarnator();
          });
        }
        else {
          createNewIncarnator();
        }
      }
    }(next));
  }

  var forwardTo = function (dbReqs, cb) {
    var reqsNum = 0;
    dbReqs.forEach( function (dbReqData) {
      reqsNum++;
      incarnator.forward(dbReqData.req, dbReqData.reName, dbReqData.gl, function (err, ret) {
        reqsNum--;
        if (err) {
          dbReqData.cb(err);
        }
        else {
          dbReqData.cb(null, ret);
        }
        if (reqsNum === 0) {
          cb();
        }
      });
    });
  }

  var sendErrTo = function (dbReqs, error, cb) {
    var reqsLeft = dbReqs.length;
    dbReqs.forEach(function (dbReqData) {
      reqsLeft--;
      dbReqData.cb(error);
      if (reqsLeft === 0) {
        cb();
      }
    });
  }

}

var errorCodes = {
  NO_SUCH_INCARNATOR: 0,
  ALREADY_INITIALIZED: 1,
  NOT_INITIALIZED: 2,
  RESET: 3,
  SERVER_ERROR: 4,
  SYNC: 5
}

var HandlerError = function (errCode) {
  Error.apply(this);
  this.code = errCode;
}
util.inherits(HandlerError, Error);

IncarnatorHandler.errorCodes = errorCodes;
module.exports = IncarnatorHandler;


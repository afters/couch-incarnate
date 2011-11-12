var util = require('util'),
  Incarnator = require('./incarnator'),
  Queue = require('./queue'),
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
  var handlerReqsCounter = 0;
  var busy = false;
  var requests = new Queue();
  var dbReqBatch = new Queue(); 

  var createUsageTrackingCallback = function (cb) {
    handlerReqsCounter++;
    return function () {
      handlerReqsCounter--;
      cb.apply(this, arguments);
    };
  }

  this.incarnatorExists = function () {
    return incarnator ? true : false;
  }

  this.isInUse = function () {
    return handlerReqsCounter !== 0;
  }

  var init = function (cb) {
    var newIncarnator = new Incarnator({
      id: incarnatorId,
      couchUrl: couchUrl,
      persister: persister,
      log: log
    });
    newIncarnator.loadPreviousState(function (err) {
      if (err && err.code !== Incarnator.errorCodes.NO_SUCH_INCARNATOR) {
        if (err.code === Incarnator.errorCodes.LOAD) {
          cb(new HandlerError(errorCodes.INCARNATOR_INIT));
        }
        if (err.code === Incarnator.errorCodes.BAD_STATE) {
          cb(new HandlerError(errorCodes.BAD_STATE));
        }
        else {
          cb(new HandlerError(errorCodes.SERVER_ERROR));
        }
        return;
      }
      if (!err) {
        incarnator = newIncarnator;
      }
      status = "initialized";
      cb();
    });
  }

  this.setupIncarnator = function (incConf, reqId, cb) {
    cb = createUsageTrackingCallback(cb);
    requests.enqueue({
      type: 'setup',
      incConf: incConf,
      id: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  this.setIncarnatorState = function (incState, reqId, cb) {
    cb = createUsageTrackingCallback(cb);
    var newIncarnator = new Incarnator({
      id: incarnatorId,
      couchUrl: couchUrl,
      persister: persister,
      log: log
    });
    newIncarnator.initFromState(incState, function (err) {
      if (err) {
        cb(new HandlerError(errorCodes.BAD_STATE));
        return;
      }
      incarnator = newIncarnator;
      cb();
    });
  }

  this.getIncarnatorState = function (reqId, cb) {
    cb = createUsageTrackingCallback(cb);
    requests.enqueue({
      type: 'state',
      id: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  this.destroyIncarnator = function (keepDbs, reqId, cb) {
    cb = createUsageTrackingCallback(cb);
    requests.enqueue({
      type: 'destroy',
      keepDbs: keepDbs,
      id: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  // opts:
  //  reName
  //  gl
  //  req
  this.incarnationRequest = function (opts, cb) {
    cb = createUsageTrackingCallback(cb);

    var dbReqData = {
      type: 'dbRequest',
      gl: opts.groupLevel,
      reName: opts.reName,
      req: opts.req,
      id: opts.reqId,
      cb: cb
    }

    // if appropriate, latch on to running sync
    if (incarnator && !requests.getLength()) {
      try {
        incarnator.addToRunningSync(opts.reName, opts.groupLevel);
        dbReqBatch.enqueue(dbReqData);
      }
      catch (e) {
        log.trace(dbReqData.id + '\t' + 'failed add to running sync of ' + incarnatorId);
        requests.enqueue(dbReqData);
      }
    }
    else {
      requests.enqueue(dbReqData);
    }
    if (!busy) getBusy();
  }

  var getBusy = function () {
    busy = true;
    
    var done = function () {
      log.trace('incarnatorHandler ' + incarnatorId+ ' no longer busy. Done handling: ' + next.type);
      busy = false;
      if (requests.getLength()) {
        getBusy();
      }
    }

    var next = requests.dequeue();

    log.trace('incarnatorHandler ' + incarnatorId + ' getting busy with: ' + next.type);

    (function (next) {
      ensureHandlerInitialized( function () {
        if (next.type === 'dbRequest') {

          dbReqBatch.enqueue(next);
          while (requests.peek() && requests.peek().type === 'dbRequest') {
            dbReqBatch.enqueue(requests.dequeue());
          }

          if (!incarnator) {
            log.trace('no such incarnator: ' + incarnatorId);
            sendError(new HandlerError(errorCodes.NO_SUCH_INCARNATOR));
            done();
            return;
          }
          var incarnations = {};
          var dbReq;
          for (var i = 0; i < dbReqBatch.getLength(); i++) {
            dbReq = dbReqBatch.peek(i);
            if (incarnator.incarnationExists(dbReq.reName, dbReq.gl)) {
              incarnations[dbReq.reName] = incarnations[dbReq.reName] || {};
              incarnations[dbReq.reName][dbReq.gl] = true;
            }
          }
          if (Object.keys(incarnations).length === 0) {
            log.trace('no incarnations to sync for incarnator: ' + incarnatorId);
            sendError(new HandlerError(errorCodes.NO_SUCH_INCARNATION));
            done();
            return;
          }
          incarnator.sync(incarnations, function (err) {
            if (err) {
              log.warn('sync error for incarnator: ' + incarnatorId);
              sendError(new HandlerError(errorCodes.SYNC));
              done();
            }
            else {
              forwardRequests(done);
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
            next.cb(new HandlerError(errorCodes.SERVER_ERROR));
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
          incarnator.destroy(next.keepDbs, function (err) {
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
                if (err.code === Incarnator.errorCodes.BAD_CONF) {
                  log.trace(next.id + '\t' + 'bad configuration supplied: ' + incarnatorId);
                  next.cb(new HandlerError(errorCodes.BAD_CONF));
                }
                else {
                  log.trace(next.id + '\t' + 'failed to init incarnator: ' + incarnatorId);
                  next.cb(new HandlerError(errorCodes.RESET));
                }
                done();
                return;
              }
              incarnator = newIncarnator;
              next.cb();
              done();
            });
          }

          if (incarnator) {
            incarnator.destroy(false, function (err) {
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
      });
    }(next));
  }

  var ensureHandlerInitialized = function (cb) {
    if (status === "initialized") {
      process.nextTick(cb);
      return;
    }
    init( function (err) {
      if (err) {
        log.info('failed to initialize handler for incarnator ' + incarnatorId);
        cb(new Error());
        return;
      }
      log.info('successfully initialized handler for incarnator ' + incarnatorId);
      status = "initialized";
      cb();
    });
  }

  var forwardRequests = function (cb) {
    var notYetForwarded = dbReqBatch.getLength();
    while (dbReqBatch.getLength()) {
      (function () {
        var dbReqData = dbReqBatch.dequeue();
        incarnator.forward(dbReqData.req, dbReqData.reName, dbReqData.gl, function (err, ret) {
          if (err) {
            dbReqData.cb(err);
          }
          else {
            dbReqData.cb(null, ret);
          }
          notYetForwarded--;
          if (notYetForwarded === 0) {
            cb();
          }
        });
      }());
    }
  }

  var sendError = function (error) {
    while (dbReqBatch.peek()) {
      dbReqBatch.dequeue().cb(error);
    }
  }

}

var errorCodes = {
  NO_SUCH_INCARNATOR: 0,
  ALREADY_INITIALIZED: 1,
  NOT_INITIALIZED: 2,
  RESET: 3,
  SERVER_ERROR: 4,
  SYNC: 5,
  NO_SUCH_INCARNATION: 6,
  BAD_CONF: 7,
  BAD_STATE: 8
}

var HandlerError = function (errCode) {
  Error.apply(this);
  this.code = errCode;
}
util.inherits(HandlerError, Error);

IncarnatorHandler.errorCodes = errorCodes;
IncarnatorHandler.HandlerError = HandlerError;
module.exports = IncarnatorHandler;


#!/usr/bin/env node

var util = require('util'),
  path = require('path'),
  fs = require('fs'),
  url_lib = require('url'),
  http = require('http'),
  Persister = require('../lib/couch_persister'),
  Queue = require('../lib/queue'),
  Logger = require('../lib/logger'),
  IncarnatorHandler = require('../lib/incarnatorHandler');


var confFilePath = path.dirname(__dirname) + '/conf.json';
var conf = JSON.parse(fs.readFileSync(confFilePath));
var port = conf.port || 4895;
var log = new Logger({
  stream: conf.log && conf.log.path ? 
    fs.createWriteStream(conf.log.path, {flags: 'a+', mode: 0666}) : null,
  ignore: conf.log ? conf.log.ignore : null
});

var createIncarnatorHandlers = function (cb) {
  var incarnatorHandles = new IncarnatorHandlers();
  incarnatorHandles.init(function (err) {
    if (err) {
      cb(new Error());
      return;
    }
    cb(null, incarnatorHandles);
  });
}

var IncarnatorHandlers = function () {

  var requests = new Queue();
  var handlers = {};
  var busy = false;
  var activeAsyncOp;
  var persister = new Persister(conf.couch, 'incarnate', 'state');

  this.init = function (cb) {
    persister.load( function (err, loadedState) {
      if (err && err.code !== Persister.errorCodes.NO_SUCH_INC) {
        cb(new HandlersError(IncarnatorHandlers.errorCodes.LOAD));
        return;
      }
      if (loadedState) {
        setState(loadedState);
      }
      if (activeAsyncOp && activeAsyncOp.type === 'move') {
        log.info('detected interrupted move operation: ' + JSON.stringify(activeAsyncOp, null, '  '));
        var req = {
          sourceId: activeAsyncOp.sourceId,
          targetId: activeAsyncOp.targetId,
          reqId: Math.random(),
          cb: function () {}
        }
        moveIncarnator(req, cb);
      }
      else {
        cb();
      }
    });
  }

  var setState = function (newState) {
    activeAsyncOp = newState.active_async_op;
  }

  var getState = function () {
    var state = {};
    if (activeAsyncOp) {
      state.activeAsyncOp = activeAsyncOp;
    }
    return state;
  }

  this.setupIncarnator = function (incarnatorId, incarnatorConf, reqId, cb) {
    requests.enqueue({
      type: 'setupIncarnator',
      incarnatorId: incarnatorId,
      incarnatorConf: incarnatorConf,
      reqId: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }
  var setupIncarnator = function (incarnatorId, incarnatorConf, reqId, cb) {
    handlerCall(incarnatorId, "setupIncarnator", [incarnatorConf, reqId, function (err) {
      if (err) {
        if (err instanceof IncarnatorHandler.HandlerError && 
            err.code === IncarnatorHandler.errorCodes.BAD_CONF) {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.BAD_CONF));
        }
        else {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.SERVER_ERROR));
        }
      }
      else {
        cb();
      }
    }]);
  }

  var setIncarnatorState = function (incarnatorId, incarnatorState, reqId, cb) {
    handlerCall(incarnatorId, "setIncarnatorState", [incarnatorState, reqId, function (err) {
      if (err) {
        if (err.code === IncarnatorHandler.errorCodes.BAD_STATE) {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.BAD_STATE));
        }
        else {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.SERVER_ERROR));
        }
      }
      else {
        cb();
      }
    }]);
  }

  this.destroyIncarnator = function (incarnatorId, reqId, cb) {
    requests.enqueue({
      type: 'destroyIncarnator',
      incarnatorId: incarnatorId,
      reqId: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }
  var destroyIncarnator = function (incarnatorId, keepDbs, reqId, cb) {
    handlerCall(incarnatorId, "destroyIncarnator", [keepDbs, reqId, function (err) {
      if (err) {
        if (err.code === IncarnatorHandler.errorCodes.NO_SUCH_INCARNATOR) {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR));
        }
        else {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.SERVER_ERROR));
        }
      }
      else {
        cb();
      }
    }]);
  }

  this.getIncarnatorState = function (incarnatorId, reqId, cb) {
    requests.enqueue({
      type: 'getIncarnatorState',
      incarnatorId: incarnatorId,
      reqId: reqId,
      cb: cb
    });
    if (!busy) getBusy();
  }
  var getIncarnatorState = function (incarnatorId, reqId, cb) {
    handlerCall(incarnatorId, "getIncarnatorState", [reqId, function (err, state) {
      if (err) {
        if (err.code === IncarnatorHandler.errorCodes.NO_SUCH_INCARNATOR) {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR));
        }
        else {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.SERVER_ERROR));
        }
      }
      else {
        cb(null, state);
      }
    }]);
  }

  this.incarnationRequest = function (incarnatorId, opts, cb) {
    requests.enqueue({
      type: 'incarnationRequest',
      incarnatorId: incarnatorId,
      opts: opts,
      cb: cb
    });
    if (!busy) getBusy();
  }

  var incarnationRequest = function (incarnatorId, opts, cb) {
    handlerCall(incarnatorId, "incarnationRequest", [opts, function (err, ret) {
      if (err) {
        if (err.code === IncarnatorHandler.errorCodes.NO_SUCH_INCARNATOR) {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR));
        }
        else if (err.code === IncarnatorHandler.errorCodes.NO_SUCH_INCARNATION) {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATION));
        }
        else {
          cb(new HandlersError(IncarnatorHandlers.errorCodes.SERVER_ERROR));
        }
      }
      else {
        cb(null, ret);
      }
    }]);
  }

  this.moveIncarnator = function (sourceId, targetId, reqId, cb) {
    requests.enqueue({
      type: 'moveIncarnator',
      sourceId: sourceId,
      targetId: targetId,
      cb: cb
    });
    if (!busy) getBusy();
  }

  var moveIncarnator = function (req, cb) {

    var setAndSaveState = function (req, stage, sourceState, cb) {
      activeAsyncOp = {
        type: 'move',
        targetId: req.targetId,
        sourceId: req.sourceId,
        sourceState: sourceState,
        stage: stage
      }
      persister.save(getState(), cb);
    }

    var startMove = function () {
      getIncarnatorState(sourceId, reqId, function (err, sourceState) {
      if (err) { req.cb(err); cb(); return; }
      destroyIncarnator(targetId, false, reqId, function (err) {
      if (err && err.code !== IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR) {
        req.cb(err);
        cb();
        return;
      }
      setAndSaveState(req, 'pre_init_target', sourceState, function (err) {
      if (err) {
        log.error('save state failed');
        req.cb(err);
        cb(new Error());
        return;
      }
      continueFromPreInitTarget();
      })})});
    }

    var continueFromPreInitTarget = function () {
      setIncarnatorState(targetId, activeAsyncOp.sourceState, reqId, function (err) {
      if (err) {
        log.error('setIncarnatorState failed');
        req.cb(err);
        cb(new Error());
        return;
      }
      setAndSaveState(req, 'pre_delete_source', activeAsyncOp.sourceState, function (err) {
      if (err) {
        log.error('save state failed');
        req.cb(err);
        cb(new Error());
        return;
      }
      continueFromPreDeleteSource();
      })});
    }

    var continueFromPreDeleteSource = function () {
      destroyIncarnator(sourceId, true, reqId, function (err) {
        if (err && err.code !== IncarnatorHandler.errorCodes.NO_SUCH_INCARNATOR) {
          log.error('delete incarnator failed');
          req.cb(err);
          cb(new Error());
          return;
        }
        req.cb();
        cb();
      });
      activeAsyncOp = null;
      persister.save(getState(), cb);
    }

    var sourceId = req.sourceId,
      targetId = req.targetId,
      reqId = req.reqId;

    if (!activeAsyncOp) {
      startMove();
    }
    else if (activeAsyncOp.type === 'move' && activeAsyncOp.stage === 'pre_init_target') {
      continueFromPreInitTarget()
    }
    else if (activeAsyncOp.type === 'move' && activeAsyncOp.stage === 'pre_delete_source') {
      continueFromPreDeleteSource();
    }
    else {
      log.warn('illegal state');
      req.cb(new Error());
      cb(new HandlersError(IncarnatorHandlers.errorCodes.ILLEGAL_STATE));
    }

  }

  var getBusy = function () {
    busy = true;
    var done = function () {
      busy = false;
      if (requests.getLength()) {
        process.nextTick(getBusy);
      }
    }
    var req = requests.dequeue();
    if (!req) return;
    if (req.type === 'incarnationRequest') {
      incarnationRequest(req.incarnatorId, req.opts, req.cb);
      done();
    }
    else if (req.type === 'getIncarnatorState') {
      getIncarnatorState(req.incarnatorId, req.reqId, req.cb);
      done();
    }
    else if (req.type === 'setupIncarnator') {
      setupIncarnator(req.incarnatorId, req.incarnatorConf, req.reqId, req.cb);
      done();
    }
    else if (req.type === 'destroyIncarnator') {
      destroyIncarnator(req.incarnatorId, false, req.reqId, req.cb);
      done();
    }
    else if (req.type === 'moveIncarnator') {
      moveIncarnator(req, function (err) {
        if (err) throw 'illegal state';
        done();
      });
    }

  }


  var handlerCall = function (incarnatorId, fnName, args) {
    var handler = handlers[incarnatorId] = handlers[incarnatorId] || 
      new IncarnatorHandler({
        id: incarnatorId, 
        couchUrl: conf.couch, 
        persister: new Persister(conf.couch, 'incarnate', incarnatorId),
        log: log
      });
    var origCb = args[args.length - 1];
    var newCb = function () {
      if (!handler.incarnatorExists() && !handler.isInUse()) {
        delete handlers[incarnatorId];
      }
      origCb.apply(this, arguments);
    }
    var argsWithModifiedCb = [].concat(args);
    argsWithModifiedCb[argsWithModifiedCb.length - 1] = newCb;
    handler[fnName].apply(handler, argsWithModifiedCb);
  }

}

IncarnatorHandlers.errorCodes = {
  NO_SUCH_INCARNATION: 0,
  NO_SUCH_INCARNATOR: 1,
  SERVER_ERROR: 2,
  INCARNATOR_DELETED: 3,
  BAD_CONF: 4,
  BAD_STATE: 5,
  BAD_INCARNATOR: 6
}

var HandlersError = function (errCode) {
  Error.apply(this);
  this.code = errCode;
}
util.inherits(HandlersError, Error);

var incarnatorHandlers = new IncarnatorHandlers();

var msgs = {
  INC_READY: {
    statusCode: 200,
    body: {msg: 'Ready and waiting'}
  },
  NO_SUCH_DB: {
    statusCode: 404,
    body: {err: "no such DB"}
  },
  NO_SUCH_INCARNATION: {
    statusCode: 404,
    body: {err: "no such incarnation"}
  },
  NO_SUCH_INCARNATOR: {
    statusCode: 404,
    body: {err: "no such incarnator"}
  },
  METHOD_NOT_SUPPORTED: {
    statusCode: 405,
    body: {err: "method not supported"}
  },
  BAD_JSON_DOC: {
    statusCode: 400,
    body: {err: "bad json document"}
  },
  INC_SETUP_SUCCESSFUL: {
    statusCode: 201,
    body: {msg: "inc setup successful"}
  },
  INC_DELETED: {
    statusCode: 200,
    body: {msg: "inc delete successful"}
  },
  ERR_SAVE_INC: {
    statusCode: 500,
    body: {err: "save incarnation failed"}
  },
  ERR_LOAD_INC: {
    statusCode: 500,
    body: {err: "load incarnation failed"}
  },
  ERR_RM_INC: {
    statusCode: 500,
    body: {err: "del incarnation failed"}
  },
  DB_REQ_FAILED: {
    statusCode: 500,
    body: {err: "DB request failed"}
  },
  SERVER_ERROR: {
    statusCode: 500,
    body: {err: "server error"}
  },
  BAD_CONF: {
    statusCode: 400,
    body: {err: "bad configuration"}
  },
  BAD_INCARNATOR: {
    statusCode: 500,
    body: {err: "bad incarnator"}
  },
  MOVE_SUCCESSFUL: {
    statusCode: 200,
    body: {msg: "move successful"}
  }
}

createIncarnatorHandlers( function (err, incarnatorHandlers) {
  if (err) {
    log.error('failed to create incarnatorHandlers');
    throw new Error();
  }
  var server = http.createServer( function (req, res) {
    reqId = Math.random();
    log.info(reqId + ':\t' + req.method + '\t' + req.url);
  
    var urlParts = url_lib.parse(req.url).pathname.split('/')
      .filter(function (e) { return (e !== ''); });
    var incarnatorId = urlParts[0];
    var incarnatorStatus;
    var reduceId = urlParts[1];
  
    var groupLevel = urlParts[2];
    var destination = req.headers.destination;
    var sendRes = function (statusCode, jsonBody) {
      var bodyString = JSON.stringify(jsonBody, null, '  ');
      log.trace(reqId + ':\t' + 'sending ' + statusCode + ': ' + bodyString);
      res.writeHead(statusCode, 'application/json');
      if (jsonBody) {
        res.write( bodyString + '\n', 'utf8');
      }
      res.end();
    }
  
    var sendMsg = function (msg) {
      sendRes(msg.statusCode, msg.body);
    }
  
    var send = function (statusCode, body) {
      sendRes(statusCode, body);
    }
    
    var fetchBody = function (req, callback) {
      var body = '';
      req.addListener('data', function (chunk) {
        body += chunk;
      });
      req.addListener('end', function () {
        callback(body === '' ? null : JSON.parse(body));
      });
    }
  
  
    // '/'
  
    if (urlParts.length === 0) {
      if (req.method === 'GET') {
        sendMsg(msgs.INC_READY);
      }
      else {
        sendMsg(msgs.METHOD_NOT_SUPPORTED);  
      }
      return;
    }
    
  
    // '/incarnator_name'
  
    else if (urlParts.length === 1) {
      // GET '/inc_name'
      if (req.method === 'GET') {
        log.trace(reqId + '\t' + 'get state of incarnator ' + incarnatorId);
        incarnatorHandlers.getIncarnatorState(incarnatorId, reqId, function (err, state) {
          if (err) {
            if (err.code === IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR) {
              sendMsg(msgs.NO_SUCH_INCARNATOR);
            }
            else if (err.code === IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATION) {
              sendMsg(msgs.NO_SUCH_INCARNATION);
            }
            else {
              sendMsg(msgs.SERVER_ERROR);
            }
          }
          else {
            send(200, state);
          }
        });
      }
      // PUT '/incarnator_name'
      else if (req.method === 'PUT') {
        fetchBody(req, function (incarnatorConf) {
          if (!incarnatorConf) {
            sendMsg(msgs.BAD_JSON_DOC);
          }
          else {
            log.trace(reqId + '\t' + 'call setup incarnator ' + incarnatorId);
            incarnatorHandlers.setupIncarnator(incarnatorId, incarnatorConf, reqId, function (err) {
              if (err) {
                if (err.code === IncarnatorHandlers.errorCodes.BAD_CONF) {
                  sendMsg(msgs.BAD_CONF);
                }
                else {
                  sendMsg(msgs.SERVER_ERROR);
                }
              }
              else {
                sendMsg(msgs.INC_SETUP_SUCCESSFUL);
              }
            });
          }
        });
      }
      else if (req.method === 'DELETE') {
        log.trace(reqId + '\t' + 'destroy incarnator ' + incarnatorId);
        incarnatorHandlers.destroyIncarnator(incarnatorId, reqId, function (err) {
          if (err) {
            if (err.code === IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR) {
              sendMsg(msgs.NO_SUCH_INCARNATOR);
            }
            else {
              sendMsg(msgs.SERVER_ERROR);
            }
          }
          else {
            sendMsg(msgs.INC_DELETED);
          }
        });
      }
      else if (req.method === 'MOVE') {
        log.trace(reqId + '\t' + 'move incarnator ' + incarnatorId);
        incarnatorHandlers.moveIncarnator(incarnatorId, destination, reqId, function (err) {
          if (!err) {
            sendMsg(msgs.MOVE_SUCCESSFUL);
            return;
          }
          if (err.code === IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR) {
            sendMsg(msgs.NO_SUCH_INCARNATOR);
          }
          else {
            sendMsg(msgs.SERVER_ERROR);
          }
        });
      }
      else {
        sendMsg(msgs.METHOD_NOT_SUPPORTED);
      }
    }
  
  
    // '/inc_name/reduce_name'
  
    else if (urlParts.length === 2) {
      sendMsg(msgs.METHOD_NOT_SUPPORTED);
    }
  
  
    // '/inc_name/reduce_name/group_level/...'
  
    else {
      log.trace(reqId + '\t' + 'send db-req to incarnator ' + incarnatorId + 
        ' gl: ' + groupLevel + ' reName: ' + reduceId);
      incarnatorHandlers.incarnationRequest(incarnatorId, {reName: reduceId, groupLevel: groupLevel, req: req, reqId: reqId}, function (err, ret) {
        if (err) {
          if (err.code === IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATOR) {
            sendMsg(msgs.NO_SUCH_INCARNATOR);
          }
          else if (err.code === IncarnatorHandlers.errorCodes.NO_SUCH_INCARNATION) {
            sendMsg(msgs.NO_SUCH_INCARNATION);
          }
          if (err.code === IncarnatorHandlers.errorCodes.BAD_INCARNATOR) {
            sendMsg(msgs.BAD_INCARNATOR);
          }
          else {
            sendMsg(msgs.SERVER_ERROR);
          }
        }
        else {
          res.writeHead(ret.res.statusCode, ret.res.headers);
          if (ret.body) {
            res.write(ret.body, 'utf8');
          }
          res.end();
        }
      });
    }
  });
  log.info('listening to port ' + conf.port);
  server.listen(conf.port, "127.0.0.1");
});


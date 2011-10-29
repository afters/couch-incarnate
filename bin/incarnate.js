#!/usr/bin/env node

var util = require('util'),
  fs = require('fs'),
  url_lib = require('url'),
  http = require('http'),
  Persister = require('../lib/couch_persister'),
  Logger = require('../lib/logger'),
  IncarnatorHandler = require('../lib/incarnatorHandler');


var confFilePath = 'conf';
var conf = JSON.parse(fs.readFileSync(confFilePath));
var port = conf.port || 4895;
var log = new Logger({
  stream: conf.log && conf.log.path ? 
    fs.createWriteStream(conf.log.path, {flags: 'a+', mode: 0666}) : null,
  ignore: conf.log ? conf.log.ignore : null
});

var IncarnatorHandlers = function () {
  var handlers = [];

  var ensureHandlerExists = function (incarnatorId, cb) {
    var newHandler;
    if (!handlers[incarnatorId]) {
      newHandler = new IncarnatorHandler({
        id: incarnatorId, 
        couchUrl: conf.couch, 
        persister: new Persister(conf.couch, 'incarnate', incarnatorId),
        log: log
      });
      newHandler.init( function (err) {
        if (err) {
          log.info('failed to initialize handler for incarnator ' + incarnatorId);
          cb(new Error());
          return;
        }
        log.info('successfully initialized handler for incarnator ' + incarnatorId);
        handlers[incarnatorId] = newHandler;
        cb();
      });
      return;
    }
    cb();
  }

  var handlerCall = function (incarnatorId, fnName, args) {
    ensureHandlerExists(incarnatorId, function (err) {
      var handler = handlers[incarnatorId];
      var origCb = args[args.length - 1];
      if (err) {
        origCb(new HandlersError(IncarnatorHandlers.errorCodes.SERVER_ERROR));
        return;
      }
      var newCb = function () {
        if (!handler.incarnatorExists() && !handler.isInUse()) {
          delete handlers[incarnatorId];
        }
        origCb.apply(this, arguments);
      }
      var argsWithModifiedCb = [].concat(args);
      argsWithModifiedCb[argsWithModifiedCb.length - 1] = newCb;
      handler[fnName].apply(handler, argsWithModifiedCb);
    });
  }

  this.setupIncarnator = function (incarnatorId, incarnatorConf, reqId, cb) {
    handlerCall(incarnatorId, "setupIncarnator", [incarnatorConf, reqId, function (err) {
      if (err) {
        if (err.code === IncarnatorHandler.errorCodes.BAD_CONF) {
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

  this.destroyIncarnator = function (incarnatorId, reqId, cb) {
    handlerCall(incarnatorId, "destroyIncarnator", [reqId, function (err) {
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
}

IncarnatorHandlers.errorCodes = {
  NO_SUCH_INCARNATION: 0,
  NO_SUCH_INCARNATOR: 1,
  SERVER_ERROR: 2,
  INCARNATOR_DELETED: 3,
  BAD_CONF: 4
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
  }
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


var util = require('util'),
  url_lib = require('url'),
  request = require('request'),
  SourceDb = require('./dbs').SourceDb,
  MapDb = require('./dbs').MapDb,
  IncarnationDb = require('./dbs').IncarnationDb,
  Persister = require('./fs_persister'),
  Logger = require('./logger'),
  Syncer = require('./syncer');


var randomDbNameString = function () {
  return Math.random().toString().substr(2);
}

var nameIncarnate = function (id, reName, gl) {
  return 'incarnate_' + id + '___' + reName + '_' + gl;
}

var Incarnator = function (opts) {

  var self = this;

  var id = opts.id;
  var couchUrl = opts.couchUrl;
  var persister = opts.persister;
  var log = opts.log || new Logger();

  var internalId;
  var sourceDb;
  var mapDb;
  var incarnations;
  var syncer;
  var conf;

  var status = 'uninitialized';
  var busy = false;

  // assume DB's conform to loaded state,
  this.loadPreviousState = function (cb) {
    persister.load(function (err, loadedState) {
      if (err) { 
        if (err.code === Persister.errorCodes.NO_SUCH_INC) {
          cb(new IncarnatorError(errorCodes.NO_SUCH_INCARNATOR)); 
        }
        else {
          cb(new IncarnatorError(errorCodes.LOAD)); 
        }
        return;
      }
      try {
        setState(loadedState);
      }
      catch (e) {
        cb(errorCodes.BAD_STATE);
        return;
      }
      cb();
    });
  }

  this.initFromState = function (state, cb) {
    try {
      setState(state);
    }
    catch (e) {
      cb(errorCodes.BAD_STATE);
      return;
    }
    saveState( function (err) {
      if (err) {
        log.warn('failed to save state of incarnator: ' + id);
        cb(new IncarnatorError());
        return;
      }
      cb();
    });
  }

  this.initFromConf = function (newConf, cb) {
    try {
      setState({conf: newConf, internalId: randomDbNameString(), status: "initializing"});
    }
    catch (e) {
      cb(new IncarnatorError(errorCodes.BAD_CONF));
      return;
    }
    saveState( function (err) {
    if (err) {
      log.warn('failed to save state of incarnator: ' + id);
      cb(new IncarnatorError());
      return;
    }
    initDbs( function (err) {
    if (err) {
      log.warn('failed to initialize incarnator DB\'s: ' + id);
      cb(new IncarnatorError());
      return;
    }
    status = "initialized";
    saveState( function (err) {
    if (err) {
      log.warn('failed to save state of incarnator: ' + id);
      cb(new IncarnatorError());
      return;
    }
    cb();
    });});});
  }


  this.destroy = function (keepDbs, cb) {
    if (status === "uninitialized") { 
      cb(new IncarnatorError(errorCodes.UNINITIALIZED)); 
      return;
    }
    log.info('deleting incarnator: ' + id);
    if (keepDbs) {
      persister.rm(function (err) {
        if (err) { 
          log.warn('failed to delete state file for incarnator: ' + id);
          cb(new IncarnatorError()); 
        }
        else {
          cb();
        }
      });
      return;
    }

    log.trace('setting incarnator state to \'deleting\': ' + id);
    status = 'deleting';

    saveState(function (err) {
    if (err) { 
      log.warn('failed to save state of incarnator: ' + id);
      cb(new IncarnatorError()); 
      return; 
    }
    log.trace('deleting incarnator DB\'s: ' + id);

    delDbs( function (err) {
    if (err) { 
      log.warn('failed to delete incarnator DB\'s: ' + id);
      cb(new IncarnatorError()); 
      return; 
    }

    log.trace('deleting state file for incarnator: ' + id);

    persister.rm(function (err) {
    if (err) { 
      log.warn('failed to delete state file for incarnator: ' + id);
      cb(new IncarnatorError()); 
      return;
    }
    cb();
    });});});
  }

  this.getState = function () {
    if (status === "uninitialized") return null;
    return {
      conf: conf,
      status: status,
      internalId: internalId,
      sourceToMapSeq: syncer.getSourceToMapSeq(),
      mapToIncSeqs: (function () {
        var retval = {};
        var reName, gl, reduce, inc;
        for (var incId in incarnations) {
          inc = incarnations[incId];
          retval[inc.reName] = retval[inc.reName] || {}
          retval[inc.reName][inc.gl] = syncer.getMapToIncSeq(incId);
        }
        return retval;
      }())
    }
  }

  this.incarnationExists = function (reName, gl) {
    var incName = nameIncarnate(internalId, reName, gl);
    return incarnations[incName] ? true : false;
  }

  // incs - by reduce name and incarnation
  this.sync = function (incs, cb) {
    if (status !== "initialized") { 
      cb(new IncarnatorError(errorCodes.UNINITIALIZED)); 
      return;
    }
    var reName, gl;
    var incIds = [];
    for (reName in incs) {
      for (gl in incs[reName]) {
        incIds.push(nameIncarnate(internalId, reName, gl));
      }
    }

    log.trace('incarnator ' + id + ': syncing incarnations: ' + 
      JSON.stringify(incIds));

    syncer.sync(incIds, function (syncerErr) {
      saveState( function (saveErr) {
        if (syncerErr || saveErr) { 
          if (syncerErr) { 
            log.warn('incarnator ' + id + ': error syncing: ' + syncerErr); 
          }
          if (saveErr) { 
            log.warn('incarnator ' + id + ': error saving state: ' + saveErr); 
          }
          cb(new IncarnatorError());
          return; 
        }
        cb();
      });
    });
  }

  this.addToRunningSync = function (reName, gl) {
    if (status !== "initialized") { 
      cb(new IncarnatorError(errorCodes.UNINITIALIZED)); 
      return;
    }
    try {
      syncer.addToRunningSync(nameIncarnate(internalId, reName, gl));
    }
    catch (e) {
      throw new IncarnatorError();
    }
  }

  this.forward = function (req, reName, gl, cb) {
    if (status !== "initialized") { 
      cb(new IncarnatorError(errorCodes.UNINITIALIZED)); 
      return;
    }

    var pathInDb = url_lib.parse(req.url).pathname.split('/')
      .filter(function (e) { return (e !== ''); }).slice(3);

    var destUrl = couchUrl + '/' + 
      nameIncarnate(internalId, reName, gl) + '/' + 
      pathInDb + 
      '?' + url_lib.parse(req.url).query;

    forward(req, destUrl, cb);
  }

  var forward = function (inReq, destUrl, cb) {
    var headers = inReq.headers;
    headers['X-Forwarded-For'] = inReq.connection.remoteAddress;
    headers['X-Forwarded-Port'] = inReq.connection.remotePort;

    request(
      {
        method: inReq.method,
        url: destUrl,
        headers: inReq.headers,
        followRedirects: false
      },
      function (err, outRes, outBody) {
        if (err) {
          cb(err);
          return;
        }
        cb(null, {res: outRes, body: outBody});
      }
    );
  }

  var initDbs = function (cb) {
    log.trace('init incarnation ' + id);
    log.trace('Initializing MapDb for: ' + id);
    mapDb.init( function (err) {
      if (err) { cb('ERR_MAP_INIT'); return; }
      if (Object.keys(incarnations).length === 0) {
        cb(); 
        return;
      }
      var reName, groupLevel;
      var incarnationInits = [], initsLeft = Object.keys(incarnations).length;
      var stopped = false;

      for (var incId in incarnations) {
        inc = incarnations[incId];
        inc.db.init(function (err) {
          initsLeft--;
          if (stopped) return;
          if (err) { 
            stopped = true; 
            cb(err);
            return; 
          }
          if (initsLeft === 0) {
            cb();
          }
        });
      }
    });
  }

  var delDbs = function (cb) {
    log.trace('incarnator ' + id + ': ' + 'deleting mapDb');
    mapDb.del( function (err) {
      if (err) { 
        log.warn('incarnator ' + id + ': ' + 'failed to delete mapDb');
        cb('ERR_MAP_INIT'); 
        return; 
      }
      var reName, groupLevel;
      var leftToDelete = Object.keys(incarnations).length, stopped = false;
      if (leftToDelete === 0) {
        cb();
        return;
      }

      for (var incId in incarnations) {
        (function (incId) {
          inc = incarnations[incId];
          log.trace('incarnator ' + id + ': ' + 'deleting incarnation-db: ' + incId);
          inc.db.del(function (err) {
            delete incarnations[incId];
            leftToDelete--;
            if (stopped) return;
            if (err) {
              log.warn('incarnator ' + id + ': ' + 'failed to delete incarnation-db: ' + incId);
              stopped = true;
              cb(err);
              return;
            }
            if (leftToDelete === 0) {
              cb();
            }
          });
        }(incId));
      }
    });
  }

  var saveState = function (cb) {
    persister.save(self.getState(), cb);
  }

  var setState = function (newState) {
    var newSourceDb = new SourceDb({
      url: newState.conf.source, 
      log: log
    });
    var newMapDb = new MapDb({
      id: newState.internalId,
      couchUrl: couchUrl,
      reduces: newState.conf.reduces,
      log: log
    });
    var newIncarnations = (function () {
      var reduces = newState.conf.reduces || {};
      var reName, incName;
      var reduce, i, 
        retval = {};
      for (reName in reduces) {
        groupLevels = reduces[reName].group_levels;
        for (i = 0; i < groupLevels.length; i++) {
          incName = nameIncarnate(newState.internalId, reName, groupLevels[i]);
          retval[incName] = {
            gl: groupLevels[i].toString(),
            reName: reName,
            db: new IncarnationDb({
              id: incName,
              couchUrl: couchUrl,
              log: log
            }) 
          };
        }
      }
      return retval;
    }());
    var newSyncerIncarnations = (function () {
      var retval = {};
      var inc, seq;
      for (var incId in newIncarnations) {
        inc = newIncarnations[incId];
        seq = newState && newState.mapToIncSeq && newState.mapToIncSeq[inc.reName] ? 
          newState.mapToIncSeq[inc.reName][inc.gl] : 0;
        retval[incId] = {
          reName: inc.reName,
          gl: inc.gl,
          db: inc.db,
          mapToIncSeq: seq
        };
      }
      return retval;
    }());
    try {
      syncer = new Syncer({
        id: id,
        sourceDb: newSourceDb,
        map: {
          db: newMapDb,
          fn: newState.conf.map,
          sourceToMapSeq: newState.sourceToMapSeq || 0
        },
        incarnations: newSyncerIncarnations,
        log: log
      });
    }
    catch (e) {
      throw new Error();
    }
    conf = newState.conf;
    status = newState.status || "initialized";
    internalId = newState.internalId;
    sourceDb = newSourceDb;
    mapDb = newMapDb;
    incarnations = newIncarnations;
  }
}


var errorCodes = {
  UNINITIALIZED: 0,
  NO_SUCH_INCARNATOR: 1,
  LOAD: 2,
  BAD_STATE: 3,
  BAD_CONF: 4
}

var IncarnatorError = function (errCode) {
  Error.apply(this);
  this.code = errCode;
}
util.inherits(IncarnatorError, Error);

Incarnator.errorCodes = errorCodes;
module.exports = Incarnator;


var util = require('util'),
  dbs = require('./dbs'),
  vm = require('vm');

var MapDb = dbs.MapDb,
  sourceDb = dbs.sourceDb,
  IncarnationDb = dbs.IncarnationDb,
  Logger = require('./logger');



// opts:
//  id
//  sourceDb
//  map: {
//    db: ...,
//    sourceToMapSeq: ...
//  }
//  incarnations: [
//    {
//      mapToIncSeq: ...,
//      db: ...,
//      reName: ...,
//      gl: ...
//    },
//    ...
//  ]
//  [log]
//    
var Syncer = function (opts) {

  var log = opts.log || new Logger();
  var id = opts.id;
  var map = opts.map;
  var sourceToMapSeq = opts.map_last_sync_seq || 0;
  var mapToIncSeqs = {}
  var sourceDb = opts.sourceDb;
  var status = 'idle';
  var incs = opts.incarnations;
  var syncingIncsById;

  var verifyMapStringIsFunction = function () {
    var basicFuncTemplate = new RegExp(
      '^' + 
      '\\s*' + 
      'function' + 
      '\\s*' + 
      '\\(([^\\)]*)\\)' + 
      '\\s*' + 
      '{([\\s\\S]*)}' + 
      '\\s*' + 
      '$'
    );
    var match = basicFuncTemplate.exec(map.fn);
    if (!match) throw new Error();
    try {
      var argsStr = match[1];
      var bodyStr = match[2];
      new Function(argsStr, bodyStr);
    }
    catch (e) {
      throw new Error();
    }
  }

  try {
    verifyMapStringIsFunction();
  }
  catch (e) {
    throw new Error();
  }

  this.getSourceToMapSeq = function () {
    return sourceToMapSeq;
  }


  this.getMapToIncSeq = function (incId) {
    return incs[incId].mapToIncSeq;
  }


  this.sync = function (incIds, cb) {
    log.info('syncing ' + id + ' incarnations: ' + incIds);
    syncingIncsById = {};
    incIds.forEach( function (id) {
      syncingIncsById[id] = true;
    });
    status = 'mapDb'
    syncMapDbFromSource( function (err) {
    if (err) {
      log.warn('failed to sync MapDB from source for ' + id);
      stop();
      cb(new Error('MAP_SYNC'));
      return;
    }
    status = 'incarnations';
    syncIncDbsFromMapDb( function (err) {
    if (err) {
      log.warn('failed to sync some incarnation DB\'s from MapDB for ' + id);
      stop();
      cb(new Error('INC_SYNC'));
      return;
    }
    cb();
    });});
  }


  var stop = function () {
    status = 'idle';
    syncingIncsById = null;
  }


  this.addToRunningSync = function (incId) {
    if (status === 'idle' || status === 'incarnations') {
      throw new Error('add_failed');
    }
    syncingIncsById[incId] = true;
  }

  this.isSyncingMap = function () {
    return status === 'map';
  }

  var toInt = function (str) {
    var integer = parseInt(str, 10);
    return (!isNaN(integer) && integer.toString() === str) ? integer : null;
  };
  
  var keysUnion = function () {
    var args = Array.prototype.slice.call(arguments);
    var i, key, obj, allKeys = {};

    for (i = 0; i < args.length; i++) {
      obj = args[i];
      for (key in obj) {
        allKeys[key] = true;
      }
    }
    return Object.keys(allKeys);
  }
  
  var emitFor = function (changes) {

    var adjustedMap = function (doc) {
      var emits = {}
      var sandbox = {
        emit: function (key, value) {
          var emittedKey = JSON.stringify([key, doc._id]);
          emits[emittedKey] = {
            key: key, 
            id: doc._id,
            value: value
          }
        }
      }
      var code = '(' + map.fn + '(' + JSON.stringify(doc) + '))';
      try {
        vm.runInNewContext(code, sandbox);
      }
      catch (e) {
        throw new Error();
      }
      return emits;
    }

    var emitsForId, emitsByOrigin = {};
    for (var i = 0; i < changes.length; i++) {
      var doc = changes[i].doc;
      var id = changes[i].id;
      if (id[0] !== '_' && !changes[i].changes.deleted) {
        try {
          emitsForId = adjustedMap(doc);
        }
        catch (e) {
          throw new Error();
        }
        //if (Object.keys(emitsForId).length !== 0) {
          emitsByOrigin[id] = emitsForId;
        //}
      }
    }
    return emitsByOrigin;
  }
  
  
  var compareSets = function (o1, o2) {
    var res = {
      onlyIn1: [],
      onlyIn2: [],
      inBoth: []
    }
    var key;
    for (key in o1) {
      if (o2[key]) {
        res.inBoth.push(key);
      }
      else {
        res.onlyIn1.push(key);
      }
    }
    for (key in o2) {
      if (!o1[key]) {
        res.onlyIn2.push(key);
      }
    }
    return res;
  }
  
  
  var createChangeSet = function (oldDocs, newDocs) {
    var cmp = compareSets(newDocs || {}, oldDocs || {});
    var keysToAdd = cmp.onlyIn1;
    var keysToDelete = cmp.onlyIn2;
    //TODO keysToUpdate: in case of recovering from failure, we wouldn't know
    //  which of the destination's docs are already updated.
    //  Currently, we update them again to make sure, but it does change their
    //  _rev num. Instead, we could diff and skip identical docs, but we would
    //  also skip identical docs with just a rev-change. What's better?
    var keysToUpdate = cmp.inBoth;
  
    var changeSet = [];
    var i, key, operation, oldDoc;
  
    for (i = 0; i < keysToAdd.length; i++) {
      key = keysToAdd[i];
      operation = newDocs[key];
      operation._id = key;
      changeSet.push(operation);
    }
    for (i = 0; i < keysToDelete.length; i++) {
      key = keysToDelete[i];
      operation = {
        _id: key,
        _rev: oldDocs[key]._rev,
        _deleted: true
      }
      changeSet.push(operation);
    }
    for (i = 0; i < keysToUpdate.length; i++) {
      key = keysToUpdate[i];
      operation = newDocs[key];
      operation._id = key;
      operation._rev = oldDocs[key]._rev;
      changeSet.push(operation);
    }
    return changeSet;
  }
  
  
  var incKeysFromMapKeys = function (changes, groupLevel) {
    var glNumber = toInt(groupLevel);
    var incKeys = {};
    if (groupLevel === "0") {
      for (var i = 0; i < changes.length; i++) {
        if (changes[i].id[0] !== '_') {
          incKeys[JSON.stringify(null)] = true;
        }
      }
    }
    else {
      changes.forEach( function (change) {
        if (change.id[0] === '_') return;
        var mapKey = JSON.parse(change.id)[0];
        var incKey = (Array.isArray(mapKey) && glNumber !== null) ? mapKey.slice(0, glNumber) : mapKey;
        incKeys[JSON.stringify(incKey)] = true;
      });
    }
    return Object.keys(incKeys);
  }
  
  
  // opts:
  //  db
  //  viewPath
  //  queriesById
  //  rowToDbEntry
  var getViewResultsByQueryAsDictionaries = function (opts, cb) {
    var rowToDbEntry = opts.rowToDbEntry;
    opts.db.multiqueryView(opts.viewPath, opts.queriesById, function (err, resultsByQueryId) {
      if (err) { cb(err); return; }
      var valuesByOrigin = {};
      for (var id in resultsByQueryId) {
        resultsByQueryId[id].rows.forEach( function (row) {
          var entry = rowToDbEntry(row, id);
          valuesByOrigin[id] = valuesByOrigin[id] || {};
          valuesByOrigin[id][entry.docId] = entry.docContents;
        });
      }
      cb(null, valuesByOrigin);
    });
  }

  var syncMapDbFromSourceChanges = function (since, limit, cb) {

    var getPastEmitsMetaByOrigin = function (changes, cb) {
      var docId, change, queriesById = {};
      for (var i = 0; i < changes.length; i++) {
        change = changes[i];
        docId = change.id;
        if (docId[0] !== '_') {
          queriesById[docId] = {key: docId};
        }
      }
      getViewResultsByQueryAsDictionaries(
        {
          db: map.db,
          viewPath: '_design/incarnate/_view/meta_by_origin',
          queriesById: queriesById,
          rowToDbEntry: function (row, idString) {
            return {
              docId: row.id,
              docContents: { _rev: row.value._rev }
            };
          }
        },
        cb
      );
    }

    // 1. calculate map-values resulting from recently updated sourceDb docs
    log.trace('syncer ' + id + ': calculating new map-values');
    var changesOpts = {
      since: since, 
      limit: limit,
      include_docs: true, 
      reduce: false
    }
    sourceDb.changes(changesOpts, function (err, changes) {
    if (err) {
      log.warn('syncer ' + id + ': failed to fetch changes from source');
      cb(err); 
      return; 
    }
    if (changes.results.length === 0) {
      cb(null, since, 0);
      return;
    }
    changes = changes.results;
    var emitsByOrigin;
    try {
      emitsByOrigin = emitFor(changes);
    }
    catch (e) {
      log.warn('syncer ' + id + ': emitting from source-DB changes failed');
      cb(new Error()); 
      return; 
    }


    // 2. get previous map-values resulting from same sourceDb docs
    log.trace('syncer ' + id + ': fetching previous map-values');
    getPastEmitsMetaByOrigin(changes, function (err, pastEmitsMetaByOrigin) {
    if (err) {
      log.warn('syncer ' + id + ': failed to fetch docs from MapDB');
      cb(err); 
      return; 
    }


    // 3. deduce change-set between new and previous values
    log.trace('syncer ' + id + ': submitting change-set to MapDB');
    var origins = keysUnion(emitsByOrigin, pastEmitsMetaByOrigin);
    var changeSet = [];
    var originChangeSet;
    for (var i = 0; i < origins.length; i++) {
      origin = origins[i];
      originChangeSet = createChangeSet(
        pastEmitsMetaByOrigin[origin], 
        emitsByOrigin[origin]
      );
      changeSet = changeSet.concat(originChangeSet);
    }
    map.db.bulkDocs({docs: changeSet, all_or_nothing: true}, function (err, docs) {
    if (err) {
      log.warn('syncer ' + id + ': failed to submit change-set to MapDB');
      cb(err); 
      return; 
    }

    cb(null, changes[changes.length - 1].seq, changes.length);
    });});});
  }

  var syncMapDbFromSource = function (cb) {
    var since = sourceToMapSeq;
    var limit = 100000;
    var syncCallback = function (err, lastSyncedSeq, changesSyncedNum) {
      if (err) {
        cb(err);
        return;
      }
      if (changesSyncedNum < limit) {
        sourceToMapSeq = lastSyncedSeq;
        cb();
        return;
      }
      since = lastSyncedSeq;
      syncMapDbFromSourceChanges(since, limit, syncCallback);
    }
    syncMapDbFromSourceChanges(since, limit, syncCallback);
  }

  var syncIncDbsFromMapDb = function (cb) {
    var syncsNotDone = Object.keys(syncingIncsById).length;
    var stopped = false;
    for (var incId in syncingIncsById) {
      syncIncFromMapDb(incId, function (err) {
        syncsNotDone--;
        if (stopped) return;
        if (err) {
          stopped = true;
          cb(err);
          return;
        }
        if (syncsNotDone === 0) {
          cb();
        }
      });
    }
  }

  var syncIncFromMapDb = function (incId, cb) {
    var inc = incs[incId];
    var since = inc.mapToIncSeq;
    var limit = 100000;
    var syncCallback = function (err, lastSyncedSeq, changesSyncedNum) {
      if (err) {
        cb(err);
        return;
      }
      if (changesSyncedNum < limit) {
        inc.mapToIncSeq = lastSyncedSeq;
        cb();
        return;
      }
      since = lastSyncedSeq;
      syncIncFromMapDbChanges(incId, since, limit, syncCallback);
    }
    syncIncFromMapDbChanges(incId, since, limit, syncCallback);
  }

  var syncIncFromMapDbChanges = function (incId, since, limit, cb) {

    var newIncValuesByOrigin = function (queryKeys, reName, gl, cb) {
      var queriesById = gl === '0' ?
        {'null': {group: true, group_level: 0}}
        :
        (function () {
          var retval = {};
          var intGl = toInt(inc.gl);
          queryKeys.forEach( function (key) {
            var parsedKey = JSON.parse(key);
            retval[key] = intGl !== null && Array.isArray(parsedKey) && parsedKey.length >= intGl ? 
              {
                startkey: parsedKey,
                endkey: [].concat(parsedKey).concat([{}]),
                group: true
              } :
              {
                key: parsedKey,
                group: true
              };
            if (intGl !== null) {
              retval[key].group_level = intGl;
            }
          });
          return retval;
        }());
  
      getViewResultsByQueryAsDictionaries(
        {
          db: map.db,
          viewPath: '_design/incarnate/_view/' + reName,
          queriesById: queriesById,
          rowToDbEntry: function (row, qId) {
            var unstringifiedDocId, query = queriesById[qId];
            if (query.startkey !== undefined) {
              unstringifiedDocId = query.startkey;
            }
            else if (query.key !== undefined) {
              unstringifiedDocId = query.key;
            }
            else {
              unstringifiedDocId = null;
            }
            return {
              docId: qId,
              docContents: {
                key: unstringifiedDocId,
                value: row.value
              }
            };
          }
        },
        cb
      );
    }
  
    var pastIncValuesByOrigin = function (queryKeys, incDb, cb) {
      queriesById= {};
      queryKeys.forEach( function (key) {
        queriesById[key] = {key: key};
      });
  
      //TODO doc heads are enough, we only want to figure out if they already exist
      getViewResultsByQueryAsDictionaries(
        {
          db: incDb,
          viewPath: '_all_docs',
          queriesById: queriesById,
          rowToDbEntry: function (row, idString) {
            return {
              docId: row.key,
              docContents: {_rev: row.value.rev}
            };
          }
        },
        cb
      );
    }

    var inc = incs[incId];

    var logPrefix = 'syncer ' + id + ': incarnation ' + incId + ': ';

    log.trace(logPrefix + 'fetching changes from MapDB');
    map.db.changes({since: since, limit: limit}, function (err, changes) {
    log.trace('MapDB changes: ' + JSON.stringify(changes, null, ' '));
    if (err) { 
      log.warn(logPrefix + 'failed to fetch changes from MapDB');
      cb(err); 
      return; 
    }
    if (changes.results.length === 0) {
      cb(null, since, 0);
      return;
    }
    var queryKeys = incKeysFromMapKeys(changes.results, inc.gl);

    log.trace(logPrefix + 'fetch new values from MapDB');
    newIncValuesByOrigin(queryKeys, inc.reName, inc.gl, function (err, valuesByOrigin) {
    if (err) { 
      log.warn(logPrefix + 'failed to fetch new values from MapDB');
      cb(err); 
      return; 
    }

    log.trace(logPrefix + 'fetch previous incarnationDB values');
    pastIncValuesByOrigin(queryKeys, inc.db, function (err, pastValuesByOrigin) {
    if (err) { 
      log.warn(logPrefix + 'failed to fetch previous docs from incarnationDB');
      cb(err); 
      return; 
    }

    log.trace(logPrefix + 'submit change-set to incarnationDB');
    var changeSet = [], keyChangeSet;
    queryKeys.forEach( function (origin) {
      var originChangeSet = createChangeSet(pastValuesByOrigin[origin], valuesByOrigin[origin]);
      changeSet = changeSet.concat(originChangeSet);
    });

    inc.db.bulkDocs({docs: changeSet, all_or_nothing: true}, function (err) {
    if (err) { 
      log.warn(logPrefix + 'failed to submit changes to incarnationDB');
      cb(err); 
      return; 
    }

    cb(null, changes.last_seq, changes.results.length);
    });});});});
  }
}

var errorCodes = {
  MAP_SYNC: 0,
  INC_SYNC: 1,
  ADD: 2
}

var SyncerError = function (errCode) {
  Error.apply(this);
  this.code = errCode;
}
util.inherits(SyncerError, Error);

Syncer.errorCodes = errorCodes;
module.exports = Syncer;

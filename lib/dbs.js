var util = require('util'),
  Logger = require('./logger'),
  couchdb = require('./couchdb');


// opts:
//  url
//  [log]
var SourceDb = function (opts) {
  var url = opts.url,
    log = opts.log || new Logger();

  var db = new couchdb.Db(url);

  this.getUrl = function () {
    return url;
  }

  this.changes = function (opts, cb) {
    db.changes(opts, cb);
  }
}


// opts:
//  id
//  couchUrl
//  reduces
//  lastSyncSeq
//  [log]
var MapDb = function (opts) {

  var id = opts.id;
  var couchUrl = opts.couchUrl;
  var reduces = opts.reduces;
  var lastSyncSeq = opts.lastSyncSeq;
  var log = opts.log || new Logger();

  var couch = new couchdb.Couch(couchUrl);
  var dbName = 'incarnate_' + id + '__map';
  var db = couch.db(dbName);

  var results = {};

  this.lastSyncSeq = lastSyncSeq;

  this.getDbName = function () {
    return dbName;
  }

  var setViews = function (cb) {
    var designId = '_design/incarnate';

    db.getDoc(designId, function (err, doc) {
      if (err && err !== 404) { cb(err); return; }
      doc = doc || { _id: designId, views: {} };
      doc.views.meta_by_origin = {
        map: (function (doc) {
          emit(doc.id, {_id: doc._id, _rev: doc._rev});
        }).toString()
      }
      
      var reduceStr, newReduceStr;
      if (reduces && Object.keys(reduces).length) {
        for (var name in reduces) {
          //TODO should 'reduceStr' be checked here? Could couch catch carelessly contrived code?
          reduceStr = reduces[name]["function"];
          newReduce = 'function (keys, values, rereduce) {\n' +
            'var origFn = ' + reduceStr + ';\n' +
            'if (!rereduce) {\n' +
            '  keys = values.map(function (v) { return v.key; });\n' + 
            '  values = values.map(function (v) { return v.value; });\n' +
            '}\n' +
            'return origFn(keys, values, rereduce);\n' +
          '}';
          doc.views[name] = {
            map: (function (doc) { return emit(doc.key, doc); }).toString(),
            reduce: newReduce
          }
        }
      }
      log.trace('setting mapDB views: ' + dbName);
      db.setDoc(doc, function (err) {
        if (err) { cb(err); return; }
        cb();
      });
    });
  }

  this.init = function (cb) {
    this.del(function (err) {
      if (err) { cb(err); return; }
      log.trace('creating mapDB: ' + dbName);
      couch.createDb(dbName, function (err) {
        if (err) { cb('CREATE_DB_ERROR'); return; }
        setViews( function (err) {
          if (err) { cb('ERR_SET_VIEWS_FUNC'); return; }
          cb();
        });
      });
    });
  }

  this.del = function (cb) {
    couch.deleteDb(dbName, function (err) {
      if (err && err !== 404) { cb('DELETE_DB_ERROR'); return; }
      cb();
    });
  }
  
  this.multiqueryView = function (viewPath, queriesById, mapRow, cb) {
    db.multiqueryView(viewPath, queriesById, mapRow, cb);
  }

  this.changes = function (opts, cb) {
    db.changes(opts, cb);
  }
  
  this.bulkDocs = function (opts, cb) {
    db.bulkDocs(opts, cb);
  }

}


// opts:
//  id
//  couchUrl
//  [log]
var IncarnationDb = function (opts) {
  var id = opts.id;
  var couchUrl = opts.couchUrl;
  var log = opts.log || new Logger();

  var couch = new couchdb.Couch(couchUrl);
  var dbName = id;
  var db = couch.db(dbName);

  this.getDbName = function () {
    return dbName;
  }

  var fetchDesignDocs = function (cb) {
    var dDocs = {};
    db.queryView('_all_docs', 
      {
        startkey: "_design/", 
        endkey: "_design0"
      }, 
        function (err, res) {
      if (err && err !== 404) { cb(err); return; }
      if (!err) {
        res.rows.forEach( function (row) {
          dDocs[row.id] = row.doc;
        });
      }
      cb(null, dDocs);
    });
  }

  var placeDesignDocs = function (dDocs, cb) {
    var dDoc, changeSet = [];
    for (var id in dDocs) {
      dDoc = dDocs[id];
      delete dDoc._rev;
      changeSet.push(dDoc);
    }
    db.bulkDocs({docs: changeSet, all_or_nothing: true}, cb);
  }
  
  this.init = function (cb) {
    log.trace('fetching design docs of: ' + dbName);
    fetchDesignDocs( function (err, dDocs) {
      if (err && err !== 404) { cb(err); return; }
      log.trace('deleting db: ' + dbName);
      couch.deleteDb(dbName, function (err) {
        if (err && err !== 404) { cb(err); return; }
        log.trace('creating db: ' + dbName);
        couch.createDb(dbName, function (err) {
          if (err) { cb(err); return; }
          if (Object.keys(dDocs).length === 0) {
            cb();
            return;
          }
          log.trace('placing design docs in: ' + dbName);
          placeDesignDocs(dDocs, cb);
        });
      });
    });
  }

  this.getDocs = function (keys, cb) {
    db.getDocs(keys, cb);
  }

  this.bulkDocs = function (opts, cb) {
    db.bulkDocs(opts, cb);
  }

  this.del = function (cb) {
    couch.deleteDb(dbName, function (err) {
      if (err && err !== 404) { cb('DELETE_DB_ERROR'); return; }
      cb();
    });
  }

  this.multiqueryView = function (viewPath, queriesById, mapRow, cb) {
    db.multiqueryView(viewPath, queriesById, mapRow, cb);
  }
}

exports.IncarnationDb = IncarnationDb;
exports.SourceDb = SourceDb;
exports.MapDb = MapDb;

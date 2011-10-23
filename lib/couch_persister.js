var fs = require('fs'),
  couchdb = require('./couchdb'),
  path_lib = require('path');
        
var Persister = function (couchUrl, dbName, docId) {

  var couch = new couchdb.Couch(couchUrl);
  var db = couch.db(dbName);

  this.save = function (state, cb) {
    couch.createDb(dbName, function (err) {
    if (err && err !== 412) {
      cb(new PersisterError(errorCodes.PERSISTER_FAULT));
      return;
    }
    db.getDoc(docId, function (err, oldStateDoc) {
    if (err && err !== 404) {
      cb(new PersisterError(errorCodes.PERSISTER_FAULT));
      return;
    }
    var newStateDoc = {_id: docId};
    for (var field in state) {
      newStateDoc[field] = state[field];
    }
    if (oldStateDoc) {
      newStateDoc._rev = oldStateDoc._rev;
    }
    db.setDoc(newStateDoc, function (err) {
    if (err) {
      cb(new PersisterError(errorCodes.PERSISTER_FAULT));
      return;
    }
    cb();
    })})});
  }

  this.load = function (cb) {
    db.getDoc(docId, function (err, stateDoc) {
      if (err) {
        if (err === 404) {
          cb(new PersisterError(errorCodes.NO_SUCH_INC));
          return;
        }
        cb(new PersisterError(errorCodes.PERSISTER_FAULT));
        return;
      }
      delete stateDoc._id;
      delete stateDoc._rev;
      cb(null, stateDoc);
    });
  }

  this.rm = function (cb) {
    db.getDoc(docId, function (err, stateDoc) {
    if (err && err !== 404) {
      cb(new PersisterError(errorCodes.PERSISTER_FAULT));
      return;
    }
    db.delDoc(stateDoc._id, stateDoc._rev, function (err) {
    if (err) {
      cb(new PersisterError(errorCodes.PERSISTER_FAULT));
      return;
    }
    cb();
    })});
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

var util = require('util'),
  qs = require('querystring'),
  request = require('request');

var Db = function (uri) {
  var self = this;

  this.docExists = function (name, cb) {
    request(
      {
        method: 'HEAD',
        uri: uri + '/' + name
      },
      function (err, res) {
        if (err) {
          cb('CONN_ERROR');
          return;
        }
        if (res.statusCode !== 200) {
          cb(res.statusCode);
          return;
        }
        cb();
      }
    );
  }

  this.getDoc = function (id, cb) {
    request(
      {
        method: 'GET',
        uri: uri + '/' + id,
      },
      function (err, res, body) {
        if (err) { cb('CONN_ERROR'); return; }
        if (res.statusCode !== 200) {
          cb(res.statusCode);
          return;
        }
        cb(null, body);
      }
    );
  }

  var keyValuefy = function (viewResults) {
    var retval = {};
    var rows = viewResults.rows;
    for (var i = 0; i < rows.length; i++) {
      retval[rows[i].key] = rows[i].doc;
    }
    return retval;
  }

  this.getDocs = function (ids, cb) {
    request(
      {
        method: 'POST',
        uri: uri + '/_all_docs?include_docs=true',
        headers: {
          'Content-Type': 'application/json; charset=utf-8'
        },
        body: JSON.stringify({
          keys: ids
        })
      },
      function (err, res, body) {
        if (err) {
          cb('CONN_ERROR');
          return;
        }
        if (res.statusCode !== 200) {
          cb(res.statusCode);
          return;
        }
        cb(null, keyValuefy(JSON.parse(body)));
      }
    );
  }

  this.setDoc = function (doc, cb) {
    request(
      {
        method: doc._id ? 'PUT' : 'POST',
        uri: doc._id ? uri + '/' + doc._id : uri,
        headers: {
          'Content-Type': 'application/json; charset=utf-8'
        },
        body: JSON.stringify(doc)
      },
      function (err, res) {
        if (err) {
          cb('CONN_ERROR');
          return;
        }
        if (res.statusCode !== 201) {
          cb(res.statusCode);
          return;
        }
        cb();
      }
    );
  }

  // viewName
  // opts:
  //  docs
  //  all_or_nothing [false]
  this.bulkDocs = function (opts, cb) {
    request(
      {
        method: 'POST',
        uri: uri + '/_bulk_docs',
        headers: {
          'Content-Type': 'application/json; charset=utf-8'
        },
        body: JSON.stringify({
          all_or_nothing: opts.all_or_nothing,
          docs: opts.docs
        })
      },
      function (err, res, body) {
        if (err) {
          cb('CONN_ERROR');
          return;
        }
        if (res.statusCode !== 201) {
          cb(res.statusCode);
          return;
        }
        cb(null, JSON.parse(body));
      }
    );
  }

  var jsonifyValues = function (obj) {
    var jsonified = {};
    for (var key in obj) {
      jsonified[key] = JSON.stringify(obj[key]);
    }
    return jsonified;
  }

  this.queryView = function (viewPath, opts, cb) {
    var args = qs.stringify(jsonifyValues(opts), '&', '=');
    request(
      {
        method: 'GET',
        uri: uri + '/' + viewPath+ '?' + args
      },
      function (err, res, body) {
        if (err) { cb('CONN_ERR'); return; }
        if (res.statusCode !== 200) { cb(res.statusCode); return; }
        cb(null, JSON.parse(body));
      }
    );
  }

  this.multiqueryView = function (viewPath, queriesById, cb) {
    var stopped = false;
    var resultsById = {};
    var queriesLeft = Object.keys(queriesById).length;
    for (var queryId in queriesById) {
      (function (queryId) {
        var queryOpts = queriesById[queryId];
        self.queryView(viewPath, queryOpts, function (err, res) {
          if (stopped) return;
          if (err) { 
            stopped = true; 
            cb(err); 
            return ;
          }
          resultsById[queryId] = res;
          if (--queriesLeft === 0) {
            stopped = true;
            cb(null, resultsById);
            return;
          }
        });
      }(queryId));
    }
  }

  this.changes = function (opts, cb) {
    var args = qs.stringify(jsonifyValues(opts), '&', '=');
    request(
      {
        method: 'GET',
        uri: uri + '/_changes?' + args
      },
      function (err, res, body) {
        if (err) { cb('CONN_ERR'); return; }
        if (res.statusCode !== 200) { cb(res.statusCode); return; }
        cb(null, JSON.parse(body));
      }
    );
  }
}

var Couch = function (uri) {
  var self = this;

  this.createDb = function (dbName, cb) {
    request(
      {
        method: 'PUT',
        uri: uri + '/' + dbName
      },
      function (err, res) {
        if (err) {
          cb('CONN_ERROR');
          return;
        }
        if (res.statusCode !== 201) {
          cb(res.statusCode);
          return;
        }
        cb(null, self.db(dbName));
      }
    );
  }

  this.deleteDb = function (dbName, cb) {
    request(
      {
        method: 'DELETE',
        uri: uri + '/' + encodeURIComponent(dbName)
      },
      function (err, res) {
        if (err) {
          cb('CONN_ERROR');
          return;
        }
        if (res.statusCode !== 200) {
          cb(res.statusCode);
          return;
        }
        cb();
      }
    );
  }

  this.db = function (dbName, cb) {
    return new Db(uri + '/' + dbName);
  }
}

exports.Db = Db;
exports.Couch = Couch;

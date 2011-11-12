var util = require('util'),
  assert = require('assert'),
  request = require('request');

var serverUrl = "http://localhost:4895"
var sourceDbUrl = "http://localhost:5984/my_db";

var randomIntString = function () {
  return Math.random().toString().substr(2);
}

var genIncarnatorName = randomIntString;

var getIncarnatorState = function (name, cb) {
  request({
    method: 'GET',
    uri: serverUrl + '/' + name
  }, cb);
}
    
var createIncarnator = function (name, conf, cb) {
  request({
    method: 'PUT',
    uri: serverUrl + '/' + name,
    body: JSON.stringify(conf)
  }, cb);
}

var delIncarnator = function (name, cb) {
  request({
    method: 'DELETE',
    uri: serverUrl + '/' + name
  }, cb);
}

var getIncarnationState = function (path, cb) {
  request({
    method: 'GET',
    uri: serverUrl + '/' + path
  }, cb);
}

var basicConf = {
  "source": sourceDbUrl,
  "map": "function (doc) { \n  if (doc.score && doc.for !== undefined) {\n    emit(doc.for, doc.score); \n  }\n}",
  "reduces": {
    "count": {
      "function": "function (key, values, rereduce) { \n  if (!rereduce) return values.length; \n  var count = 0; \n  for (var i = 0; i < values.length; i++) { \n    count += values[i]; \n  }; \n  return count; \n}\n",
      "group_levels": [1]
    }
  }
}

// opts: 
//  interval // [100] should be big enough to make sure that call-order will be the same when received at the server
//  baseInterval // [0]
var TimeKeeper = function (opts) {
  opts = opts || {};
  var interval = opts.interval === undefined || opts.interval === undefined ? 
    100 : opts.interval;
  var baseInterval = opts.baseInterval === undefined || opts.baseInterval === undefined ? 
    0 : opts.interval;
  var tick = 0;
  this.nextTick = function (cb) {
    setTimeout(cb, baseInterval + interval*(tick++));
  }
}

var basicCreateAndDelete = function (cb) {
  var basicInctorName = genIncarnatorName();
  var timeKeeper = new TimeKeeper();
  var testsToFinish = 0;

  var done = function () {
    testsToFinish--;
    if (!testsToFinish) {
      return cb && cb();
    }
  }

  testsToFinish++;
  timeKeeper.nextTick( function () {
  delIncarnator(basicInctorName, function (err, res, body) {
    console.log('delete nonexistent inctor 1');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 404);
    done();
  })});

  testsToFinish++;
  timeKeeper.nextTick( function () {
  createIncarnator(basicInctorName, basicConf, function (err, res, body) {
    console.log('create non-existent basic inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 201);
    done();
  })});
  
  testsToFinish++;
  timeKeeper.nextTick( function () {
  getIncarnatorState(basicInctorName, function (err, res, body) {
    console.log('get inctor state');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 200);
    done();
  })});
  
  testsToFinish++;
  timeKeeper.nextTick( function () {
  delIncarnator(basicInctorName, function (err, res, body) {
    console.log('delete inctor 2');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 200);
    done();
  })});
}

var incarnatorInitsAreProperlyQueued = function (cb) {
  var basicInctorName = genIncarnatorName();
  var timeKeeper = new TimeKeeper();
  var testsToFinish = 0;

  var done = function () {
    testsToFinish--;
    if (!testsToFinish) {
      return cb && cb();
    }
  }

  testsToFinish++;
  timeKeeper.nextTick( function () {
  createIncarnator(basicInctorName, basicConf, function (err, res, body) {
    console.log('create non-existent basic inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 201);
    done();
  })});
  
  testsToFinish++;
  timeKeeper.nextTick( function () {
  createIncarnator(basicInctorName, basicConf, function (err, res, body) {
    console.log('create non-existent basic inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 201);
    done();
  })});
  
  testsToFinish++;
  timeKeeper.nextTick( function () {
  createIncarnator(basicInctorName, basicConf, function (err, res, body) {
    console.log('create non-existent basic inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 201);
    done();
  })});
  
  testsToFinish++;
  timeKeeper.nextTick( function () {
  delIncarnator(basicInctorName, function (err, res, body) {
    console.log('delete inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 200);
    done();
  })});
}

var basicAccessIncarnation = function (cb) {
  var timeKeeper = new TimeKeeper();
  var testsToFinish = 0;
  var done = function () {
    testsToFinish--;
    if (!testsToFinish) {
      return cb && cb();
    }
  }

  var inctorName = genIncarnatorName();
  var reduceName = 'count';
  var groupLevel = '1';
  var inctionPath = inctorName + '/' + reduceName + '/' + groupLevel;

  testsToFinish++;
  timeKeeper.nextTick( function () {
  getIncarnationState(inctionPath, function (err, res, body) {
    console.log('access incarnation in nonexistent inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 404);
    done();
  })});

  testsToFinish++;
  timeKeeper.nextTick( function () {
  createIncarnator(inctorName, basicConf, function (err, res, body) {
    console.log('create nonexistent basic inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 201);
    done();
  })});

  testsToFinish++;
  timeKeeper.nextTick( function () {
  var bogusInctionPath = inctorName + '/boguspath/4';
  getIncarnationState(bogusInctionPath, function (err, res, body) {
    console.log('access incarnation with nonexistent reduce-name');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 404);
    done();
  })});

  testsToFinish++;
  timeKeeper.nextTick( function () {
  var bogusInctionPath = inctorName + '/' + reduceName + '/9';
  getIncarnationState(bogusInctionPath, function (err, res, body) {
    console.log('access incarnation with nonexistent group-level');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 404);
    done();
  })});

  testsToFinish++;
  timeKeeper.nextTick( function () {
  getIncarnationState(inctionPath, function (err, res, body) {
    console.log('access incarnation');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 200);
    done();
  })});

  testsToFinish++;
  timeKeeper.nextTick( function () {
  delIncarnator(inctorName, function (err, res, body) {
    console.log('delete inctor');
    assert.strictEqual(err, null, "request got error");
    assert.strictEqual(res.statusCode, 200);
    done();
  })});
}

request(
  {
    method: 'GET',
    uri: sourceDbUrl,
  }, function (err, res, body) {
    if (err || res.statusCode !== 201) {
      throw new Error('unable to access DB at: ' + sourceDbUrl)
    }
    basicCreateAndDelete();
    incarnatorInitsAreProperlyQueued();
    basicAccessIncarnation();
  }
);



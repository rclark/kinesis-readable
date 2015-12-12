var tape = require('tape');
var queue = require('queue-async');
var crypto = require('crypto');

var kinesalite = require('kinesalite')({
  ssl: false,
  createStreamMs: 1,
  deleteStreamMs: 1
});

var AWS = require('aws-sdk');
var kinesis = new AWS.Kinesis({
  accessKeyId: '-',
  secretAccessKey: '-',
  endpoint: 'http://localhost:7654',
  region: '-'
});

var testStreamName = 'test-stream';

function test(name, callback) {
  tape('start kinesalite', function(assert) {
    queue(1)
      .defer(kinesalite.listen.bind(kinesalite), 7654)
      .defer(kinesis.createStream.bind(kinesis), {
        StreamName: testStreamName,
        ShardCount: 1
      })
      .await(function(err) {
        if (err) throw err;
        assert.end();
      });
  });

  tape(name, callback);

  tape('stop kinesalite', function(assert) {
    queue(1)
      .defer(function(next) {
        kinesis.deleteStream({
          StreamName: testStreamName
        }, function(err) {
          if (err) return next(err);
          setTimeout(next, 20);
        });
      })
      .defer(kinesalite.close.bind(kinesalite))
      .await(function(err) {
        if (err) throw err;
        assert.end();
      });
  });
}

test('reads records that already exist', function(assert) {
  var records = [];
  for (var i = 0; i < 20; i++) records.push({
    Data: crypto.randomBytes(10),
    PartitionKey: 'key'
  });

  kinesis.putRecords({
    StreamName: testStreamName,
    Records: records
  }, function(err) {
    if (err) throw err;
    readRecords();
  });

  function readRecords() {
    var readable = require('..')(kinesis, testStreamName);

    var count = 0;

    readable
      .on('data', function(recordSet) {
        assert.equal(recordSet.length, 20, 'got records');

        recordSet.forEach(function(record, i) {
          var expected = records[i].Data.toString('hex');
          assert.equal(record.Data.toString('hex'), expected, 'anticipated data');
        });

        count += recordSet.length;
        if (count > records.length) assert.fail('should not read extra records');
        if (count === records.length) readable.close();
      })
      .on('end', function() {
        assert.ok('fires end event after close');
        assert.end();
      })
      .on('error', function(err) {
        assert.ifError(err, 'should not error');
      });
  }
});

test('can use stream name predefined by the client', function(assert) {
  var records = [];
  for (var i = 0; i < 20; i++) records.push({
    Data: crypto.randomBytes(10),
    PartitionKey: 'key'
  });

  kinesis.putRecords({
    StreamName: testStreamName,
    Records: records
  }, function(err) {
    if (err) throw err;
    readRecords();
  });

  function readRecords() {
    var client = new AWS.Kinesis({
      params: { StreamName: testStreamName },
      accessKeyId: '-',
      secretAccessKey: '-',
      endpoint: 'http://localhost:7654',
      region: '-'
    });

    var readable = require('..')(client);

    var count = 0;

    readable
      .on('data', function(recordSet) {
        count += recordSet.length;
        if (count > records.length) assert.fail('should not read extra records');
        if (count === records.length) readable.close();
      })
      .on('end', function() {
        assert.equal(count, 20, 'read 20 records');
        assert.end();
      })
      .on('error', function(err) {
        assert.ifError(err, 'should not error');
      });
  }
});

test('reads ongoing records', function(assert) {
  var records = [];
  for (var i = 0; i < 20; i++) records.push({
    Data: crypto.randomBytes(10),
    PartitionKey: 'key'
  });

  var readable = require('..')(kinesis, testStreamName);
  var count = 0;

  readable
    .on('data', function(recordSet) {
      count += recordSet.length;
      if (count > records.length) assert.fail('should not read extra records');
      if (count === records.length) readable.close();
    })
    .on('end', function() {
      assert.equal(count, 20, 'read 20 records');
      assert.end();
    })
    .on('error', function(err) {
      assert.ifError(err, 'should not error');
    });

  setTimeout(function() {
    kinesis.putRecords({
      StreamName: testStreamName,
      Records: records
    }, function(err) {
      if (err) throw err;
    });
  }, 500);
});

test('reads latest records', function(assert) {
  var initialRecords = [];
  var subsequentRecords = [];
  for (var i = 0; i < 20; i++) {
    initialRecords.push({
      Data: crypto.randomBytes(10),
      PartitionKey: 'key'
    });
    subsequentRecords.push({
      Data: crypto.randomBytes(10),
      PartitionKey: 'key'
    });
  }

  function readRecords() {
    var readable = require('..')(kinesis, testStreamName, { iterator: 'LATEST' });
    var count = 0;

    readable
      .on('data', function(recordSet) {
        recordSet.forEach(function(record, i) {
          var expected = subsequentRecords[i].Data.toString('hex');
          assert.equal(record.Data.toString('hex'), expected, 'anticipated data');
        });

        count += recordSet.length;
        if (count > 20) assert.fail('should not read extra records');
        if (count === 20) readable.close();
      })
      .on('end', function() {
        assert.equal(count, 20, 'read 20 records');
        assert.end();
      })
      .on('error', function(err) {
        assert.ifError(err, 'should not error');
      });

    setTimeout(function() {
      kinesis.putRecords({
        StreamName: testStreamName,
        Records: subsequentRecords
      }, function(err) {
        if (err) throw err;
      });
    }, 500);
  }

  kinesis.putRecords({
    StreamName: testStreamName,
    Records: initialRecords
  }, function(err) {
    if (err) throw err;
    readRecords();
  });
});

test('emits checkpoints, obeys limits', function(assert) {
  var records = [];
  for (var i = 0; i < 20; i++) records.push({
    Data: crypto.randomBytes(10),
    PartitionKey: 'key'
  });

  var readable = require('..')(kinesis, testStreamName, { limit: 1 });
  var count = 0;
  var checkpoints = 0;

  readable
    .on('data', function(recordSet) {
      assert.equal(recordSet.length, 1, 'obeys requested limit');
      count += recordSet.length;
      if (count > records.length) assert.fail('should not read extra records');
      if (count === records.length) readable.close();
    })
    .on('checkpoint', function(sequenceNum) {
      if (typeof sequenceNum !== 'string') assert.fail('invalid sequenceNum emitted');
      checkpoints++;
    })
    .on('end', function() {
      assert.equal(checkpoints, 20, 'emits on each read');
      assert.end();
    })
    .on('error', function(err) {
      assert.ifError(err, 'should not error');
    });

  kinesis.putRecords({
    StreamName: testStreamName,
    Records: records
  }, function(err) {
    if (err) throw err;
  });
});

test('reads after checkpoint', function(assert) {
  var records = [];
  for (var i = 0; i < 15; i++) records.push({
    Data: crypto.randomBytes(10),
    PartitionKey: 'key'
  });

  kinesis.putRecords({
    StreamName: testStreamName,
    Records: records
  }, function(err, resp) {
    if (err) throw err;
    readRecords(resp.Records[9].SequenceNumber);
  });

  function readRecords(startAfter) {
    var readable = require('..')(kinesis, testStreamName, { limit: 1, startAfter: startAfter });
    var count = 10; // should start at the 10th record and read 5 more

    readable
      .on('data', function(recordSet) {
        var record = recordSet[0];
        var expected = records[count].Data.toString('hex');
        assert.equal(record.Data.toString('hex'), expected, 'anticipated data');
        count += recordSet.length;
        if (count > records.length) assert.fail('should not read extra records');
        if (count === records.length) readable.close();
      })
      .on('end', function() {
        assert.end();
      })
      .on('error', function(err) {
        assert.ifError(err, 'should not error');
      });
  }
});

test('reads from checkpoint', function(assert) {
  var records = [];
  for (var i = 0; i < 15; i++) records.push({
    Data: crypto.randomBytes(10),
    PartitionKey: 'key'
  });

  kinesis.putRecords({
    StreamName: testStreamName,
    Records: records
  }, function(err, resp) {
    if (err) throw err;
    readRecords(resp.Records[9].SequenceNumber);
  });

  function readRecords(startAt) {
    var readable = require('..')(kinesis, testStreamName, { limit: 1, startAt: startAt });
    var count = 9; // should start at the 9th record and read 6 more

    readable
      .on('data', function(recordSet) {
        var record = recordSet[0];
        var expected = records[count].Data.toString('hex');
        assert.equal(record.Data.toString('hex'), expected, 'anticipated data');
        count += recordSet.length;
        if (count > records.length) assert.fail('should not read extra records');
        if (count === records.length) readable.close();
      })
      .on('end', function() {
        assert.end();
      })
      .on('error', function(err) {
        assert.ifError(err, 'should not error');
      });
  }
});

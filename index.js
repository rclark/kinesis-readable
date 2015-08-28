var AWS = require('aws-sdk');
var stream = require('stream');
var util = require('util');

// required config:
// - name
// - region
//
// optional config:
// - endpoint
// - accessKeyId
// - secretAccesKey
// - sessionToken

module.exports = function(config) {
  var kinesis = new AWS.Kinesis({
    endpoint: config.endpoint,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    sessionToken: config.sessionToken,
    region: config.region
  });

  util.inherits(KinesisReadable, stream.Readable);

  // Optional:
  // - shardId: to read from
  // - latest: to only read records written after you start listening
  // - lastCheckpoint: start reading the record after the given sequence number
  // - limit: max number of records that will be passed to `data` event
  function KinesisReadable(options) {
    options = options || {};

    this.name = config.name;
    this.shardId = options.shardId;
    this.kinesis = kinesis;
    this.iterator = null;
    this.pending = 0;
    this.latest = !!options.latest;
    this.trimHorizon = !!options.trimHorizon;
    this.lastCheckpoint = options.lastCheckpoint;
    this.limit = Math.min(10000, options.limit) || 1;

    stream.Readable.call(this, { objectMode: true });
  }

  KinesisReadable.prototype.close = function(callback) {
    callback = callback || function() {};
    var _this = this;

    _this.drain = true;
    if (_this.pending) return setImmediate(_this.close.bind(_this), callback);

    _this.once('end', callback);

    _this.push(null);
    _this.resume();
  };

  KinesisReadable.prototype._read = function read() {
    var _this = this;
    if (_this.drain) return;
    if (!_this.iterator) return _this._describe(haveIterator);
    haveIterator();

    function haveIterator(err) {
      if (_this.drain) return;
      if (err) return _this.emit('error', err);

      _this.pending++;
      kinesis.getRecords({
        ShardIterator: _this.iterator,
        Limit: _this.limit
      }, function(err, data) {
        _this.pending--;
        if (err) return _this.emit('error', err);

        _this.iterator = data.NextShardIterator;

        if (!data.Records.length) return setImmediate(function() {
          _this._read();
        });

        _this.push(data.Records);
        _this.emit('checkpoint', data.Records.slice(-1)[0].SequenceNumber);
      });
    }
  };

  KinesisReadable.prototype._describe = function describe(callback) {
    var _this = this;
    if (_this.drain) return callback();

    _this.kinesis.describeStream({
      StreamName: _this.name
    }, function(err, data) {
      if (err) return callback(err);
      if (_this.drain) return callback();

      if (typeof _this.shardId === 'string') {
        var match = data.StreamDescription.Shards.filter(function(shard) {
          return shard.ShardId === _this.shardId;
        });
        if (match.length === 0)
          return callback(new Error('Shard ' + _this.shardId + ' does not exist'));
      } else {
        _this.shardId = data.StreamDescription.Shards[0].ShardId;
      }

      var params = {
        StreamName: _this.name,
        ShardId: _this.shardId,
        ShardIteratorType: 'AT_SEQUENCE_NUMBER',
        StartingSequenceNumber: data.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber
      };

      if (_this.trimHorizon) {
        params.ShardIteratorType = 'TRIM_HORIZON';
        delete params.StartingSequenceNumber;
      }

      if (_this.latest) {
        params.ShardIteratorType = 'LATEST';
        delete params.StartingSequenceNumber;
        _this.latest = false;
      }

      if (_this.lastCheckpoint) {
        params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
        params.StartingSequenceNumber = _this.lastCheckpoint;
        _this.lastCheckpoint = false;
      }

      kinesis.getShardIterator(params, function(err, data) {
        if (err) return callback(err);
        if (_this.drain) return callback();
        _this.iterator = data.ShardIterator;
        callback();
      });
    });
  };

  return KinesisReadable;
};

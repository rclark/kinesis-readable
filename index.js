var AWS = require('aws-sdk');
var stream = require('stream');
var util = require('util');

// required config:
// - streamName
// - streamRegion
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
    region: config.streamRegion
  });

  util.inherits(KinesisReadable, stream.Readable);
  function KinesisReadable(shardId) {
    this.streamName = config.streamName;
    this.shardId = shardId;
    this.kinesis = kinesis;
    this.iterator = null;
    this.pending = 0;
    stream.Readable.call(this, { objectMode: true });
  }

  KinesisReadable.prototype.close = function(callback) {
    callback = callback || function() {};
    this.drain = true;
    if (this.pending) return setImmediate(this.close.bind(this), callback);
    this.on('end', callback);
    this.push(null);
    this.resume();
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
        Limit: 1
      }, function(err, data) {
        _this.pending--;
        if (err) return _this.emit('error', err);

        _this.iterator = data.NextShardIterator;

        if (!data.Records.length) setImmediate(function() {
          _this._read();
        });

        data.Records.forEach(function(record) {
          _this.push(record);
        });
      });
    }
  };

  KinesisReadable.prototype._describe = function describe(callback) {
    var _this = this;
    if (_this.drain) return callback();

    _this.kinesis.describeStream({
      StreamName: _this.streamName
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

      kinesis.getShardIterator({
        StreamName: _this.streamName,
        ShardId: _this.shardId,
        ShardIteratorType: 'AT_SEQUENCE_NUMBER',
        StartingSequenceNumber: data.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber
      }, function(err, data) {
        if (err) return callback(err);
        if (_this.drain) return callback();
        _this.iterator = data.ShardIterator;
        callback();
      });
    });
  };

  return KinesisReadable;
};

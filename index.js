var stream = require('stream');

module.exports = KinesisReadable;

/**
 * A factory to generate a {@link KinesisClient} that pulls records from a Kinesis stream
 *
 * @param {object} client - an AWS.Kinesis client capable of reading the desired stream
 * @param {string} [name] - the name of the shard to read. Not required if already
 * set by the provided AWS.Kinesis client.
 * @param {object} [options] - configuration details
 * @param {string} [options.shardId] - the shard id to read from. Each KinesisReadable
 * instance is only capable of reading a single shard. If unspecified, the instance
 * will read from the first shard returned by a DescribeStream request.
 * @param {string} [options.iterator] - the iterator type. One of `LATEST` or `TRIM_HORIZON`.
 * If unspecified, defaults to `TRIM_HORIZON`
 * @param {string} [options.startAt] - a sequence number to start reading from.
 * @param {string} [options.startAfter] - a sequence number to start reading after.
 * @param {number} [options.timestamp] - a timestamp to start reading after.
 * @param {number} [options.limit] - the maximum number of records that will
 * be passed to any single `data` event.
 * @param {number} [options.readInterval] - time in ms to wait between getRecords API calls
 * @returns {KinesisClient} a readable stream of kinesis records
 */
function KinesisReadable(client, name, options) {
  if (typeof name === 'object') {
    options = name;
    name = undefined;
  }

  if (!options) options = {};

  if (options.iterator && options.iterator !== 'LATEST' && options.iterator !== 'TRIM_HORIZON')
    throw new Error('options.iterator must be one of LATEST or TRIM_HORIZON');

  var readable = new stream.Readable({
    objectMode: true,
    highWaterMark: 100
  });

  var checkpoint = new stream.Transform({
    objectMode: true,
    highWaterMark: 100
  });

  var iterator, drain, ended, pending = 0;

  function describeStream(callback) {
    pending++;
    client.describeStream({ StreamName: name }, function(err, data) {
      pending--;
      if (err) return callback(err);

      var shardId = options.shardId ?
        data.StreamDescription.Shards.filter(function(shard) {
          return shard.ShardId === options.shardId;
        }).map(function(shard) {
          return shard.ShardId;
        })[0] : data.StreamDescription.Shards[0].ShardId;

      if (!shardId) return callback(new Error('Shard ' + options.shardId + ' does not exist'));
      getShardIterator(shardId, callback);
    });
  }

  function getShardIterator(shardId, callback) {
    var params = {
      ShardId: shardId,
      StreamName: name
    };

    if (options.iterator) {
      params.ShardIteratorType = options.iterator;
    } else if (options.startAt) {
      params.ShardIteratorType = 'AT_SEQUENCE_NUMBER';
      params.StartingSequenceNumber = options.startAt;
    } else if (options.startAfter) {
      params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
      params.StartingSequenceNumber = options.startAfter;
    } else if (options.timestamp) {
      params.ShardIteratorType = 'AT_TIMESTAMP';
      params.Timestamp = options.timestamp;
    } else {
      params.ShardIteratorType = 'TRIM_HORIZON';
    }

    pending++;
    client.getShardIterator(params, function(err, data) {
      pending--;
      if (err) return callback(err);
      iterator = data.ShardIterator;
      callback();
    });
  }

  function read(callback) {
    if (drain && !pending) return callback(null, { Records: null });
    if (drain && pending) return setImmediate(read, callback);

    pending++;
    client.getRecords({
      ShardIterator: iterator,
      Limit: options.limit
    }, function(err, data) {
      pending--;
      if (err) return callback(err);

      iterator = data.NextShardIterator;

      if (!data.Records.length) {
        if (!drain) return setTimeout(read, options.readInterval || 500, callback);
        data.Records = null;
      }

      callback(null, data);
    });
  }

  readable._read = function() {
    if (iterator) return read(gotRecords);

    describeStream(function(err) {
      if (err) return checkpoint.emit('error', err);
      read(gotRecords);
    });

    function gotRecords(err, data) {
      if (err) return checkpoint.emit('error', err);
      setTimeout(readable.push.bind(readable), options.readInterval || 500, data.Records);
    }
  };

  checkpoint._transform = function(data, enc, callback) {
    checkpoint.emit('checkpoint', data.slice(-1)[0].SequenceNumber);
    callback(null, data);
  };

  checkpoint._flush = function(callback) {
    ended = true;
    callback();
  };

  /**
   * A kinesis stream persists beyond the duration of a readable stream. In order
   * to stop reading from the stream, call `.close()`. Then listen for the `end`
   * event to indicate that all data that as been read from Kinesis has been passed
   * downstream.
   *
   * @instance
   * @memberof KinesisClient
   * @returns {KinesisClient}
   */
  checkpoint.close = function() {
    drain = true;
    if (!ended) readable._read();
    return checkpoint;
  };

  /**
   * A client that implements a node.js readable stream interface for reading kinesis
   * records. See node.js documentation for details.
   *
   * In addition to the normal events emitted by a readable stream, the KinesisClient
   * emits `checkpoint` events, which indicate the most recent sequence number that
   * has been read from Kinesis and passed downstream.
   *
   * @name KinesisClient
   */
  return readable.pipe(checkpoint);
}

# kinesis-readable

[![Build Status](https://travis-ci.org/rclark/kinesis-readable.svg?branch=master)](https://travis-ci.org/rclark/kinesis-readable)

Node.js stream interface for reading records from [AWS Kinesis](http://aws.amazon.com/kinesis/).

## Usage

```js
var config = {
  name: 'my-stream',
  region: 'my-region'
};

var Readable = require('kinesis-readable')(config);

// See below for options
var readable = new Readable(options);

readable
  // 'data' events will trigger for a set of records in the stream
  .on('data', function(records) {
    console.log(records);
  })
  .on('error', function(err) {
    console.error(err);
  })
  .on('end', function() {
    console.log('all done!');
  });

// Calling .close() will finish all pending GetRecord requests before emitting
// the 'end' event.
// Because the kinesis stream persists, the readable stream will not
// 'end' until you explicitly close it
setTimeout(function() {
  readable.close();
}, 60 * 60 * 1000);
```

## Options

You can pass options to create the readable stream, all parameters are optional:

```js
var options = {
  shardId: 'shard-identifier', // default to first shard in the stream
  latest: true, // default to false
  lastCheckpoint: '12345678901234567890', // start reading after this sequence number
  limit: 100 // default to 1
};
```

- **shardId** allows you to specify which shard in your stream to read from
- **latest** if true, begins reading records written to the stream *after* reading begins. Use this to ignore records written to the stream before you started listening.
- **lastCheckpoint** pass a sequence number and the stream will start reading from the next record
- **limit** allows you to set the maximum number of records that will be passed to any single `data` event

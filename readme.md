# kinesis-readable

[![Build Status](https://travis-ci.org/rclark/kinesis-readable.svg?branch=master)](https://travis-ci.org/rclark/kinesis-readable)

Node.js stream interface for reading records from [AWS Kinesis](http://aws.amazon.com/kinesis/).

## Usage

```js
var config = {
  streaName: 'my-stream',
  streamRegion: 'my-region'
};

var Readable = require('kinesis-readable')(config);

// Optionally, you can specify a shardId, when creating the readable stream,
// otherwise it'll just read the first shard returned in a describeStream request
var readable = new Readable();

readable
  // 'data' events will trigger for each record in the stream
  .on('data', function(record) {
    console.log(record);
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

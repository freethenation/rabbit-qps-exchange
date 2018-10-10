# Rabbit QPS Exchange

## Creates and maintains a rabbit exchange which enforces a QPS across queues

Creates and maintains a rabbit exchange, 'qps_exchange', which when published to enforces a QPS across queues using parameters provided via rabbit message headers.
Publish your messages to 'qps_exchange' instead of the default (empty) exchange supplying a 'qps-key' header and a 'qps-delay' header.
The 'qps-key' is used to serialize all messages sent to the 'qps_exchange' regardless of their routing key, which will be preserved.
The 'qps-delay' header (in milliseconds) delays after routeing the message using the default exchange. This can be used to approximate a desired QPS.
You may also optionally provide a 'qps-max-priority' to add a priority to the serialization and then use rabbit priority as normal.

Usage: `./rabbit_qps_shovel.js [OPTIONS] COMMAND`

commands:

* `help`   show help
* `init`   create exchange and required queues then exit
* `clean`  clean up idle queues then exit
* `start`  ensure exchange and required queues, clean idle queues, and then start processing messages

options:

* `--management` Rabbit management connection string. Example http://guest:guest@localhost:15672/ (REQUIRED)
* `--connection` Rabbit connection string. See https://www.rabbitmq.com/uri-spec.html (REQUIRED)

### Configuration headers

| Header              | Description                                                                                                                | Type      | Default         |
|---------------------|----------------------------------------------------------------------------------------------------------------------------|-----------|-----------------|
| qps-key             | Used to serialize all messages sent to the `qps_exchange`                                                                  | string    | (required)      |
| qps-delay           | Delay (in milliseconds) to wait after routing the message to the default exchange - used to approximate QPS                | int/float | 1000 ms         |
| qps-max-priority    | Can be set along with normal Rabbit priority headers to allow prioritization                                               | integer   | no priority     |
| qps-batch-size      | If set, will group this many messages together into a batch, all of which will be sent with no other batches between them  | integer   | 1 (no batching) |
| qps-batch-timeout   | Maximum time (in millisecond) to wait for a full batch                                                                     | int/float | 1000 ms         |
| qps-batch-combine   | If set, will combine batch messages together with ASCII newlines (`\n`, `0x0A`)                                            | boolean   | false           |
| qps-delay-lock-key  | Can be set to share the delay lock between multiple QPS keys - defaults to `qps-key` if not set                            | string    | `qps-key`       |

### Installing
* The rabbit server version must be > 3.5.0
* The rabbit server must have the management plugin installed (it's http interface is used)
* Install a modern version of node (async support is required)
* Run `npm install`

### Testing
* Run a rabbitmq server
* The rabbit server must have the management plugin installed
* Run tests with `npm test`
* The environment variables `rabbitConnString` and `managementConnString` can be used to provide connection information


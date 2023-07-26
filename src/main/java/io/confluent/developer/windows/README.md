### Exercise Descriptions

Here's a brief description of each example in this repository.  For detailed step-by-step descriptions follow the Kafka Streams
course videos.  Note that for the purposes of facilitating the learning process, each exercise uses a utility class `TopicLoader` that will create
the required topics and populate them with some sample records for the Kafka Streams application. As a result when you run each exercise, the first output you'll
see on the console is from the `Callback` interface, and it will look similar to this:
```text
Record produced - offset - 56 timestamp - 1689974156214 
Record produced - offset - 57 timestamp - 1689974156228 
Record produced - offset - 58 timestamp - 1689974156228 
Record produced - offset - 59 timestamp - 1689974156228 
Record produced - offset - 60 timestamp - 1689974156228 
Record produced - offset - 61 timestamp - 1689974156228 
Record produced - offset - 62 timestamp - 1689974156228 
```
Each exercise is incomplete, and it's up to you to follow the instructions and hints in the comments to get each application into running shape.  There's also a `solution` directory in each module that contains the fully completed example for you compare with your version or to help you if you get stuck.

#### Basic Operations

The basic operations exercise demonstrates using Kafka Streams stateless operations like `filter` and `mapValues`.
You run the basic operations example with this command ` ./gradlew runStreams -Pargs=basic` and your output on the console should resemble this:
```text
Incoming record - key order-key value orderNumber-1001
Outgoing record - key order-key value 1001
Incoming record - key order-key value orderNumber-5000
Outgoing record - key order-key value 5000
Incoming record - key order-key value orderNumber-999
Incoming record - key order-key value orderNumber-3330
Outgoing record - key order-key value 3330
Incoming record - key order-key value bogus-1
Incoming record - key order-key value bogus-2
Incoming record - key order-key value orderNumber-8400
Outgoing record - key order-key value 8400
```
Take note that it's expected to not have a corresponding output record for each input record due to the filters applied by the Kafka Steams application.

#### KTable
It's recommended to watch the [KTable lecture](https://developer.confluent.io/learn-kafka/kafka-streams/ktable/) and the [Hands On: KTable](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-ktable/) videos first.

This exercise is a gentle introduction to the Kafka Streams `KTable` abstraction.  This example uses the same topology as the `basic` example, but your expected output
is different due to fact that a `KTable` is an update-stream, and records with the same key are considered updates to previous records.  The default behavior
of a `KTable` then is to emit only the latest update per key.

You run the `KTable`  example with this command ` ./gradlew runStreams -Pargs=ktable` and your output on the console should resemble this:
```text
Outgoing record - key order-key value 8400
```
The sample data for this exercise has the same key, so your output for this exercise contains only one record.

NOTE: Since the default behavior for materialized `KTable`s is to emit changes on commit or when the cache is full, you'll need
to let this application run for roughly 40 seconds to see a result.

#### Joins

It's recommended to watch the [Joins lecture](https://developer.confluent.io/learn-kafka/kafka-streams/joins/) and the [Hands On: Joins](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-joins/) videos first.

The Joins exercise creates a join between two `KStream` objects resulting in a new `KStream` which is
further joined against a `KTable`.  You'll see the input records for the two `KStream`s , the results of the
Stream-Stream join, and the final Stream-Table join results.

You run the joins example with this command ` ./gradlew runStreams -Pargs=joins` and the output for the exercise should like this:
```text
Appliance stream incoming record key 10261998 value {"order_id": "remodel-1", "appliance_id": "dishwasher-1333", "user_id": "10261998", "time": 1689975652482}
Appliance stream incoming record key 10261999 value {"order_id": "remodel-2", "appliance_id": "stove-2333", "user_id": "10261999", "time": 1689975652482}
Electronic stream incoming record 10261998 value {"order_id": "remodel-1", "electronic_id": "television-2333", "user_id": "10261998", "price": 0.0, "time": 1689975652484}
Stream-Stream Join record key 10261998 value {"electronic_order_id": "remodel-1", "appliance_order_id": "remodel-1", "appliance_id": "dishwasher-1333", "user_name": "", "time": 1689975654383}
Stream-Table Join record key 10261998 value {"electronic_order_id": "remodel-1", "appliance_order_id": "remodel-1", "appliance_id": "dishwasher-1333", "user_name": "Elizabeth Jones", "time": 1689975654383}
Electronic stream incoming record 10261999 value {"order_id": "remodel-2", "electronic_id": "laptop-5333", "user_id": "10261999", "price": 0.0, "time": 1689975652493}
Stream-Stream Join record key 10261999 value {"electronic_order_id": "remodel-2", "appliance_order_id": "remodel-2", "appliance_id": "stove-2333", "user_name": "", "time": 1689975654415}
Stream-Table Join record key 10261999 value {"electronic_order_id": "remodel-2", "appliance_order_id": "remodel-2", "appliance_id": "stove-2333", "user_name": "Art Vandelay", "time": 1689975654415}
```

#### Aggregation

It's recommended to watch the [Stateful operations](https://developer.confluent.io/learn-kafka/kafka-streams/stateful-operations/) and the [Hands On: Aggregations](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-aggregations/) videos first.

This exercise demonstrates an aggregation of a simulated stream of electronic purchase.
To run the aggregation example use this command ` ./gradlew runStreams -Pargs=aggregate`
You'll see the incoming records on the console along with the aggregation results:

```text
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1689975700091}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1689975710091}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1689975720091}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689975732091}
Outgoing record - key HDTV-2333 value 19666.42
```
NOTE that you'll need to let the streams application run for ~40 seconds to see the aggregation result

#### Windowing

It's recommended to watch the [Windowing](https://developer.confluent.io/learn-kafka/kafka-streams/windowing/) and the [Hands On: Windowing](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-windowing/) videos before attempting the exercises.

This exercise uses builds on top of the aggregation exercise, but adds windowing to it.

To run the windowing exercise execute this command ` ./gradlew runStreams -Pargs=windows`
You'll use slightly different input records, and your output should look something like this:
```text
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1689975757580}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1689976657580}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1689977557580}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689978457580}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689979537580}
Incoming record - key SUPER-WIDE-TV-2333 value {"order_id": "instore-1", "electronic_id": "SUPER-WIDE-TV-2333", "user_id": "1038884844", "price": 5333.98, "time": 1689979537580}
Incoming record - key SUPER-WIDE-TV-2333 value {"order_id": "instore-1", "electronic_id": "SUPER-WIDE-TV-2333", "user_id": "1038884844", "price": 4333.98, "time": 1689982237580}
Outgoing record - key HDTV-2333 value 21668.359999999997
Outgoing record - key HDTV-2333 value 7167.959999999999
Outgoing record - key SUPER-WIDE-TV-2333 value 14001.939999999999
```
Three things to note about this example:
1. The timestamps on the record are simulated to emit windowed results so what you'll see is approximated
2. This application uses the default timestamp extractor [FailOnInvalidTimestamp](https://kafka.apache.org/10/documentation/streams/developer-guide/config-streams.html#timestamp-extractor)
3. You need to let the application run for ~40 seconds to see the windowed aggregated output

#### Time Concepts

It's recommended to watch the [Time Concepts](https://developer.confluent.io/learn-kafka/kafka-streams/time-concepts/) and the [Hands On: Time Concepts](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-time-concepts/) videos before moving on to the exercises.

The time concepts exercise uses an aggregation with windowing.  However, this example uses a custom
`TimestampExtractor` to use timestamps embedded in the record itself (event time) to drive the behavior of Kafka Steams
application.

To run this example execute ` ./gradlew runStreams -Pargs=time`.

Your output will include statements from the `TimestampExtractor` and it should look
something like this:
```text
Extracting time of 1689975867935 from {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1689975867935}
Extracting time of 1689976767935 from {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1689976767935}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1689975867935}
Extracting time of 1689977667935 from {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1689977667935}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1689976767935}
Extracting time of 1689978567935 from {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689978567935}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1689977667935}
Extracting time of 1689979647935 from {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689979647935}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689978567935}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689979647935}
Outgoing record - key HDTV-2333 value 3999.23
Outgoing record - key HDTV-2333 value 7167.959999999999
```

Two things to note about this example:
1. The timestamps on the record are simulated to emit windowed results so what you'll see is approximated
2. You need to let the application run for ~40 seconds to see the windowed aggregated output

#### Processor API

It's recommended to watch the [Processor API](https://developer.confluent.io/learn-kafka/kafka-streams/processor-api/) and [Hands On: Processor API](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-processor-api/) videos before moving on to the exercises.

This exercise covers working with the Processor API.  The application creates an aggregation but uses a punctuation every 30 seconds
(stream-time) to emit records.

You run this example with the command: ` ./gradlew runStreams -Pargs=processor` and the results should look like this:
```text
Punctuation forwarded record - key HDTV-2333 value 9833.21
Processed incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1689975937714}
Punctuation forwarded record - key HDTV-2333 value 11833.21
Processed incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1689975972714}
Punctuation forwarded record - key HDTV-2333 value 13832.439999999999
Processed incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1689976007714}
Punctuation forwarded record - key HDTV-2333 value 18332.44
Processed incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1689976042714}
Punctuation forwarded record - key HDTV-2333 value 19666.42
```
Note that for this example the timestamps have been modified to advance stream-time by 30 seconds for each incoming record.
The output here does not reflect what you would see on a production system.

#### Error Handling

It's recommended to watch the [Error Handling](https://developer.confluent.io/learn-kafka/kafka-streams/error-handling/) and [Hands On: Error Handling](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-error-handling/) videos before attempting the exercises.

The error handling exercise injects a simulated transient error. The Kafka Streams `StreamsUncaughtExceptionHandler`
examines the exception and returns a `StreamThreadExceptionResponse.REPLACE_THREAD` response that allows the application
to resume processing after the error.

Use this command, ` ./gradlew runStreams -Pargs=errors`, to run the errors example.

When the application runs you'll see a stacktrace then in a few seconds the application will recover and continue running:
```text
Incoming record - key order-key value orderNumber-1001
Exception in thread "streams-error-handling-f589722e-89f3-4304-a38e-77a9b9ad5166-StreamThread-1" org.apache.kafka.streams.errors.StreamsException: Exception caught in process. taskId=0_4, processor=KSTREAM-SOURCE-0000000000, topic=streams-error-input, partition=4, offset=0, stacktrace=java.lang.IllegalStateException: Retryable transient error
...(full stacktrace not shown here for clarity)
Incoming record - key order-key value orderNumber-1001
Outgoing record - key order-key value 1001
Incoming record - key order-key value orderNumber-5000
Outgoing record - key order-key value 5000
Incoming record - key order-key value orderNumber-999
Incoming record - key order-key value orderNumber-3330
Outgoing record - key order-key value 3330
Incoming record - key order-key value bogus-1
Incoming record - key order-key value bogus-2
Incoming record - key order-key value orderNumber-8400
Outgoing record - key order-key value 8400
```

#### Testing

It's recommended to watch the [Testing](https://developer.confluent.io/learn-kafka/kafka-streams/testing/) and the [Hands On: Testing](https://developer.confluent.io/learn-kafka/kafka-streams/hands-on-testing/) videos before attempting the exercises.

To run the unit test with the `TopologyTestDriver` you can either execute `./gradlew test` from the root of the project
or run the `io.confluent.developer.aggregate.StreamsAggregateTest` from a test runner in your IDE.
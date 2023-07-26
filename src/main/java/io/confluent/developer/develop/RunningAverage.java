package io.confluent.developer.develop;

import io.confluent.common.utils.TestUtils;
import io.confluent.demo.CountAndSum;
import io.confluent.demo.Rating;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Optional.ofNullable;
import static org.apache.kafka.common.serialization.Serdes.Double;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.streams.kstream.Grouped.with;

public class RunningAverage {

    private void run() throws IOException {

        final Properties allProps = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/streams.properties")) {
            allProps.load(inputStream);
        }
        allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("running-average.develop.application.id"));
        allProps.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        allProps.put("running-average.input.ratings.topic.name", allProps.getProperty("running-average.input.ratings.topic.name"));
        allProps.put("running-average.output.rating-averages.topic.name", allProps.getProperty("running-average.output.rating-averages.topic.name"));

        TopicLoader.runProducer();

        Topology topology = this.buildTopology(new StreamsBuilder(), allProps);

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    protected static KTable<Long, Double> getRatingAverageTable(KStream<Long, Rating> ratings,
                                                                String avgRatingsTopicName,
                                                                SpecificAvroSerde<CountAndSum> countAndSumSerde) {

        // Grouping Ratings
        KGroupedStream<Long, Double> ratingsById = ratings
                .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating.getRating()))
                .groupByKey(with(Long(), Double()));

        final KTable<Long, CountAndSum> ratingCountAndSum =
                ratingsById.aggregate(() -> new CountAndSum(0L, 0.0),
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value);
                            return aggregate;
                        },
                        Materialized.with(Long(), countAndSumSerde));

        //ratingCountAndSum.toStream().peek((key, value) -> System.out.println("Outgoing sum record - key " + key + " value " + value));

        final KTable<Long, Double> ratingAverage =
                ratingCountAndSum.mapValues(value -> value.getSum() / value.getCount(),
                        Materialized.as("average-ratings"));

        // persist the result in topic
        ratingAverage
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing Average record - key " + key + " value " + value))
                .to(avgRatingsTopicName);
        return ratingAverage;
    }

    //region buildTopology
    private Topology buildTopology(StreamsBuilder builder,
                                   Properties envProps) {


        final String ratingTopicName = envProps.getProperty("running-average.input.ratings.topic.name");
        final String avgRatingsTopicName = envProps.getProperty("running-average.output.rating-averages.topic.name");
        KStream<Long, Rating> ratingStream = builder.stream(ratingTopicName,
                Consumed.with(Serdes.Long(), getRatingSerde(envProps)))
                .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        getRatingAverageTable(ratingStream, avgRatingsTopicName, getCountAndSumSerde(envProps));

        // finish the topology
        return builder.build();
    }
    //endregion

    public static SpecificAvroSerde<CountAndSum> getCountAndSumSerde(Properties envProps) {
        SpecificAvroSerde<CountAndSum> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(envProps), false);
        return serde;
    }

    public static SpecificAvroSerde<Rating> getRatingSerde(Properties envProps) {
        SpecificAvroSerde<Rating> serde = new SpecificAvroSerde<>();
        serde.configure(getSerdeConfig(envProps), false);
        return serde;
    }

    protected static Map<String, String> getSerdeConfig(Properties config) {
        final HashMap<String, String> map = new HashMap<>();

        final String srUrlConfig = config.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        map.put(SCHEMA_REGISTRY_URL_CONFIG, ofNullable(srUrlConfig).orElse(""));
        return map;
    }

    public static void main(String[] args) throws IOException {
        new RunningAverage().run();
    }
}
package io.confluent.developer.list;

import io.confluent.common.utils.TestUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.RollupList;
import io.confluent.developer.logingroup.TopicLoader;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class StreamsGroupList {

    public static Topology buildTopology(Properties allProps,
                                         final SpecificAvroSerde<ElectronicOrder> electronicOrderSpecificAvroSerde) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String inputTopic = allProps.getProperty("develop.input.topic");
        final String outputTopic = allProps.getProperty("develop.output.topic");

        final KStream<String, ElectronicOrder> electronicStream=
                builder.stream(inputTopic, Consumed.with(Serdes.String(), electronicOrderSpecificAvroSerde));

        electronicStream.peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        final Serde<RollupList> rollupListSerde = getSpecificAvroSerde(allProps);

        final Aggregator<String, ElectronicOrder, RollupList> aggregatorList = new AggregatorList();

        final KGroupedStream<String, ElectronicOrder> appOneGrouped = electronicStream.groupByKey();

        KStream<String,RollupList> rollupListKStream=
        appOneGrouped.cogroup(aggregatorList)
                .aggregate(() -> new RollupList(new ArrayList<>()),
                        Materialized.with(Serdes.String(), rollupListSerde))
                .toStream();

        rollupListKStream.peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value));
                //.to(outputTopic, Produced.with(Serdes.String(), rollupListSerde));


        return builder.build();
    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Properties allProps) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure((Map)allProps, false);
        return specificAvroSerde;
    }

    private static SpecificAvroSerde<ElectronicOrder> electronicSerde(final Properties allProps) {
        final SpecificAvroSerde<ElectronicOrder> serde = new SpecificAvroSerde<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", allProps.getProperty("schema.registry.url"));
        serde.configure(config, false);
        return serde;
    }

    public static void main(String[] args) throws IOException {

        final Properties allProps = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/streams.properties")) {
            allProps.load(inputStream);
        }
        allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("develop.list.application.id"));
        allProps.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        allProps.put("develop.list.input.topic", allProps.getProperty("develop.list.input.topic"));
        allProps.put("develop.list.output.topic", allProps.getProperty("develop.list.output.topic"));

        TopicLoader.runProducer();

        Topology topology = buildTopology(allProps, electronicSerde(allProps));

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, allProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }

}
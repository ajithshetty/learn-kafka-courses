package io.confluent.developer.tickets;

import io.confluent.common.utils.TestUtils;
import io.confluent.developer.avro.TicketSale;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TicketsStreams {

        public static Topology buildTopology(Properties allProps,
        final SpecificAvroSerde<TicketSale> ticketSaleSpecificAvroSerde) {
            final StreamsBuilder builder = new StreamsBuilder();

            final String inputTopic = allProps.getProperty("tickets.input.topic");
            final String outputTopic = allProps.getProperty("tickets.output.topic");

            final KStream<String, TicketSale> ticketSaleKStream=
                    builder.stream(inputTopic, Consumed.with(Serdes.String(), ticketSaleSpecificAvroSerde));

            ticketSaleKStream.peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

            ticketSaleKStream
                    .map((k, v) -> new KeyValue<>(v.getTitle(), v.getTicketTotalValue()))
                    // Group by title
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                    // Apply COUNT method
                    .count()
                    // Write to stream specified by outputTopic
                    .toStream()
                    .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                    .mapValues(v -> v.toString() + " tickets sold").to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            return builder.build();
        }

        private static SpecificAvroSerde<TicketSale> ticketsSerde(final Properties allProps) {
            final SpecificAvroSerde<TicketSale> serde = new SpecificAvroSerde<>();
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
            allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("tickets.application.id"));
            allProps.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
            allProps.put("tickets.input.topic", allProps.getProperty("tickets.input.topic"));
            allProps.put("tickets.output.topic", allProps.getProperty("tickets.output.topic"));

            System.out.println("here..........");
            TopicLoader.runProducer();

            Topology topology = buildTopology(allProps, ticketsSerde(allProps));

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

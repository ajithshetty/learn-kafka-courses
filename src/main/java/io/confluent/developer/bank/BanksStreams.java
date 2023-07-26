package io.confluent.developer.bank;

import io.confluent.common.utils.TestUtils;
import io.confluent.demo.CountAndSum;
import io.confluent.developer.avro.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.common.serialization.Serdes.Double;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.streams.kstream.Grouped.with;

public class BanksStreams {

    public static Topology buildTopology(Properties allProps,
                                         final SpecificAvroSerde<Bank> bankSpecificAvroSerde){

        final StreamsBuilder builder=new StreamsBuilder();

        final String inputTopic = allProps.getProperty("bank.input.topic");
        final String outputTopic = allProps.getProperty("bank.output.topic");
        //final String rejectedTopic = allProps.getProperty("bank.rejected.topic");

        final KStream<Long,Bank> bankKStream=builder.stream(inputTopic, Consumed.with(Serdes.Long(),bankSpecificAvroSerde));
        bankKStream.peek((key, value) ->System.out.println("Incoming record - key " + key + " value " + value));


       KStream<Long, Bank> bankBalancesStream=bankKStream
                .filter(
                (key, value) -> {
                        if(value.getBalance()<=value.getWithdraw()){
                            value.setBankTransactionState(state.REJECTED);
                        }else{
                            value.setBalance(value.getBalance()-value.getWithdraw());
                            value.setBankTransactionState(state.APPROVED);
                        }
                    return true;
                });

        bankBalancesStream.peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value));

        bankBalancesStream
                .filter(((key, value) -> value.getBankTransactionState()== state.APPROVED))
                .peek((key, value) -> System.out.println("Approved record - key " + key + " value " + value))
                .to(outputTopic);

        bankBalancesStream
                .filter(((key, value) -> value.getBankTransactionState()== state.REJECTED))
                .peek((key, value) -> System.out.println("Rejected record - key " + key + " value " + value));
                //.to(outputTopic);

        return builder.build();
    }

    public static SpecificAvroSerde<CountAndSum> getCountAndSumSerde(Properties allProps) {
        SpecificAvroSerde<CountAndSum> serde = new SpecificAvroSerde<>();
        Map<String,String> config= new HashMap<>();
        config.put("schema.registry.url",allProps.getProperty("schema.registry.url"));
        serde.configure(config,false);
        return serde;
    }

    private static SpecificAvroSerde<Bank> bankSerde(final Properties allProps){
        final SpecificAvroSerde<Bank> serde= new SpecificAvroSerde<>();
        Map<String,String> config= new HashMap<>();
        config.put("schema.registry.url",allProps.getProperty("schema.registry.url"));
        serde.configure(config,false);
        return serde;
    }

    public static void main(String[] args) throws IOException {

        final Properties allProps = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/streams.properties")) {
            allProps.load(inputStream);
        }
        allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("bank.application.id"));
        allProps.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        allProps.put("bank.input.topic", allProps.getProperty("bank.input.topic"));
        allProps.put("bank.output.topic", allProps.getProperty("bank.output.topic"));

        TopicLoader.runProducer();

        Topology topology = buildTopology(allProps, bankSerde(allProps));

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


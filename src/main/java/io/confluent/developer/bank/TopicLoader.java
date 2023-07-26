package io.confluent.developer.bank;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.Bank;
import io.confluent.developer.avro.state;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    public static void runProducer() throws IOException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try(Admin adminClient = Admin.create(properties);
            Producer<Long, Bank> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("bank.input.topic");
            final String outputTopic = properties.getProperty("bank.output.topic");
            List <org.apache.kafka.clients.admin.NewTopic> topics = Arrays.asList(
                    StreamsUtils.createTopic(inputTopic),
                    StreamsUtils.createTopic(outputTopic));
            //adminClient.deleteTopics(Arrays.asList(inputTopic, outputTopic));
            adminClient.createTopics(topics);


            Callback callback = StreamsUtils.callback();

            Instant instant = Instant.now();

            List<Bank> data = List.of(
                    Bank.newBuilder()
                            .setAccountId(1001)
                            .setTime(instant.toEpochMilli())
                            .setBalance(1000)
                            .setWithdraw(100)
                            .setBankTransactionState(state.CREATED)
                            .build(),
                    Bank.newBuilder()
                            .setAccountId(1002)
                            .setTime(instant.plusSeconds(10L).toEpochMilli())
                            .setBalance(1000)
                            .setWithdraw(200)
                            .setBankTransactionState(state.CREATED)
                            .build(),
                    Bank.newBuilder()
                            .setAccountId(1003)
                            .setTime(instant.plusSeconds(20L).toEpochMilli())
                            .setBalance(300)
                            .setWithdraw(300)
                            .setBankTransactionState(state.CREATED)
                            .build(),
                    Bank.newBuilder()
                            .setAccountId(1004)
                            .setTime(instant.plusSeconds(30L).toEpochMilli())
                            .setBalance(1000)
                            .setWithdraw(800)
                            .setBankTransactionState(state.CREATED)
                            .build(),
                    Bank.newBuilder()
                            .setAccountId(1005)
                            .setTime(instant.plusSeconds(40L).toEpochMilli())
                            .setBalance(1000)
                            .setWithdraw(500)
                            .setBankTransactionState(state.CREATED)
                            .build(),
                    Bank.newBuilder()
                            .setAccountId(1006)
                            .setTime(instant.plusSeconds(50L).toEpochMilli())
                            .setBalance(2000)
                            .setWithdraw(500)
                            .setBankTransactionState(state.CREATED)
                            .build()
            );

            data.forEach((bankTransaction -> {
                ProducerRecord<Long, Bank> producerRecord = new ProducerRecord<>(
                        inputTopic,
                        0,
                        bankTransaction.getTime(),
                        bankTransaction.getAccountId(),
                        bankTransaction);
                producer.send(producerRecord, callback);
            }));



        }
    }
}

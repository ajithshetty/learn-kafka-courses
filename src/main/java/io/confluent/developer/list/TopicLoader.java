package io.confluent.developer.list;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

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
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try(Admin adminClient = Admin.create(properties);
            Producer<String, ElectronicOrder> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("develop.list.input.topic");
            final String outputTopic = properties.getProperty("develop.list.output.topic");
            List <org.apache.kafka.clients.admin.NewTopic> topics = Arrays.asList(
                    StreamsUtils.createTopic(inputTopic),
                    StreamsUtils.createTopic(outputTopic));
            adminClient.deleteTopics(Arrays.asList(inputTopic, outputTopic));
            adminClient.createTopics(topics);


            Callback callback = StreamsUtils.callback();

            Instant instant = Instant.now();

            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("10261998")
                    .setPrice(2000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(10L);

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1033737373")
                    .setPrice(1000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(10L);

            ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1026333")
                    .setPrice(3000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(12L);

            ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(5000.00)
                    .setTime(instant.toEpochMilli()).build();


            List <ElectronicOrder> electronicOrders = Arrays.asList(
                    electronicOrderOne,
                    electronicOrderTwo,
                    electronicOrderThree,
                    electronicOrderFour );

            electronicOrders.forEach((electronicOrder -> {
                ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord<>(
                        inputTopic,
                        0,
                        electronicOrder.getTime(),
                        electronicOrder.getElectronicId(),
                        electronicOrder);
                producer.send(producerRecord, callback);
            }));



        }
    }
}

package io.confluent.developer.windows;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
            final String inputTopic = properties.getProperty("windowed.input.topic");
            final String outputTopic = properties.getProperty("windowed.output.topic");
            List<NewTopic> topics = Arrays.asList(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            adminClient.createTopics(topics);

            Callback callback = StreamsUtils.callback();

            Instant instant = Instant.now();

            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("10261998")
                    .setPrice(2000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plus(15L, ChronoUnit.MINUTES);

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1033737373")
                    .setPrice(1999.23)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plus(15L, ChronoUnit.MINUTES);

            ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1026333")
                    .setPrice(4500.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plus(15L, ChronoUnit.MINUTES);

            ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(1333.98)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plus(18L, ChronoUnit.MINUTES);

            ElectronicOrder electronicOrderFive = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(1333.98)
                    .setTime(instant.toEpochMilli()).build();


            ElectronicOrder electronicOrderSix = ElectronicOrder.newBuilder()
                    .setElectronicId("SUPER-WIDE-TV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(5333.98)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plus(45L, ChronoUnit.MINUTES);

            ElectronicOrder electronicOrderSeven = ElectronicOrder.newBuilder()
                    .setElectronicId("SUPER-WIDE-TV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(4333.98)
                    .setTime(instant.toEpochMilli()).build();
            List<ElectronicOrder> electronicOrders = Arrays.asList(electronicOrderOne, electronicOrderTwo, electronicOrderThree, electronicOrderFour, electronicOrderFive, electronicOrderSix, electronicOrderSeven);

            electronicOrders.forEach((electronicOrder -> {
                ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord<>(inputTopic,
                        0,
                        electronicOrder.getTime(),
                        electronicOrder.getElectronicId(),
                        electronicOrder);
                producer.send(producerRecord, callback);
            }));



        }
    }
}

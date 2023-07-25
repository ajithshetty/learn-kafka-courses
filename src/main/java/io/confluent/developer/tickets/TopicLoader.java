package io.confluent.developer.tickets;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.TicketSale;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
            Producer<String, TicketSale> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("tickets.input.topic");
            final String outputTopic = properties.getProperty("tickets.output.topic");
            System.out.println("inputTopic:"+inputTopic);
            List <org.apache.kafka.clients.admin.NewTopic> topics = Arrays.asList(
                    StreamsUtils.createTopic(inputTopic),
                    StreamsUtils.createTopic(outputTopic));
            //adminClient.deleteTopics(Arrays.asList(inputTopic, outputTopic));
            adminClient.createTopics(topics);


            Callback callback = StreamsUtils.callback();

            TicketSale ticketSale1 = TicketSale.newBuilder()
                    .setTitle("Die Hard")
                    .setTicketTotalValue(12)
                    .setSaleTs("2019-07-18T10:00:00Z").build();


            TicketSale ticketSale2 = TicketSale.newBuilder()
                    .setTitle("Die Hard")
                    .setTicketTotalValue(12)
                    .setSaleTs("2019-07-18T10:01:00Z").build();

            TicketSale ticketSale3 = TicketSale.newBuilder()
                    .setTitle("Die Hard")
                    .setTicketTotalValue(24)
                    .setSaleTs("2019-07-18T10:01:36Z").build();

            TicketSale ticketSale4 = TicketSale.newBuilder()
                    .setTitle("The Godfather")
                    .setTicketTotalValue(12)
                    .setSaleTs("2019-07-18T10:01:31Z").build();

            TicketSale ticketSale5 = TicketSale.newBuilder()
                    .setTitle("The Godfather")
                    .setTicketTotalValue(18)
                    .setSaleTs("2019-07-18T10:02:00Z").build();

            TicketSale ticketSale6 = TicketSale.newBuilder()
                    .setTitle("The Godfather")
                    .setTicketTotalValue(36)
                    .setSaleTs("2019-07-18T11:40:00Z").build();

            TicketSale ticketSale7 = TicketSale.newBuilder()
                    .setTitle("The Godfather")
                    .setTicketTotalValue(18)
                    .setSaleTs("2019-07-18T11:40:09Z").build();

            TicketSale ticketSale8 = TicketSale.newBuilder()
                    .setTitle("The Big Lebowski")
                    .setTicketTotalValue(12)
                    .setSaleTs("2019-07-18T11:03:21Z").build();

            TicketSale ticketSale9 = TicketSale.newBuilder()
                    .setTitle("The Big Lebowski")
                    .setTicketTotalValue(12)
                    .setSaleTs("2019-07-18T11:03:50Z").build();

            List <TicketSale> electronicOrders = Arrays.asList(
                    ticketSale1,
                    ticketSale2,
                    ticketSale3,
                    ticketSale4,
                    ticketSale5,
                    ticketSale6,
                    ticketSale7,
                    ticketSale8,
                    ticketSale9);

            Instant instant = Instant.now();
            electronicOrders.forEach((ticketSale -> {
                ProducerRecord<String, TicketSale> producerRecord = new ProducerRecord<>(
                        inputTopic,
                        0,
                        instant.toEpochMilli(),
                        ticketSale.getTitle(),
                        ticketSale);
                producer.send(producerRecord, callback);
            }));



        }
    }
}

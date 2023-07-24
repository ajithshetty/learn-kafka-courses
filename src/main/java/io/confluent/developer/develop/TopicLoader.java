package io.confluent.developer.develop;

import io.confluent.demo.Rating;
import io.confluent.developer.StreamsUtils;
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
            Producer<Long, Rating> producer = new KafkaProducer<Long, Rating>(properties)) {
            final String inputTopic = properties.getProperty("running-average.input.ratings.topic.name");
            System.out.println("inputTopic:"+inputTopic);
            final String outputTopic = properties.getProperty("running-average.output.ratings.topic.name");
            List <org.apache.kafka.clients.admin.NewTopic> topics = Arrays.asList(
                    StreamsUtils.createTopic(inputTopic),
                    StreamsUtils.createTopic(outputTopic));
            adminClient.deleteTopics(Arrays.asList(inputTopic, outputTopic));
            adminClient.createTopics(topics);


            Callback callback = StreamsUtils.callback();

            Instant instant = Instant.now();

            Rating rating1 = Rating.newBuilder()
                    .setRating(5)
                    .setMovieId(1001)
                    .setMovieName("name-1001")
                    .setTime(instant.toEpochMilli())
                    .build();

            instant = instant.plusSeconds(8L);
            Rating rating2 = Rating.newBuilder()
                    .setRating(7)
                    .setMovieId(1002)
                    .setMovieName("name-1002")
                    .setTime(instant.toEpochMilli())
                    .build();

            instant = instant.plusSeconds(10L);
            Rating rating3 = Rating.newBuilder()
                    .setRating(8)
                    .setMovieId(1003)
                    .setMovieName("name-1003")
                    .setTime(instant.toEpochMilli())
                    .build();

            instant = instant.plusSeconds(12L);
            Rating rating4 = Rating.newBuilder()
                    .setRating(9)
                    .setMovieId(1001)
                    .setMovieName("name-1001")
                    .setTime(instant.toEpochMilli())
                    .build();

            List <Rating> ratingOrders = Arrays.asList(
                    rating1,
                    rating2,
                    rating3,
                    rating4 );

            ratingOrders.forEach((ratingOrder -> {
                ProducerRecord<Long, Rating> producerRecord = new ProducerRecord<>(
                        inputTopic,
                        0,
                        ratingOrder.getTime(),
                        ratingOrder.getMovieId(),
                        ratingOrder);
                producer.send(producerRecord, callback);
            }));

        }
    }
}

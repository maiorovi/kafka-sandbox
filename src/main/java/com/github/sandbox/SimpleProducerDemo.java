package com.github.sandbox;

import com.github.sandbox.util.BasicProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        BasicProperties basicProperties = new BasicProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(basicProperties.toProperties());
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello Kafka From Java2");

        Future<RecordMetadata> send = producer.send(record);

        send.get();
    }
}

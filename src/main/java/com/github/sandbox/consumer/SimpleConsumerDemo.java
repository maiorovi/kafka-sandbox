package com.github.sandbox.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SimpleConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerDemo.class);

    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(ConsumerProperties.newConsumerProperties());

        kafkaConsumer.subscribe(Collections.singleton("first_topic"));

        //poll for a new data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records) {
                LOG.info("Key: " + record.key() + ", Value: " + record.value());
                LOG.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

}

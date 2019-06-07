package com.github.sandbox;

import com.github.sandbox.util.BasicProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

public class SimpleProducerWithCallbackDemo {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducerWithCallbackDemo.class);

    public static void main(String[] args) throws InterruptedException {
        BasicProperties basicProperties = new BasicProperties();

        KafkaProducer<String, String> producer = new KafkaProducer<>(basicProperties.toProperties());

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "this is key", "Calback Content1234");
        LOG.info("Sending >>>>");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.error("Exception Happened", exception);
                } else  {
                    LOG.info("Message received by kafka!");
                    LOG.info("Received new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset " + metadata.offset() + "\n" +
                            "Timestamp" + metadata.timestamp() );
                }
            }
        });

        Thread.sleep(5000);
    }

}

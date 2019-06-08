package com.github.sandbox.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static com.github.sandbox.consumer.Constants.FIRST_TOPIC;

public class ConsumerWithThreads {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerWithThreads.class);

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerThread consumerThread = new ConsumerThread(countDownLatch, FIRST_TOPIC);

        new Thread(consumerThread).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Caught shutdownhook");
            consumerThread.shutdown();
        }));

        countDownLatch.await();
    }

    public static class ConsumerThread implements Runnable {
        private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(CountDownLatch countDownLatch, String topic) {
            this.countDownLatch = countDownLatch;

            kafkaConsumer = new KafkaConsumer<>(ConsumerProperties.newConsumerProperties());
            kafkaConsumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {

                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        LOG.info("Key: " + record.key() + ", Value: " + record.value());
                        LOG.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException ex) {
                LOG.info("Received shutdown signal!");
            } finally {
                kafkaConsumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            kafkaConsumer.wakeup();
        }
    }
}

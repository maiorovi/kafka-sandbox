package com.github.sandbox.elastic;

import com.github.sandbox.consumer.ConsumerProperties;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElastiSearchConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(ElastiSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        ElastiSearchConsumer elastiSearchConsumer = new ElastiSearchConsumer();
        RestHighLevelClient highLevelClient = elastiSearchConsumer.createHighLevelClient();
        KafkaConsumer<String, String> kafkaConsumer = elastiSearchConsumer.createKafkaConsumer();

        kafkaConsumer.subscribe(Collections.singleton("twitter_tweets"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                int count = records.count();
                LOG.info("Received " + count + " records");
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    //kafka generic ID
                    String id = record.topic() + "_" + record.partition() + "_" +record.offset();
                    //process record
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .id(id) //id is for make request idempotent
                            .source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }

                if (count > 0) {
                    BulkResponse bulk = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    LOG.info("Committing offsets...");
                    kafkaConsumer.commitSync();
                    LOG.info("Offsets have been committed");
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {

        } finally {
            kafkaConsumer.close();
        }

        highLevelClient.close();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = ConsumerProperties.newConsumerProperties("kafka-demo-elastic");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return new KafkaConsumer<>(properties);
    }


    private RestHighLevelClient createHighLevelClient() {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);

        return restHighLevelClient;
    }
}

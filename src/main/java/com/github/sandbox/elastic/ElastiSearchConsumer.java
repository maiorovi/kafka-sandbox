package com.github.sandbox.elastic;

import com.github.sandbox.consumer.ConsumerProperties;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

                for (ConsumerRecord<String, String> record : records) {
                    //process record
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON);

                    IndexResponse response = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

                    LOG.info("Inserted tweet with id {} ", response.getId());
                    Thread.sleep(2000);
                }
            }
        } catch (Exception e) {

        } finally {
            kafkaConsumer.close();
        }

        highLevelClient.close();
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        return new KafkaConsumer<>(ConsumerProperties.newConsumerProperties("kafka-demo-elastic"));
    }


    private RestHighLevelClient createHighLevelClient() {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);

        return restHighLevelClient;
    }
}

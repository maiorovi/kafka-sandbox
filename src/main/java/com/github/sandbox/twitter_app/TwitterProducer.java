package com.github.sandbox.twitter_app;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static final Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    String consumerKey = "";
    String consumerSecret = "";
    String token = "";
    String secret = "";

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();
        LOG.info("Connected to application");

        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                twitterClient.stop();
                throw new RuntimeException(e);
            }

            if (msg != null) {
                LOG.info("Received message {}", msg);
            }
        }

        LOG.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue ) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        return hosebirdClient;
    }
}

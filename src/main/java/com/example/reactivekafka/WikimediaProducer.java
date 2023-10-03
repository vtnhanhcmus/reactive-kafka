package com.example.reactivekafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaProducer implements CommandLineRunner {

    @Value("${spring.kafka.topic.name}")
    private String topic;

    @Autowired
    private ReactiveKafkaProducerTemplate<String, WikiData> reactiveKafkaProducerTemplate;

    @Autowired
    private ObjectMapper mapper;


    public void sendMessage() throws InterruptedException {
        EventHandler eventHandler = new WikimediaChangesHandler(reactiveKafkaProducerTemplate, topic, mapper);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }

    @Override
    public void run(String... args) throws Exception {
        sendMessage();
    }
}

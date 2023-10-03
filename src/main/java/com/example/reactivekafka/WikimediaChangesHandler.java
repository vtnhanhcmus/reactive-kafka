package com.example.reactivekafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;

public class WikimediaChangesHandler implements EventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesHandler.class);

    private ReactiveKafkaProducerTemplate<String, WikiData> reactiveKafkaProducerTemplate;
    private String topic;

    private ObjectMapper mapper;

    public WikimediaChangesHandler(ReactiveKafkaProducerTemplate<String, WikiData> reactiveKafkaProducerTemplate, String topic, ObjectMapper mapper) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
        this.topic = topic;
        this.mapper = mapper;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        LOGGER.info(String.format("event data -> %s", messageEvent.getData()));
        WikiData wikiData = new Gson().fromJson(messageEvent.getData(), WikiData.class);
        reactiveKafkaProducerTemplate.send(topic, wikiData)
                .doOnSuccess(senderResult -> LOGGER.info("sent {} offset : {}", wikiData, senderResult.recordMetadata().offset()))
                .subscribe();
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}

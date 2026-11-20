package com.fairstock.fairstock.function;

import com.fairstock.fairstock.model.ReservationEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class EventParser
        extends RichFlatMapFunction<String, ReservationEvent> {

    private transient ObjectMapper mapper;

    @Override
    public void open(
            org.apache.flink.configuration.Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(
            String json,
            Collector<ReservationEvent> out) {

        try {
            JsonNode node = mapper.readTree(json);

            String eventId =
                    node.path("EventId").asText("");

            String eventType =
                    node.path("EventType").asText("");

            String productId =
                    node.path("ProductId").asText("");

            if (eventId.isBlank()
                    || eventType.isBlank()
                    || productId.isBlank()) {
                System.err.println(
                        "Skipping incomplete event: " + json);
                return;
            }

            ReservationEvent event = new ReservationEvent(
                    eventId,
                    eventType,
                    productId,
                    node.path("ReservationId").asText(""),
                    node.path("CustomerId").asText("")
            );

            out.collect(event);

        } catch (Exception ex) {
            System.err.println(
                    "Invalid Kafka event: " + ex.getMessage());
        }
    }
}

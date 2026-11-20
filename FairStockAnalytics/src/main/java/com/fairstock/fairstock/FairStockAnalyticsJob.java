package com.fairstock.fairstock;

import com.fairstock.fairstock.function.DemandAggregator;
import com.fairstock.fairstock.function.EventParser;
import com.fairstock.fairstock.function.PressureCalculator;
import com.fairstock.fairstock.model.ReservationEvent;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FairStockAnalyticsJob {

    public static void main(String[] args) throws Exception {

        String bootstrapServers = System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:9092");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        env.setParallelism(1);

        // 1. Kafka source: C# reservation events
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setTopics("reservation-events")
                        .setGroupId("fairstock-flink-analytics")
                        .setStartingOffsets(
                                OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(
                                new SimpleStringSchema())
                        .build();

        // 2. Read messages continuously
        DataStream<String> rawEvents =
                env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Reservation Kafka Source");

        // 3. Parse JSON into Java objects
        DataStream<ReservationEvent> events =
                rawEvents.flatMap(new EventParser())
                        .name("Parse Reservation Events");

        // 4. Group by product and aggregate 30-second windows
        DataStream<String> pressureSignals =
                events
                        .keyBy(event -> event.productId)
                        .window(
                                TumblingProcessingTimeWindows.of(
                                        Time.seconds(30)))
                        .aggregate(
                                new DemandAggregator(),
                                new PressureCalculator())
                        .name("Calculate Demand Pressure");

        // 5. Kafka sink: pressure signals for C#
        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema
                                        .<String>builder()
                                        .setTopic("inventory-pressure")
                                        .setValueSerializationSchema(
                                                new SimpleStringSchema())
                                        .build())
                        .setDeliveryGuarantee(
                                DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        pressureSignals.sinkTo(sink)
                .name("Publish Pressure Signals");

        pressureSignals.print("FAIRSTOCK PRESSURE");

        env.execute("FairStock Real-Time Demand Analytics");
    }
}


package com.appu.main;

import com.appu.constants.AppConstants;
import com.appu.constants.EquipmentKeys;
import com.appu.util.DeviceTableHelper;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
/**
 * Analytic Class to capture the changes from metadata and update to final stream
 *
 * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
@Slf4j
public class EquipmentAnalytics {
    public static void main(String args[]) {
        StreamsBuilder builder = new StreamsBuilder();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "equipment-analytics-app-001");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS_CONFIG);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConstants.AUTO_OFFSET_RESET_CONFIG_EARLIEST);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        runAnalytics(builder);
        KafkaStreams deviceStream = new KafkaStreams(builder.build(),props);
        deviceStream.start();
        // print the topology
        log.info(deviceStream.toString());

        // shutdown hook to correctly close the streams application
       Runtime.getRuntime().addShutdownHook(new Thread(deviceStream::close));
    }
    /**
     * Method to do execute analytics
     *
     * * Date Sep 30 2021
     */
    public static void runAnalytics(StreamsBuilder builder)
    {
        final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);
        final Serde<String> stringSerde = Serdes.String();

        //meta data update of device coming from another topic
        KStream<String,JsonNode> metaUpdate = builder.stream(AppConstants.DEVICE_META_UPDATE_TOPIC, Consumed.with(stringSerde,jsonSerde)).selectKey((k,v) -> v.get(EquipmentKeys.SERIAL_KEY).toString().trim());

        // device stream in json format
        KStream<String,JsonNode> deviceStream = builder.stream(AppConstants.DEVICE_SOURCE_TOPIC_JSON, Consumed.with(stringSerde,jsonSerde));
        KTable<String,JsonNode>  deviceStagingTable = deviceStream.selectKey((k,v) -> v.get(EquipmentKeys.SERIAL_KEY).toString().trim()).groupByKey().reduce((oldvalue, updatevalue) -> updatevalue, Materialized.with(stringSerde,jsonSerde));

        KStream<String,JsonNode>  deviceUpdatedStream = DeviceTableHelper.joinMetadata(metaUpdate,deviceStagingTable);

        deviceUpdatedStream.to(AppConstants.DEVICE_SOURCE_TOPIC_JSON, Produced.with(stringSerde,jsonSerde));
   
        deviceStagingTable.toStream().to(AppConstants.DEVICE_FINAL_lIVE_DATA);

    }
}



package com.appu.main;

import com.appu.constants.AppConstants;
import com.appu.equipmentvalue;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
/**
 * Class to convert the incoming avro stream to json
 *
 * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
@Slf4j
public class EquipmentAvroToJson {
    public static void main(String args[]) {
        StreamsBuilder builder = new StreamsBuilder();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "equipment-avrotojson-001");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS_CONFIG);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConstants.AUTO_OFFSET_RESET_CONFIG_EARLIEST);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,AppConstants.SCHEMA_REGISTRY_URL_CONFIG);
        //Exactly once processing!!
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        jsonConvert(builder);
        KafkaStreams avrostream = new KafkaStreams(builder.build(),props);
        avrostream.start();

        // print the topology
        log.info(avrostream.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(avrostream::close));
    }
    /**
     * Method to do convert avro to json
     *
     * * Date Sep 30 2021
     */
    public static void jsonConvert(StreamsBuilder builder) {
        final Serde<String> stringSerde = Serdes.String();

        //Reading Data From Device
        KStream<String,String> inputStream = builder.stream(AppConstants.DEVICE_SOURCE_TOPIC_AVRO).selectKey((k,v) -> ((equipmentvalue) v).getSerial().toString().trim()).mapValues((k,v) -> v.toString());

        inputStream.to(AppConstants.DEVICE_SOURCE_TOPIC_JSON, Produced.with(stringSerde,stringSerde));

    }
}


    
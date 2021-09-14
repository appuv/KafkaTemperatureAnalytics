package com.appu.helper;

import com.appu.constants.AppConstants;
import com.appu.equipmentkey;
import com.appu.equipmentvalue;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
/**
 * Helper Class to consume equipment avro streams
 *
 * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */

@Slf4j
public class EquipmentAvroConsumer {

    public static void main(String[] args) {

        String groupId= "avro-consumer-app-001";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConstants.SCHEMA_REGISTRY_URL_CONFIG);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppConstants.AUTO_OFFSET_RESET_CONFIG_EARLIEST);

        String topic = AppConstants.DEVICE_SOURCE_TOPIC_AVRO;
        log.info("topic is " + topic);
        // create consumer
        KafkaConsumer<equipmentkey, equipmentvalue> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        // poll for new data
        while (true) {
            ConsumerRecords<equipmentkey, equipmentvalue> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<equipmentkey, equipmentvalue> record : records) {
                log.info("Key: " + record.key().toString() + ", Value: " + record.value().toString());
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }

        }
    }
}

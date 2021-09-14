package com.appu.helper;

import com.appu.constants.AppConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;
/**
 * Helper Class to produce mock equipment json streams
 *
 * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
@Slf4j
public class EquipmentProducer {

    public static void main(String[] args) {


        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = null;

        String key = "";
        String value = "";
        String file_path = AppConstants.EQUIPMENT_META_MOCK_AVRO_DATA_LOCATION;

        String topic = AppConstants.DEVICE_META_UPDATE_TOPIC;
        log.info("topic is "+ topic);
        InputStream is = EquipmentAvroProducer.class.getResourceAsStream(file_path);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = br.readLine()) != null) {
                key= line.strip().split(AppConstants.DELIMITTER_PIPE)[0];
                value = line.strip().split(AppConstants.DELIMITTER_PIPE)[1];
                record =new ProducerRecord<String, String>(topic, key,value );
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e.getMessage());
                        }
                    }
                });

            }
        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
        }
        catch (IOException e) {
            log.error(e.getMessage());
        }

        producer.flush();
        producer.close();
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "Shutdown-thread"));
    }
}

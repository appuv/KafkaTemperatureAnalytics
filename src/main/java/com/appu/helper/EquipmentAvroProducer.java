package com.appu.helper;

import com.appu.constants.AppConstants;
import com.appu.constants.EquipmentKeys;
import com.appu.equipmentkey;
import com.appu.equipmentvalue;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.json.JSONObject;

import java.io.*;
import java.util.Properties;
/**
 * Helper Class to produce mock equipment avro streams
 *
 * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
@Slf4j
public class EquipmentAvroProducer {

    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS_CONFIG);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConstants.SCHEMA_REGISTRY_URL_CONFIG);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        final KafkaProducer<equipmentkey, equipmentvalue> producer = new KafkaProducer<>(properties);

        String file_path = AppConstants.EQUIPMENT_MOCK_AVRO_DATA_LOCATION;
        String topic = AppConstants.DEVICE_SOURCE_TOPIC_AVRO;
        log.info("topic is "+ topic);
        InputStream is = EquipmentAvroProducer.class.getResourceAsStream(file_path);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = br.readLine()) != null) {
                log.info(line);
                JSONObject keyObject = new JSONObject((line.strip().split(AppConstants.DELIMITTER_PIPE)[0]));
                JSONObject valueObject = new JSONObject((line.strip().split(AppConstants.DELIMITTER_PIPE)[1]));

                equipmentkey key = new equipmentkey();
                key.setSerial(keyObject.get(EquipmentKeys.SERIAL_KEY).toString());
                equipmentvalue value = new equipmentvalue();

                value.setSerial(valueObject.get(EquipmentKeys.SERIAL_KEY).toString());
                value.setOwner(valueObject.get(EquipmentKeys.OWNER).toString());
                value.setTemp(valueObject.get(EquipmentKeys.TEMP).toString());


                ProducerRecord<equipmentkey, equipmentvalue> producerRecord = new ProducerRecord<equipmentkey, equipmentvalue>(
                        topic, key,value
                );

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            log.info(String.valueOf(metadata));
                        } else {
                            log.error(exception.getMessage());
                        }
                    }
                });
                producer.flush();
            }
        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());

        }
        producer.flush();
        producer.close();
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "Shutdown-thread"));
    }
}

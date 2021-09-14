package com.appu.constants;
import java.util.ResourceBundle;
/**
 * Constant Class for App Configurations
 *
 *  Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
public class AppConstants {

    //public static final String PROPERTY_FILE_LOCATION = "src/main/resources/application.properties";
    private static final ResourceBundle rb = ResourceBundle.getBundle("application");
    public static final String BOOTSTRAP_SERVERS_CONFIG = rb.getString("kafka.bootstrap.servers");
    public static final String SCHEMA_REGISTRY_URL_CONFIG = rb.getString("kafka.schema.registry.url");
    public static final String AUTO_OFFSET_RESET_CONFIG_EARLIEST = rb.getString("kafka.auto.offset.reset.config");

    public static final String DEVICE_SOURCE_TOPIC_AVRO = rb.getString("kafka.topic_equipment_avro");
    public static final String DEVICE_SOURCE_TOPIC_JSON = rb.getString("kafka.topic_equipment_json");
    public static final String DEVICE_META_UPDATE_TOPIC = rb.getString("kafka.topic_equipment_meta_json") ;
    public static final String DEVICE_UPDATED_lIVE_DATA = rb.getString("kafka.topic_equipment_output_json");
    public static final String DEVICE_FINAL_lIVE_DATA = rb.getString("kafka.topic_equipment_final_output_json");

    public static final String EQUIPMENT_MOCK_AVRO_DATA_LOCATION = rb.getString("equipment.mock_data.avro");
    public static final String EQUIPMENT_MOCK_JSON_DATA_LOCATION = rb.getString("equipment.meta.mock_data.json");
    public static final String EQUIPMENT_META_MOCK_AVRO_DATA_LOCATION = rb.getString("equipment.mock_data.json");

    public static final String DELIMITTER_PIPE = "\\|";



}

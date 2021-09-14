package com.appu.main;

import com.appu.constants.AppConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.codehaus.plexus.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
/**
 * Test Class to test equipment stream app
 *
 * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
@Slf4j
public class EquipmentAnalyticsTest {

    final static Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
    final static Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
    private TopologyTestDriver topologyTestDriver;
    private StreamsBuilder builder = new StreamsBuilder();
    private TestInputTopic<String, JsonNode> equipmentJsonTopic = null;
    private TestInputTopic<String, JsonNode> equipmentMetaTopic = null;
    private TestOutputTopic<String, JsonNode> equipmentFinalTopic = null;

    @Before
    public void setup ()
    {
        Properties props = new Properties();
        EquipmentAnalytics.runAnalytics(builder);

        Topology topology = builder.build();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "device-temperature-analytics-test-001");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyTestDriver = new TopologyTestDriver(topology,props);

        equipmentJsonTopic = topologyTestDriver.createInputTopic(
                AppConstants.DEVICE_SOURCE_TOPIC_JSON,
                new StringSerializer(),
                jsonNodeSerializer);

        equipmentMetaTopic = topologyTestDriver.createInputTopic(
                AppConstants.DEVICE_META_UPDATE_TOPIC,
                new StringSerializer(),
                jsonNodeSerializer);

        equipmentFinalTopic = topologyTestDriver.createOutputTopic(
                AppConstants.DEVICE_FINAL_lIVE_DATA,
                new StringDeserializer(),
                jsonNodeDeserializer);



    }

   @After
    public void tearDown() {
        topologyTestDriver.close();
       try {
           FileUtils.deleteDirectory(new File("/tmp/kafka-streams/device-temperature-analytics-test-001"));
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

    @Test
    public void runAnalyticsTest() {
        ObjectMapper mapper = new ObjectMapper();

        //Equipment Json Topic
        String equipmentInputValueJson = "{\"serial\":\"1\",\"owner\":\"appu\",\"temp\":\"80\",\"location\":\"earth\"}";
        String equipmentKeyJson = "{\"serial\":\"1\"}";


        //Equipment meta  Topic
       String equipmentMetaValueJson = "{\"serial\":\"1\",\"owner\":\"appu\",\"location\":\"heaven\"}";
       String equipmentMetaKeyJson = "{\"serial\":\"1\"}";

        //Expected Result
        String expectedResultValue = "{\"serial\":\"1\",\"owner\":\"appu\",\"temp\":\"80\",\"location\":\"heaven\"}";


        try {

            equipmentJsonTopic.pipeInput(equipmentKeyJson, mapper.readTree(equipmentInputValueJson));

            equipmentMetaTopic.pipeInput(equipmentMetaKeyJson, mapper.readTree(equipmentMetaValueJson));
            //equipmentMetaTopic.pipeInput("", mapper.readTree(equipmentMetaValueJson));

            Assert.assertEquals(new KeyValue<>("\"1\"",mapper.readTree(equipmentInputValueJson)),equipmentFinalTopic.readKeyValue());
            Assert.assertEquals(new KeyValue<>("\"1\"",mapper.readTree(expectedResultValue)),equipmentFinalTopic.readKeyValue());


        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

}

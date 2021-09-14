package com.appu.util;

import com.appu.constants.EquipmentKeys;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
/**
 * Utility Class to do stream level operations
 *
 * * Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
@Slf4j
public class DeviceTableHelper {
    public static KStream<String, JsonNode> joinMetadata(KStream<String,JsonNode> metaUpdate,
                                                         KTable<String,JsonNode>  deviceStagingTable) {

        log.info("Before Join");

        KStream<String,JsonNode> updatedStream = metaUpdate.leftJoin(deviceStagingTable, (v1, v2) ->
        {
            log.info("Inside Join");

            ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
            try {
                jsonNode.put(EquipmentKeys.SERIAL_KEY,v2.get(EquipmentKeys.SERIAL_KEY));
                jsonNode.put(EquipmentKeys.TEMP,v2.get(EquipmentKeys.TEMP));
                jsonNode.put(EquipmentKeys.OWNER,((v1.get(EquipmentKeys.OWNER))!=null ? v1.get(EquipmentKeys.OWNER) : v2.get(EquipmentKeys.OWNER)));
                jsonNode.put(EquipmentKeys.LOCATION,((v1.get(EquipmentKeys.LOCATION))!=null ? v1.get(EquipmentKeys.LOCATION) : v2.get(EquipmentKeys.LOCATION)));
                return jsonNode;

            } catch (Exception e)
            {
                log.error(e.toString());
                return null;
            }
        });
        return updatedStream;
    }

}



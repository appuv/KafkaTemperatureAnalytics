package com.appu.constants;

import java.util.ResourceBundle;
/**
 * Constant Class for stream key values
 *
 *  Date Sep 14 2021
 *
 * @author Appu V
 * @version 0.1
 */
public class EquipmentKeys {
    private static final ResourceBundle rb = ResourceBundle.getBundle("equipment");
    public static final String SERIAL_KEY = rb.getString("equipment.serial_key");
    public static final String OWNER = rb.getString("equipment.owner");
    public static final String TEMP = rb.getString("equipment.temp");
    public static final String LOCATION = rb.getString("equipment.location");
}


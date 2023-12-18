package com.boluo.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chao
 * @datetime 2023-12-18 11:20
 * @description
 */
public class CommonUtils {

    private CommonUtils() {

    }

    public static Map<String, String> parseJsonArgs(String... args) {
        Gson gson = new GsonBuilder().serializeNulls().create();
        return gson.fromJson(args[0], HashMap.class);
    }


}

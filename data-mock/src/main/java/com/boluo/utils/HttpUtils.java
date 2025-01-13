package com.boluo.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chao
 * @datetime 2025-01-13 0:02
 * @description
 */
public class HttpUtils extends utils.HttpUtils {
    private static final Map<String, String> paramsMap = new ConcurrentHashMap<>(32);

    public static Map<String, String> parseParams(String params) {
        String regex = "([^&=]+)=([^&]*)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(params);

        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);
            paramsMap.put(key, value);
        }
        return paramsMap;
    }


}

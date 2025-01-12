package com.boluo.utils;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chao
 * @datetime 2025-01-13 0:02
 * @description
 */
public class HttpUtils extends utils.HttpUtils {

    public static HashMap<String, String> parseParams(String params) {
        String regex = "([^&=]+)=([^&=]+)";
        System.out.println(params);

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(params);

        // 计算总共有几对
        int count = 0;
        while (matcher.find()) {
            count++;
        }

        // TODO
        if (matcher.find()) {
            System.out.println("匹配成功, key: " + matcher.group(1));
            System.out.println("匹配成功, value: " + matcher.group(2));
        }


        System.out.println("AAaa");
        return null;
    }


}

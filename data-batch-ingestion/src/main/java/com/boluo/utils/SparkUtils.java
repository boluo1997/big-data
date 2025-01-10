package com.boluo.utils;

import org.apache.spark.sql.SparkSession;

/**
 * @author chao
 * @datetime 2024-12-16 14:10
 * @description
 */
public class SparkUtils {

    public static SparkSession initialSpark() {

        String osName = System.getProperty("os.name");
        System.out.println("System name: " + osName);

        if (osName.startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "/hadoop/");
        }

        return SparkSession.builder().appName("SparkAppName").master("local[*]").getOrCreate();

    }


}

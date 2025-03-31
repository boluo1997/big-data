package utils;

import org.apache.spark.sql.SparkSession;

/**
 * @author chao
 * @datetime 2025-01-11 16:59
 * @description
 */
public class SparkUtils {

    private static SparkSession spark;

    public static SparkSession initialSpark() {
        if (spark == null) {
            spark = SparkSession.builder().appName("SparkAppName").master("local[*]").getOrCreate();
        }
        return spark;
    }

}

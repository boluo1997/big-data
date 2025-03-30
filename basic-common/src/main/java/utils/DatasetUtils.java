package utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author chao
 * @datetime 2025-03-29 22:23
 * @description
 */
public class DatasetUtils {

    public static Dataset<Row> readFromMySQL(String url, String tableName, String username, String password) {
        return SparkSession.active().read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", tableName)
                .option("user", username)
                .option("password", password)
                .load();
    }

    public static void writeToMySQL(Dataset<Row> ds, String url, String tableName, String username, String password) {
        System.out.println("write to mysql...");
        ds.write().format("jdbc")
                .option("url", url)
                .option("dbtable", tableName)
                .option("user", username)
                .option("password", password)
                .mode(SaveMode.Append)
                .save();
    }
}

package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.List;

/**
 * @author chao
 * @datetime 2025-03-29 22:23
 * @description
 */
public class DatasetUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

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


    public static Dataset<Row> list2Ds(List<ObjectNode> list) {

        Dataset<String> dsJson = SparkSession.active()
                .createDataset(list, Encoders.kryo(ObjectNode.class))
                .map((MapFunction<ObjectNode, String>) mapper::writeValueAsString, Encoders.STRING());

        return SparkSession.active().read().json(dsJson);
    }


}

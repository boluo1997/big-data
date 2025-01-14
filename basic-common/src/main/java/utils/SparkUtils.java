package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.List;

/**
 * @author chao
 * @datetime 2025-01-11 16:59
 * @description
 */
public class SparkUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static SparkSession initialSpark() {

        String osName = System.getProperty("os.name");
        System.out.println("System name: " + osName);

        if (osName.startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "/hadoop/");
        }

        return SparkSession.builder().appName("SparkAppName").master("local[*]").getOrCreate();
    }


    public static Dataset<Row> list2Ds(List<ObjectNode> list) {

        Dataset<String> dsJson = SparkSession.active()
                .createDataset(list, Encoders.kryo(ObjectNode.class))
                .map((MapFunction<ObjectNode, String>) mapper::writeValueAsString, Encoders.STRING());

        return SparkSession.active().read().json(dsJson);
    }


    public static void writeToMySQL(Dataset<Row> ds, String url, String tableName, String user, String password) {
        System.out.println("write to mysql...");
//        ds.write().format("jdbc")
//                .option("url", url)
//                .option("dbtable", tableName)
//                .option("user", user)
//                .option("password", password)
//                .mode(SaveMode.Overwrite)
//                .save();
    }

}

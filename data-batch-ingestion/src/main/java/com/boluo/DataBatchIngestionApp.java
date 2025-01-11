package com.boluo;

import com.boluo.config.BatchConfig;
import com.boluo.process.ProcessorFactory;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import utils.SparkUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * @author chao
 * @datetime 2024-12-15 20:34
 * @description
 */
public class DataBatchIngestionApp {

    // {"job_env":dev,"job_type":daily}
    public static void main(String[] args) {
        BatchConfig batchConfig = BatchConfig.getInstance().load(args);
        ProcessorFactory.getInstance().getProcessor(batchConfig).process();
    }

    private static void mockData() throws IOException {

        SparkSession spark = SparkUtils.initialSpark();
        StructType scheme = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("email", "string");

        Row row1 = RowFactory.create(1, "dingc", "cding@state.com");
        Row row2 = RowFactory.create(2, "boluo", "boluo@state.com");
        Row row3 = RowFactory.create(3, "qidai", "qidai@state.com");

        Dataset<Row> ds = spark.createDataFrame(ImmutableList.of(row1, row2, row3), scheme);
        ds.show(false);

        // mock 到 mysql,
        System.out.println("读取配置文件: ");
        Properties prop = new Properties();
        // 如果读取为null, 需要查看启动类是不是 当前子模块下
        InputStream resourceInputStream = DataBatchIngestionApp.class.getResourceAsStream("/application-dev.properties");
        prop.load(resourceInputStream);


        String url = prop.getProperty("datasource.url");
        String dbTable = prop.getProperty("datasource.db.table");
        String user = prop.getProperty("datasource.username");
        String password = prop.getProperty("datasource.password");

        ds.write().format("jdbc")
                .option("url", url)
                .option("dbtable", dbTable)
                .option("user", user)
                .option("password", password)
                .mode(SaveMode.Overwrite) // 使用覆盖模式
                .save();

        System.out.println("数据mock完成!!");
    }

}

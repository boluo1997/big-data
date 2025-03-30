package com.boluo.dao.impl;

import com.boluo.dao.ReadMysqlDao;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;
import utils.SparkUtils;

import java.util.HashMap;
import java.util.Map;


/**
 * @author chao
 * @datetime 2025-03-31 0:11
 * @description
 */
@Component
public class ReadMysqlDaoImpl implements ReadMysqlDao {

    @Override
    public void readStreamingFromMySQL(String tableName) throws Exception {

        // TODO 使用微批处理模拟流式
        SparkSession spark = SparkUtils.initialSpark();

        // MySQL JDBC 连接参数
        String jdbcUrl = "jdbc:mysql://mysql.sqlpub.com:3306/dingchao_db";
        String dbTable = "bronze_daily";
        String user = "dingchao";
        String password = "6049773e1914457d";


        // 配置JDBC连接
        Map<String, String> options = new HashMap<>();
        options.put("url", jdbcUrl);
        options.put("dbtable", dbTable);
        options.put("user", user);
        options.put("password", password);
        options.put("driver", "com.mysql.cj.jdbc.Driver");

        // 初始化起始ID
        long lastProcessedId = 0L;  // 假设你有一个起始ID

        while (true) {
            // 通过自增ID来增量读取数据
            String query = "SELECT * FROM " + dbTable + " WHERE id > " + lastProcessedId;

            // 使用JDBC查询 MySQL 数据库
            Dataset<Row> newData = spark.read()
                    .format("jdbc")
                    .options(options)
                    .option("dbtable", "(" + query + ") AS tmp")
                    .load();

            // 检查是否有数据返回
            if (newData.isEmpty()) {
                System.out.println("No new data, skipping...");
            } else {
                // 获取最大的id
                Row maxIdRow = newData.agg(functions.max("id")).head();
                if (maxIdRow != null && maxIdRow.get(0) != null) {
                    lastProcessedId = maxIdRow.getLong(0);  // 更新最大id
                }

                // 数据处理（你可以在这里做其他处理）
                newData.show();
            }

            // 模拟周期性读取
            Thread.sleep(5000);  // 每5秒读取一次（根据需要调整）
        }

    }
}

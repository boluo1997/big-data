package com.boluo.dao.impl;

import com.boluo.constants.CleansingConstants;
import com.boluo.dao.ReadJDBCDao;
import constants.SparkConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import utils.SparkUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * @author chao
 * @datetime 2025-03-31 0:11
 * @description
 */
@Component
public class ReadJDBCDaoImpl implements ReadJDBCDao {

    @Autowired
    private Environment environment;


    @Override
    public void readStreamingFromMySQL(String tableName) throws Exception {

        Map<String, String> jdbcOptions = new HashMap<String, String>() {{
            put(SparkConstants.OPTION_URL, environment.getProperty("database.mysql.url"));
            put(SparkConstants.OPTION_DRIVER, environment.getProperty("database.mysql.driver"));
            put(SparkConstants.OPTION_USER, environment.getProperty("database.mysql.username"));
            put(SparkConstants.OPTION_PASSWORD, environment.getProperty("database.mysql.password"));
        }};

        long latestProcessedId = 0L;
        while (!Thread.currentThread().isInterrupted()) {
            String query = String.format("(select * from %s where id > %d) as tmp", tableName, latestProcessedId);
            Dataset<Row> newDataDs = SparkUtils.initialSpark().read()
                    .format(SparkConstants.FORMAT_JDBC)
                    .options(jdbcOptions)
                    .option(SparkConstants.OPTION_DB_TABLE, query)
                    .load();

            if (newDataDs.isEmpty()) {
                System.out.println("No new data, skipping...");
                Thread.sleep(5000);
                continue;
            }

            Row maxIdRow = newDataDs.agg(functions.max(CleansingConstants.INCREMENTAL_KEY)).head();
            if (Objects.nonNull(maxIdRow) && maxIdRow.get(0) != null) {
                latestProcessedId = maxIdRow.getLong(0);
            }

            // process data
            newDataDs.show();

            Thread.sleep(5000);  // 每5秒读取一次（根据需要调整）
        }

    }


    @Override
    public void readStreamingFromOracle(String tableName) throws Exception {

        Map<String, String> jdbcOptions = new HashMap<String, String>() {{
            put(SparkConstants.OPTION_URL, environment.getProperty("database.oracle.url"));
            put(SparkConstants.OPTION_DRIVER, environment.getProperty("database.oracle.driver"));
            put(SparkConstants.OPTION_USER, environment.getProperty("database.oracle.username"));
            put(SparkConstants.OPTION_PASSWORD, environment.getProperty("database.oracle.password"));
            put(SparkConstants.OPTION_DB_TABLE, tableName);
        }};

        Dataset<Row> ds = SparkUtils.initialSpark().read()
                .format(SparkConstants.FORMAT_JDBC)
                .options(jdbcOptions)
                .load();

        ds.show(false);

    }


}

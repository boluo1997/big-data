package com.boluo.dao.impl;

import com.boluo.dao.ReadMysqlDao;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import utils.SparkUtils;

/**
 * @author chao
 * @datetime 2025-03-31 0:11
 * @description
 */
@Component
public class ReadMysqlDaoImpl implements ReadMysqlDao {

    @Override
    public Dataset<Row> readStreamingFromMySQL(String tableName) {
        return SparkUtils.initialSpark().readStream()
                .format("jdbc")
                .load();
    }

}

package com.boluo.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author chao
 * @datetime 2025-03-30 22:32
 * @description
 */
public interface ReadMysqlDao {
    public Dataset<Row> readStreamingFromMySQL(String tableName);
}

package com.boluo.dao;

/**
 * @author chao
 * @datetime 2025-03-30 22:32
 * @description
 */
public interface ReadJDBCDao {

    void readStreamingFromMySQL(String tableName) throws Exception;

    void readStreamingFromOracle(String tableName) throws Exception;
}

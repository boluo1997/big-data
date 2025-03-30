package com.boluo.dao;

/**
 * @author chao
 * @datetime 2025-03-30 22:32
 * @description
 */
public interface ReadMysqlDao {
    void readStreamingFromMySQL(String tableName) throws Exception;
}

package com.boluo.dao;

/**
 * @author chao
 * @datetime 2025-03-31 21:10
 * @description
 */
public interface WriteJDBCDao {

    void writeStreamingToOracle(String tableName) throws Exception;
    
}

package com.boluo.service;

import com.boluo.dao.ReadMysqlDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author chao
 * @datetime 2025-03-30 22:12
 * @description
 */
@Component
public class DataCleansingService {

    @Autowired
    private ReadMysqlDao readMysqlDao;

    public void processTrade() throws Exception {
        System.out.println("process trade started !! ");
        readMysqlDao.readStreamingFromMySQL("bronze_daily");
    }

}

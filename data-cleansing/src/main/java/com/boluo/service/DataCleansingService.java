package com.boluo.service;

import com.boluo.dao.ReadJDBCDao;
import com.boluo.process.JobTradeProcess;
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
    private JobTradeProcess jobTradeProcess;

    @Autowired
    private ReadJDBCDao readJDBCDao;

    public void processTrade() throws Exception {
        System.out.println("process trade started !! ");
        readJDBCDao.readStreamingFromMySQL("bronze_daily");
        // jobTradeProcess.processBronzeData()
        // readJDBCDao.readStreamingFromOracle("bo_ord_area");
    }

}

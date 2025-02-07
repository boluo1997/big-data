package com.boluo.service;

import com.boluo.config.KafkaConfigService;
import com.boluo.dao.KafkaTopicImpl;
import com.boluo.dao.ManualReplayImpl;
import com.boluo.dao.WriteDaoImpl;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author chao
 * @datetime 2025-02-03 19:28
 * @description
 */
@Component
public class StreamingService {

    @Autowired
    private KafkaConfigService kafkaConfigService;


    @Autowired
    private KafkaTopicImpl kafkaTopicImpl;

    @Autowired
    private ManualReplayImpl manualReplayImpl;

    @Autowired
    private WriteDaoImpl writeDaoImpl;


    public void runStream() throws TimeoutException, StreamingQueryException {
        Map<String, String> params = kafkaConfigService.getParams("job_trade");
        writeDaoImpl.write(params);
        System.out.println("test !!");
    }

    public void getKafkaTopicInfo() {
        Map<String, String> params = kafkaConfigService.getParams("job_trade");
        kafkaTopicImpl.getKafkaTopicInfo(params);
        System.out.println("test2 !!");
    }

    public void runManualReplay() throws TimeoutException, StreamingQueryException {
        Map<String, String> params = kafkaConfigService.getParams("job_trade");
        manualReplayImpl.write(params);
        System.out.println("test2 !!");
    }

}

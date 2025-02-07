package com.boluo.config;

import com.boluo.dao.KafkaTopicImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chao
 * @datetime 2025-02-03 20:48
 * @description
 */
@Component
public class KafkaConfigService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicImpl.class);

    @Autowired
    private Environment environment;



    public Map<String, String> getParams(String job) {

        Map<String, String> params = new HashMap<>();
        params.put("kafka.bootstrap.servers", environment.getProperty("bootstrap.servers"));
        params.put("subscribe", environment.getProperty(job + ".topic"));
        params.put("kafka.group.id", environment.getProperty(job + ".group.id"));

        params.put("maxOffsetsPerTrigger", environment.getProperty(job + ".maxOffsetsPerTrigger"));
        params.put("value.deserializer", environment.getProperty(job + ".value.deserializer"));

        params.put("failOnDataLoss", "false");
        params.put("startingOffsets", "earliest");

        String env = environment.getProperty("AppEnvName");

        logger.warn("Kafka connect successful, kafka params {} for Job:{}", params, job);
        return params;
    }

}

package com.boluo.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chao
 * @datetime 2025-02-25 22:12
 * @description
 */
@Component
public class StreamConfigService {

    @Autowired
    private Environment environment;

    public Map<String, String> getParams(String job) {
        Map<String, String> params = new HashMap<>();
        params.put("job", job);
        params.put("delta.table", environment.getProperty(job + "." + "delta.table"));
        params.put("checkpoint.location", environment.getProperty(job + "." + "checkpoint.location"));
        params.put("message.type", environment.getProperty(job + "." + "message.type"));
        return params;
    }

}

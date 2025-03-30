package com.boluo;

import com.boluo.service.DataCleansingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author chao
 * @datetime 2025-03-30 21:38
 * @description
 */

@SpringBootApplication
@EnableScheduling
@EnableAsync
public class DataCleansingApp implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DataCleansingApp.class);

    @Autowired
    private DataCleansingService service;


    public static void main(String[] args) {
        SpringApplication springApp = new SpringApplication(DataCleansingApp.class);
        String jobs = System.getProperty("job");
        springApp.setAdditionalProfiles(jobs.split(","));
        springApp.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("main class DataCleansingApp started !! ");
    }

    @Profile("job_trade")
    @Bean
    public void processTrade() throws Exception {
        service.processTrade();
    }


}

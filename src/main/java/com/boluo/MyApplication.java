package com.boluo;

import com.boluo.utils.CommonUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.Map;

/**
 * @author chao
 * @datetime 2023-12-18 11:07
 * @description
 */

@SpringBootApplication
@EnableConfigurationProperties
public class MyApplication implements CommandLineRunner {

    public static void main(String[] args) {
        System.out.println("Main Class Application Started");
        SpringApplication.run(MyApplication.class, args);
    }

    @Override
    public void run(String... args){
        Map<String, String> appArgs = CommonUtils.parseJsonArgs(args);

        System.out.println("AAAaaaa");
    }

}

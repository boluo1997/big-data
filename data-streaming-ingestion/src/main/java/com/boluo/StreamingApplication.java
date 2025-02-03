package com.boluo;

import com.boluo.dao.WriteDaoImpl;
import com.boluo.service.StreamingService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import utils.SparkUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;


/**
 * @author chao
 * @datetime 2025-01-24 20:56
 * @description
 */
@SpringBootApplication
@EnableScheduling
@EnableAsync
public class StreamingApplication implements CommandLineRunner {

    @Autowired
    private StreamingService streamingService;

    /**
     * @param Vm options : -Djob=job_trade,job_trade2
     */
    public static void main(String[] args) throws Exception {
        SpringApplication springApp = new SpringApplication(StreamingApplication.class);
        String jobs = System.getProperty("job");
        springApp.setAdditionalProfiles(jobs.split(","));
        springApp.run(args);
    }

    // Java消费Kafka中消息
    public static void run1() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "118.178.253.61:9092");

        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);

        // String topic = "test";
        String topic = "test";
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroupId");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic: %s, partition: %s, offset: %s, key: %s, value: %s%n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("finally close !!");
            consumer.commitAsync();
        }
    }


    @Override
    public void run(String... args) throws Exception {

    }

    @Profile("job_trade")
    @Bean
    public void runTrade() throws TimeoutException, StreamingQueryException {
        streamingService.runStream();
    }

    @Profile("job_get_kafka_topic_info")
    @Bean
    public void runGetKafkaTopicInfo() throws TimeoutException, StreamingQueryException {
        streamingService.getKafkaTopicInfo();
    }

}

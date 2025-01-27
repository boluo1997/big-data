package com.boluo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


/**
 * @author chao
 * @datetime 2025-01-24 21:02
 * @description
 */
public class StreamingApplicationTest {

    @Test
    public void func1(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "118.178.253.61:9092");
        // props.put("group.id", "");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "test";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> recode;
            for (int i = 0; i < 5; i++) {
                recode = new ProducerRecord<>(topic, "Message: " + i);
                producer.send(recode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("send finished !!");
    }


}

package com.boluo.dao;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author chao
 * @datetime 2025-02-03 20:13
 * @description
 */
@Component
public class KafkaTopicImpl {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicImpl.class);

    @Autowired
    private Environment environment;

    private static Properties properties = new Properties();

    public void getKafkaTopicInfo(Map<String, String> kafkaParams) {

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParams.get("kafka.bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaParams.get("kafka.group.id"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaParams.get("value.deserializer"));

        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        List<String> jobNames = Lists.newArrayList("job_trade");

        for (String jobName : jobNames) {
            System.out.println("join ,,,");
            String topicName = environment.getProperty(jobName + ".topic");
            getPartitionsForTopic(jobName, topicName, consumer);
        }
        consumer.close();
    }


    public static void getPartitionsForTopic(String jobName, String topicName, Consumer<Long, String> consumer) {
        System.out.println("get partitions for topic");
        // get partition info
        List<PartitionInfo> partitions = consumer.partitionsFor(topicName)
                .stream()
                .sorted(Comparator.comparingInt(PartitionInfo::partition))
                .collect(Collectors.toList());

        logger.info("\n");
        logger.info("Job: {}", jobName);
        logger.info("Topic: {} has {} partitions.", topicName, partitions.size());

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toList()));

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions.stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toList()));

        for (PartitionInfo partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topicName, partition.partition());
            long beginningOffset = beginningOffsets.get(topicPartition);
            long endOffset = endOffsets.get(topicPartition);
            logger.info("partition {}: beginning offset = {}, end offset = {}", partition.partition(), beginningOffset, endOffset);
            System.out.printf("partition %s: beginning offset = %s, end offset = %s%n", partition.partition(), beginningOffset, endOffset);
        }

    }

}

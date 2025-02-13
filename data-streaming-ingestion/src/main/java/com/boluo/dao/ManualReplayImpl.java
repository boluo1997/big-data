package com.boluo.dao;

import com.boluo.config.KafkaConfigService;
import com.boluo.service.StreamingService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StringType$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;
import utils.SparkUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;


/**
 * @author chao
 * @datetime 2025-02-03 20:13
 * @description
 */
@Component
public class ManualReplayImpl {

    private static final Logger LOG = LoggerFactory.getLogger(ManualReplayImpl.class);
    private static final SparkSession spark = SparkUtils.initialSpark();


    @Autowired
    private Environment environment;

    @Autowired
    private KafkaConfigService kafkaConfigService;

    @Autowired
    private StreamingService streamConfigService;

    @Autowired
    private WriteDaoImpl writeDAOImpl;

    @Autowired
    private KafkaTopicImpl kafkaTopicImpl;



    public void write(Map<String, String> params) throws TimeoutException, StreamingQueryException {

        Dataset<Row> jsonDs = spark.read()
                .option("multiLine", true)
                .option("mode", "permissive")
                .json("C:\\Users\\dingc\\IdeaProjects\\big-data\\doc\\streaming-ingestion\\streaming_manual_replay.json")
                .cache();

        jsonDs.show(false);
        System.out.println("Aaaaaaa");

        for (Row row : jsonDs.collectAsList()) {

            String job = row.getAs("job");
            Map<String, String> kafkaParams = kafkaConfigService.getParams(job);
            Map<String, String> streamParams = streamConfigService.getParams(job);

            String deltaTable = streamParams.get("delta.table");
            String msgType = streamParams.get("msgType");
            LOG.warn("insert data into table: {}, msgType: {}", deltaTable, msgType);

            String topicName = kafkaParams.get("subscribe");
            String groupId = kafkaParams.get("kafka.group.id");
            List<Integer> partitionNums = kafkaTopicImpl.getPartitions(topicName, kafkaParams);
            LOG.warn("topic: {}, group_id: {}, partitions: {}", topicName, groupId, partitionNums.toString());

            // example
            // kafkaParams.put("startingOffsets", "{\"ssgm.qforsk.forte.trade.dev\":{\"0\":2320000,\"1\":0,\"2\":0,\"3\":0}}");
            // kafkaParams.put("endingOffsets", "{\"ssgm.qforsk.forte.trade.dev\":{\"0\":2320051,\"1\":1,\"2\":1,\"3\":1}}");


            try {
                ObjectNode startObjectNode = (ObjectNode) (new ObjectMapper()).readTree("{}");
                ObjectNode endObjectNode = (ObjectNode) (new ObjectMapper()).readTree("{}");
                ObjectNode innerStartObjectNode = (ObjectNode) (new ObjectMapper()).readTree("{}");
                ObjectNode innerEndObjectNode = (ObjectNode) (new ObjectMapper()).readTree("{}");

                for (Integer num : partitionNums) {
                    innerStartObjectNode.put(num.toString(), 0);
                    innerEndObjectNode.put(num.toString(), 1);
                }

                WrappedArray<Row> partitions = row.getAs("partitions");
                JavaConverters.asJavaCollection(partitions.toList())
                        .stream()
                        .forEach(part -> {
                            innerStartObjectNode.put(part.getAs("partition_id"), Integer.parseInt(part.getAs("begin_offset")));
                            innerEndObjectNode.put(part.getAs("partition_id"), Integer.parseInt(part.getAs("end_offset")));
                        });

                startObjectNode.putPOJO(topicName, innerStartObjectNode);
                endObjectNode.putPOJO(topicName, innerEndObjectNode);

                System.out.println(startObjectNode.toString());
                System.out.println(endObjectNode.toString());

                kafkaParams.put("startingOffsets", startObjectNode.toString());
                kafkaParams.put("endingOffsets", endObjectNode.toString());

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }


            // insert data into delta table
            Dataset<Row> ds = spark.read().format("kafka")
                    .options(kafkaParams)
                    .option("failOnDataLoss", "false")
                    .load();


            ds.show(100);
            ds.withColumn("created_timestamp", functions.current_timestamp())
                    .write()
                    .format("delta")
                    .mode("append")
                    .insertInto(deltaTable);

            LOG.warn("write data end !!");


        }

    }

    // 可执行, 先保留
    public void write2(Map<String, String> params) throws TimeoutException, StreamingQueryException {

        // Dataset<Row> csvDs = spark.read().format("csv").option("inferSchema", "true").option("header", "true").load("C:\\Users\\dingc\\IdeaProjects\\big-data\\doc\\streaming-ingestion\\1.csv");
        // csvDs.show(false);

        // link:
        params.put("startingOffsets", "{\"test\":{\"0\":26}}");
        params.put("endingOffsets", "{\"test\":{\"0\":29}}");

        Dataset<Row> kafkaDs = spark.read().format("kafka")
                .options(params)
                .load();

        kafkaDs = processJsonData(kafkaDs);
        kafkaDs.show(false);

    }

    public static Dataset<Row> processJsonData(Dataset<Row> ds) {
        return ds.selectExpr(
                "cast(offset as string) as kafka_offset_num",
                "cast(partition as string) as kafka_partition",
                "cast(topic as string) as kafka_topic",
                "cast(value as string) as raw_msg",
                "cast(null as string) as raw_msg_schema"
        );
    }

    public static void main(String[] args) {
        dataSet2JsonNode();
    }

    public static void dataSet2JsonNode() {
        // 将json中读取的数据转成jsonNode
        Dataset<Row> jsonDs = spark.read().json("C:\\Users\\dingc\\IdeaProjects\\big-data\\doc\\streaming-ingestion\\streaming_manual_replay.json");
        jsonDs.show(false);
    }

}

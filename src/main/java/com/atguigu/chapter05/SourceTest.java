package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author dingc
 * @Date 2022-06-26 16:29
 * @Description 读取源数据
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2.从集合中读取数据, 常用于测试
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3.从元素中读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream1.print("1");
        stream2.print("2");
        stream3.print("3");
        numStream.print("nums");

        // 4.从socket文本流中读取
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);
        // stream4.print("4");

        // 5.从kafka中读取数据


        env.execute();

    }
}

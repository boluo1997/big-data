package com.atguigu.operator;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author dingc
 * @Date 2022-07-16 19:45
 * @Description
 */
public class Map {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> ds = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        SingleOutputStreamOperator<String> res = ds.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.name;
            }
        });

        res.print();
        env.execute();
    }

}

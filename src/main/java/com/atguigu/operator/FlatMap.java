package com.atguigu.operator;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author dingc
 * @Date 2022-07-16 19:59
 * @Description
 */
public class FlatMap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> ds = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("DingC", "./boy", 3000L)
        );

        SingleOutputStreamOperator<String> res = ds.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> out) throws Exception {
                out.collect(event.name);
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
        });

        res.print();
        env.execute();
    }

}

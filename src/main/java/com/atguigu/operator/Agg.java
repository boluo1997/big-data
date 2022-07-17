package com.atguigu.operator;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author dingc
 * @Date 2022-07-17 2:01
 * @Description
 */
public class Agg {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> ds = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("DingC", "./boy", 3000L),
                new Event("Mary", "./boy", 3300L),
                new Event("Mary", "./boy", 3500L),
                new Event("DingC", "./boy", 3800L),
                new Event("Bob", "./boy", 4000L)
        );

        // 按键分组之后进行聚合, 提取当前用户最近一次的访问数据
        ds.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.name;
                    }
                }).max("timestamp")
                .print("max: ");

        ds.keyBy(data -> data.name)
                .maxBy("timestamp")
                .print("maxBy");

        // 两者区别
        // max: > Event{name='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
        // maxBy> Event{name='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
        // max: > Event{name='Mary', url='./home', timestamp=1970-01-01 08:00:03.3}
        // maxBy> Event{name='Mary', url='./boy', timestamp=1970-01-01 08:00:03.3}

        env.execute();

    }

}

package com.atguigu.operator;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author dingc
 * @Date 2022-07-17 18:07
 * @Description
 */
public class Reduce {

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

        // 1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = ds
                .map(i -> Tuple2.of(i.name, 1L))
                // 因为Java泛型中的类型擦除, 所以这里要声明返回值类型
                .returns(new TypeHint<Tuple2<String, Long>>() {
                    @Override
                    public TypeInformation<Tuple2<String, Long>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                })
                .keyBy(i -> i.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });

        clicksByUser.print("temp: ");

        // 2.选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> res = clicksByUser
                .keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) throws Exception {
                        return v1.f1 > v2.f1 ? v1 : v2;
                    }
                });

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        res.print("");
        env.execute();

    }


}

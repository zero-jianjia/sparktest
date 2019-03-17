package com.zero;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yinchen
 */
public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, '\n', 0);

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] s = value.split(",");
                        for (String i : s) {
                            out.collect(new Tuple2<>(i, 1));
                        }
                    }
                }).keyBy(e -> e.f0).sum(1);
        counts.print();

        System.out.println(env.getExecutionPlan());
        env.execute("WordCount");
    }
}

package com.zero.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author yinchen
 */
public class TumblingWindowsInnerJoinDemo {

    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        long delay = 1000; //ms

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //基于eventTime处理数据
        env.setParallelism(1);

        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource =
                env.addSource(new StreamDataSource()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource =
                env.addSource(new StreamDataSource1()).name("Demo Source1");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> leftStream =
                leftSource.assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element) {
                                System.out.println(" leftStream: " + element.f0 + "---" + element.f2);
                                return element.f2;
                            }
                        }
                                                        );
        DataStream<Tuple3<String, String, Long>> rightStream = rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        System.out.println("rightStream: " + element.f0 + "---" + element.f2);
                        return element.f2;
                    }
                }
                                                                                                        );

        // join 操作
//        leftStream.join(rightStream)
//                .where(new KeySelector<Tuple3<String, String, Long>, Object>() {
//                    @Override
//                    public Object getKey(Tuple3<String, String, Long> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .equalTo(new KeySelector<Tuple3<String, String, Long>, Object>() {
//                    @Override
//                    public Object getKey(Tuple3<String, String, Long> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))  //windowSize秒一个窗口
//                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Object>() {
//                    @Override
//                    public Object join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
//                        return new Tuple6<String, String, Long, String, String, Long>(first.f0, first.f1, first.f2, second.f0, second.f1, second.f2);
//                    }
//                }).print();


        leftStream.coGroup(rightStream)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Object>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> first, Iterable<Tuple3<String, String, Long>> second, Collector<Object> out) throws Exception {
                        for (Tuple3<String, String, Long> leftElem : first) {
                            boolean hadElements = false;
                            for (Tuple3<String, String, Long> rightElem : second) {
                                out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, rightElem.f1, leftElem.f2, rightElem.f2));
                                hadElements = true;
                            }
                            if (!hadElements) {
                                out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, "null", leftElem.f2, -1L));
                            }
                        }
                    }
                })
                .print();

        env.execute("TimeWindowDemo");
    }

}

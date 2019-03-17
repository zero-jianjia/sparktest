package com.zero.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author yinchen
 */
public class StreamDataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {

        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "1", 1000000050000L),
                Tuple3.of("a", "2", 1000000054000L),
                Tuple3.of("a", "3", 1000000079900L),
                Tuple3.of("a", "4", 1000000115000L),
                Tuple3.of("b", "5", 1000000100000L),
                Tuple3.of("b", "6", 1000000108000L)
        };

        int i = 0;
        while (running && i < elements.length) {
            ctx.collect(new Tuple3<>((String) elements[i].f0, (String) elements[i].f1, (Long) elements[i].f2));
            i++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
package com.zero.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author yinchen
 */
public class StreamDataSource1 extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws InterruptedException {

        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "A", 1000000059000L),
                Tuple3.of("b", "B", 1000000105000L),
        };

        int i = 0;
        while (running && i < elements.length) {
            ctx.collect(new Tuple3<>((String) elements[i].f0, (String) elements[i].f1, (long) elements[i].f2));
            i++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
package com.zero.storm.trident.demo.spout;

import com.zero.storm.trident.demo.function.CityAssignment;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DiagnosisEventEmitter implements ITridentSpout.Emitter<Long>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisEventEmitter.class);

    private AtomicInteger successfulTransactions = new AtomicInteger(0);

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        LOG.info("start emit batch, tx={}, coordinatorMeta={}", tx, coordinatorMeta);
        for (int i = 0; i < 10; i++) {
            List<Object> events = new ArrayList<>();
            double lat = (double) (-30 + (int) (Math.random() * 75));
            double lng = (double) (-120 + (int) (Math.random() * 70));
            long time = System.currentTimeMillis();

            String diagnosisCode = Integer.toString(320 + (int) (Math.random() * 7));
            DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diagnosisCode);
            events.add(event);
            collector.emit(events);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void success(TransactionAttempt tx) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {
    }

}

package com.zero.storm.trident.demo2;

import com.zero.storm.trident.demo.spout.DefaultCoordinator;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yinchen
 */
public class NameCountAggregator implements Aggregator<Map<String, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(NameCountAggregator.class);

    private static final long serialVersionUID = -5141558506999420908L;

    @Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        LOG.info("init {}", batchId);
        return new HashMap<>();
    }

    //判断某个名字是否已经存在于map中，若无，则put，若有，则递增
    @Override
    public void aggregate(Map<String, Integer> map, TridentTuple tuple, TridentCollector collector) {
        String key = tuple.getString(0);
        if (map.containsKey(key)) {
            Integer tmp = map.get(key);
            map.put(key, ++tmp);
        }
        else {
            map.put(key, 1);
        }
    }

    //将聚合后的结果emit出去
    @Override
    public void complete(Map<String, Integer> map, TridentCollector collector) {
        if (map.size() > 0) {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                LOG.info("Thread.id={}, | {} | {}", Thread.currentThread().getId(), entry.getKey(), entry.getValue());
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            map.clear();
        }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}

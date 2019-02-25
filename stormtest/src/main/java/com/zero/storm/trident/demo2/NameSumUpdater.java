package com.zero.storm.trident.demo2;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yinchen
 */
public class NameSumUpdater extends BaseStateUpdater<NameSumState> {
    private static final long serialVersionUID = -6108745529419385248L;

    public void updateState(NameSumState state, List<TridentTuple> tuples, TridentCollector collector) {
        Map<String, Integer> map = new HashMap<>();
        for (TridentTuple t : tuples) {
            map.put(t.getString(0), t.getInteger(1));
        }
        state.setBulk(map);
    }
}
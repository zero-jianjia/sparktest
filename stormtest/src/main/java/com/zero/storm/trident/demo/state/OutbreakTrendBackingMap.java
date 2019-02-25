package com.zero.storm.trident.demo.state;

import org.apache.storm.trident.state.map.IBackingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OutbreakTrendBackingMap implements IBackingMap<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(OutbreakTrendBackingMap.class);

    private Map<String, Long> storage = new ConcurrentHashMap<>();

    @Override
    public List<Long> multiGet(List<List<Object>> keys) {
        List<Long> values = new ArrayList<>();
        for (List<Object> key : keys) {
            Long value = storage.get(key.get(0));
            if (value == null) {
                values.add(0L);
            }
            else {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<Long> vals) {
        for (int i = 0; i < keys.size(); i++) {
            LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
            storage.put((String) keys.get(i).get(0), vals.get(i));
        }
    }
}

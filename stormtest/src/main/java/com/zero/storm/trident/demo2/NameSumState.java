package com.zero.storm.trident.demo2;

import org.apache.storm.trident.state.State;

import java.util.HashMap;
import java.util.Map;

public class NameSumState implements State {

    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void beginCommit(Long txid) {

    }

    @Override
    public void commit(Long txid) {

    }

    public void setBulk(Map<String, Integer> map) {
        // 将新到的tuple累加至map中
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String key = entry.getKey();
            if (this.map.containsKey(key)) {
                this.map.put(key, this.map.get(key) + map.get(key));
            } else {
                this.map.put(key, entry.getValue());
            }
        }
        System.out.println("-------");
        // 将map中的当前状态打印出来。
        for (Map.Entry<String, Integer> entry : this.map.entrySet()) {
            String Key = entry.getKey();
            Integer Value = entry.getValue();
            System.out.println(Key + "|" + Value);
        }
    }
}

package com.zero.storm.transaction;

import org.apache.storm.testing.MemoryTransactionalSpout;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author yinchen
 */
public class TransactionalGlobalCount {

    public static void main(String[] args) {
//        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(
//                DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
//        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder(
//                "global-count", "spout", spout, 3);
//        builder.setBolt("partial-count", new BatchCount(), 5)
//                .shuffleGrouping("spout");
//        builder.setBolt("sum", new UpdateGlobalCount())
//                .globalGrouping("partial-count");
    }

}

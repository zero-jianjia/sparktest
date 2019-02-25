package com.zero.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

/**
 * @author yinchen
 */
public class TridentTest {
    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"),
                3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        /* 创建Stream spout1, 分词、统计 */
//        TridentState wordCounts =
        topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
//                        .parallelismHint(6)
                .newValuesStream()
                .each(new Fields("word","count"), new BaseFunction() {
                    @Override
                    public void execute(TridentTuple tuple, TridentCollector collector) {
                        System.out.println(tuple);
                    }
                },new Fields())
        ;


//        /* 创建Stream words，方法名为words，对入参分次，分别获取words 对应count，然后计算和 */
//        topology.newDRPCStream("words", null)
//                .each(new Fields("args"), new Split(), new Fields("word"))
//                .groupBy(new Fields("word"))
//                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
//                .each(new Fields("count"), new FilterNull())
//                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
//
//
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, topology.build());
        Thread.sleep(200000);
        cluster.shutdown();

//        cluster.submitTopology();
//        cluster.submitTopology("WordCount", conf, topology.build());
//        DRPCClient client = new DRPCClient(null, "wonderwoman", 1234);
//        for (int i = 0; i < 100; i++) {
//            try {
//                System.out.println("DRPC Result: " + client.execute("words", "cat the dog jumped"));
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                System.out.println(e.getMessage());
//            }
//        }

    }
}

package com.zero.storm.trident.demo2;

import com.zero.storm.trident.demo.spout.DiagnosisEventSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;

public class Demo2 {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();

        Stream inputStream = topology
                .newStream("tridentStateDemoId", spout)
                .parallelismHint(3)
                .shuffle()
                .parallelismHint(3)
                .each(new Fields("msg"), new Split(), new Fields("name", "age", "title", "tel"))
                .parallelismHint(3)
                .project(new Fields("name")) //不需要发射age、title、tel字段
                .parallelismHint(3)
                .partitionBy(new Fields("name"));

        inputStream
                .partitionAggregate(new Fields("name"), new NameCountAggregator(), new Fields("nameSumKey", "nameSumValue"))
                .partitionPersist(new NameSumStateFactory(), new Fields("nameSumKey", "nameSumValue"), new NameSumUpdater());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, buildTopology());
        Thread.sleep(1000000);
        cluster.shutdown();
    }
}

package com.zero.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BasicDRPCTopology {
    public static class ExclaimBolt extends BaseBasicBolt {//主要需要覆写execute方法和declareoutputfields方法

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String input = tuple.getString(1);
            collector.emit(new Values(tuple.getValue(0), input + "!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }

    }

    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");//实现DRPC模式
        builder.addBolt(new ExclaimBolt(), 3);

        Config conf = new Config();

        if (args == null || args.length == 0) {//本地调用
            LocalDRPC drpc = new LocalDRPC();//本地模拟DRPCSever
            LocalCluster cluster = new LocalCluster();//本地模拟storm集群

            cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));//组装

            for (String word : new String[]{"hello", "goodbye"}) {
                System.out.println("Result for \"" + word + "\": " + drpc.execute("exclamation", word));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {//集群模式
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
        }
    }
}

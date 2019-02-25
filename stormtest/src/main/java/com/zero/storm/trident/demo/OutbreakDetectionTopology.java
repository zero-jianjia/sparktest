package com.zero.storm.trident.demo;

import com.zero.storm.trident.demo.filter.DiseaseFilter;
import com.zero.storm.trident.demo.function.CityAssignment;
import com.zero.storm.trident.demo.function.DispatchAlert;
import com.zero.storm.trident.demo.function.HourAssignment;
import com.zero.storm.trident.demo.function.OutbreakDetector;
import com.zero.storm.trident.demo.spout.DiagnosisEventSpout;
import com.zero.storm.trident.demo.state.OutbreakTrendFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

/**
 * @author yinchen
 */
public class OutbreakDetectionTopology {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        Stream inputStream = topology.newStream("event", spout);

        inputStream
                .each(new Fields("event"), new DiseaseFilter())
                .each(new Fields("event"), new CityAssignment(), new Fields("city"))
                .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))

                .groupBy(new Fields("cityDiseaseHour"))

                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
                .newValuesStream()

                .each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
                .each(new Fields("alert"), new DispatchAlert(), new Fields());
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
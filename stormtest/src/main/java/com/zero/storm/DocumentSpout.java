package com.zero.storm;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * 向后端发射tuple数据流
 */
public class DocumentSpout extends BaseRichSpout {

    //BaseRichSpout是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现

    private SpoutOutputCollector collector;
    private String[] lines = {
            "storm flink blink",
            "spark storm",
            "blink storm",
            "flink spark",
    };

    private int index = 0;

    /**
     * open()方法中是ISpout接口中定义，在Spout组件初始化时被调用。
     * open()接受三个参数:一个包含Storm配置的Map,一个TopologyContext对象，提供了topology中组件的信息,SpoutOutputCollector对象提供发射tuple的方法。
     * 在这个例子中,我们不需要执行初始化,只是简单的存储在一个SpoutOutputCollector实例变量。
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.out.println("DocumentSpout: "+conf.get("NAME"));
        this.collector = collector;
    }

    /**
     * nextTuple()方法是任何Spout实现的核心。
     * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
     */
    public void nextTuple() {
        this.collector.emit(new Values(lines[index]));
        index++;
        if (index >= lines.length) {
            index = 0;
        }
        Utils.sleep(3000); // 模拟等待3s
    }

    /**
     * declareOutputFields是在IComponent接口中定义的，所有Storm的组件（spout和bolt）都必须实现这个接口
     * 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
     *
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义streamId，该id可以用来定义更加复杂的流拓扑结构
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line")); // 告诉组件发出数据流的字段名为 line
    }
}
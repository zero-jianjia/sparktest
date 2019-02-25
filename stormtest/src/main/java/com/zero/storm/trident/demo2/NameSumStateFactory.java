package com.zero.storm.trident.demo2;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class NameSumStateFactory implements StateFactory {

    private static final long serialVersionUID = 8753337648320982637L;

    @Override
    public State makeState(Map arg0, IMetricsContext arg1, int arg2, int arg3) {
        return new NameSumState();
    }
}
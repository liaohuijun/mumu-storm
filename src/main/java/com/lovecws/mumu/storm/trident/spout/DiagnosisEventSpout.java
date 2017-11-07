package com.lovecws.mumu.storm.trident.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: spout
 * @date 2017-11-07 8:54
 */
public class DiagnosisEventSpout implements ITridentSpout<Long> {

    SpoutOutputCollector spoutOutputCollector;

    BatchCoordinator<Long> coordinator = new DefaultCoordinator();
    Emitter<Long> emitter = new DiagnosisEventEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(final String s, final Map map, final TopologyContext topologyContext) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(final String s, final Map map, final TopologyContext topologyContext) {
        return emitter;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }
}

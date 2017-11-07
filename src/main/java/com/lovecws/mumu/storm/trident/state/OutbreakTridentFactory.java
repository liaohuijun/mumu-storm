package com.lovecws.mumu.storm.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 9:03
 */
public class OutbreakTridentFactory implements StateFactory {
    @Override
    public State makeState(final Map map, final IMetricsContext iMetricsContext, final int i, final int i1) {
        return new OutbreakTridentState(new OutbreakTridentBackingMap());
    }
}

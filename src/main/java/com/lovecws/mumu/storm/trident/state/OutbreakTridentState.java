package com.lovecws.mumu.storm.trident.state;

import org.apache.storm.trident.state.map.NonTransactionalMap;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 9:46
 */
public class OutbreakTridentState extends NonTransactionalMap<Long> {
    public OutbreakTridentState(final OutbreakTridentBackingMap backing) {
        super(backing);
    }
}

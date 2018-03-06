package com.lovecws.mumu.storm.diagnosis.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 报错
 * @date 2017-11-07 9:07
 */
public class DispatchAlert extends BaseFunction {
    @Override
    public void execute(final TridentTuple tridentTuple, final TridentCollector tridentCollector) {
        String alert = tridentTuple.getString(0);
        System.out.println("alert:" + alert);
        //System.exit(0);
    }
}

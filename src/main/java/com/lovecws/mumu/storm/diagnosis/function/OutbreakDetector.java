package com.lovecws.mumu.storm.diagnosis.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 9:06
 */
public class OutbreakDetector extends BaseFunction {
    @Override
    public void execute(final TridentTuple tridentTuple, final TridentCollector tridentCollector) {
        System.out.println(tridentTuple);
        String key = tridentTuple.getString(0);
        Long count = tridentTuple.getLong(1);
        if (count > 10000) {
            List<Object> values = new ArrayList<Object>();
            values.add("outbreak detector for key[" + key + "]");
            tridentCollector.emit(values);
        }
    }
}

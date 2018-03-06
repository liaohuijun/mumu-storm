package com.lovecws.mumu.storm.diagnosis.function;

import com.lovecws.mumu.storm.diagnosis.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 城市指定函数
 * @date 2017-11-06 17:06
 */
public class CityAssignment extends BaseFunction {
    String[] citys = new String[]{"bj", "sh", "sz", "gz"};

    @Override
    public void execute(final TridentTuple tridentTuple, final TridentCollector tridentCollector) {
        DiagnosisEvent event = (DiagnosisEvent) tridentTuple.get(0);
        List<Object> values = new ArrayList<Object>();
        values.add(citys[new Random().nextInt(4)]);
        tridentCollector.emit(values);
    }
}

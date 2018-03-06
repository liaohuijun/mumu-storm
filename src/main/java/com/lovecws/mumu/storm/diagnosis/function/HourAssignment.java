package com.lovecws.mumu.storm.diagnosis.function;

import com.lovecws.mumu.storm.diagnosis.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 城市指定函数
 * @date 2017-11-06 17:06
 */
public class HourAssignment extends BaseFunction {

    @Override
    public void execute(final TridentTuple tridentTuple, final TridentCollector tridentCollector) {
        DiagnosisEvent diagnosisEvent = (DiagnosisEvent) tridentTuple.get(0);
        String city = tridentTuple.getValue(1).toString();
        long hour = diagnosisEvent.getTime() / 1000 / 60 / 60;
        String key = city + ":" + diagnosisEvent.getCode() + ":" + hour;

        List<Object> values = new ArrayList<Object>();
        values.add(hour);
        values.add(key);
        tridentCollector.emit(values);
    }
}

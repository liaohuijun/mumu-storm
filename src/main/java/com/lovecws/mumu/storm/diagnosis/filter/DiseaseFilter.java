package com.lovecws.mumu.storm.diagnosis.filter;

import com.lovecws.mumu.storm.diagnosis.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 过滤
 * @date 2017-11-06 17:00
 */
public class DiseaseFilter extends BaseFilter {

    @Override
    public boolean isKeep(final TridentTuple tridentTuple) {
        DiagnosisEvent event = (DiagnosisEvent) tridentTuple.get(0);
        Integer code = Integer.parseInt(event.getCode());
        //只获取320 321 322 三种疾病
        if (code <= 322 && code >= 320) {
            return true;
        }
        return false;
    }
}

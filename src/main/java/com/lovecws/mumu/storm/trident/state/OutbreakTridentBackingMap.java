package com.lovecws.mumu.storm.trident.state;

import org.apache.storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 9:47
 */
public class OutbreakTridentBackingMap implements IBackingMap<Long> {
    Map<String, Long> storage = new ConcurrentHashMap<String, Long>();

    @Override
    public List<Long> multiGet(final List<List<Object>> list) {
        List<Long> values = new ArrayList<Long>();
        for (List<Object> key : list) {
            Long value = storage.get(key.get(0));
            if (value == null) {
                values.add(new Long(0));
            } else {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void multiPut(final List<List<Object>> keys, final List<Long> values) {
        for (int i = 0; i < keys.size(); i++) {
            storage.put(keys.get(i).get(0).toString(), values.get(i));
        }
    }
}

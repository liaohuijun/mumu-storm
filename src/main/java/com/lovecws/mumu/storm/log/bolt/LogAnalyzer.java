package com.lovecws.mumu.storm.log.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日志数据解析
 * @date 2018-03-06 15:44
 */
public class LogAnalyzer extends BaseBasicBolt {
    private static final Logger log = Logger.getLogger("log analyzer");
    private Map<String, Integer> ACCESS_COUNTER = new ConcurrentHashMap<String, Integer>();

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector basicOutputCollector) {
        String key = tuple.getStringByField("typename") + "." + tuple.getStringByField("method");
        Integer value = ACCESS_COUNTER.get(key);
        if (value == null) {
            value = new Integer(0);
        }
        value = value + 1;
        ACCESS_COUNTER.put(key, value);
        log.info(ACCESS_COUNTER);

        basicOutputCollector.emit(Arrays.asList(key, value));
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("method", "counter"));
    }
}

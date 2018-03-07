package com.lovecws.mumu.storm.log.bolt;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日志内容解析 和 日志数据的过滤
 * @date 2018-03-06 15:43
 */
public class LogParserBolt extends BaseBasicBolt {
    private static final Logger log = Logger.getLogger("log parse");

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector basicOutputCollector) {
        Object tupleValue = tuple.getValue(0);
        Map map = JSON.parseObject(tupleValue.toString(), Map.class);
        basicOutputCollector.emit(Arrays.asList(map.get("logtime"), map.get("method"), map.get("name"), map.get("operater"), map.get("parameter"), map.get("result"), map.get("typename"), map.get("usetime")));
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logtime", "method", "name", "operater", "parameter", "result", "typename", "usetime"));
    }

}

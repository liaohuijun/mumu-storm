package com.lovecws.mumu.storm.log.bolt;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日志存储
 * @date 2018-03-06 16:40
 */
public class LogStorageBolt extends BaseBasicBolt {
    private static final Logger log = Logger.getLogger("log storage");

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector basicOutputCollector) {
        log.info(JSON.toJSONString(tuple,true));
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

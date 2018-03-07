package com.lovecws.mumu.storm.log.function;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日志解析函数
 * @date 2018-03-07 10:40
 */
public class LogParserFunction extends BaseFunction{
    private static final Logger log = Logger.getLogger("trident log parse");

    @Override
    public void execute(final TridentTuple tuple, final TridentCollector collector) {
        log.info(JSON.toJSONString(tuple,true));
        Object tupleValue = tuple.getValue(0);

        Map map = JSON.parseObject(tupleValue.toString(), Map.class);
        collector.emit(Arrays.asList(map.get("logtime"), map.get("method"), map.get("name"), map.get("operater"), map.get("parameter"), map.get("result"), map.get("typename"), map.get("usetime")));
    }
}

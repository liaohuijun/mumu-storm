package com.lovecws.mumu.storm.log;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.storm.log.bolt.LogAnalyzerBolt;
import com.lovecws.mumu.storm.log.bolt.LogParserBolt;
import com.lovecws.mumu.storm.log.bolt.LogStorageBolt;
import com.lovecws.mumu.storm.log.function.LogParserFunction;
import com.lovecws.mumu.storm.log.spout.KafkaLogSpout;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日志分析
 * @date 2018-03-06 13:58
 */
public class LogTopology implements Serializable {

    private void startTopology(String topologyName, Map config, StormTopology topology) {
        try {
            String osName = System.getProperty("os.name");
            if (osName.contains("Windows")) {
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology("LogTopology-topology", new HashMap(), topology);
            } else {
                StormSubmitter.submitTopology(topologyName, config, topology);
            }
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
            e.printStackTrace();
        }
    }

    public void topology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaLogSpout-topology", new KafkaLogSpout());
        topologyBuilder.setBolt("LogParserBolt", new LogParserBolt()).shuffleGrouping("KafkaLogSpout-topology");
        topologyBuilder.setBolt("LogStorageBolt", new LogStorageBolt()).globalGrouping("LogParserBolt");
        topologyBuilder.setBolt("LogAnalyzerBolt", new LogAnalyzerBolt()).globalGrouping("LogParserBolt");
        startTopology("LogTopology-topology", new HashMap(), topologyBuilder.createTopology());
    }

    public void trident() {
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("KafkaLogSpout-trident", new KafkaLogSpout())
                .each(new Fields("log"), new LogParserFunction(), new Fields("logtime", "method", "name", "operater", "parameter", "result", "typename", "usetime"))
                .groupBy(new Fields("typename", "method"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .map(new MapFunction() {
                    @Override
                    public Values execute(final TridentTuple input) {
                        System.out.println(JSON.toJSONString(input, true));
                        return new Values(input.getValues());
                    }
                });
        startTopology("LogTopology-trident", new HashMap(), tridentTopology.build());
    }

    public void mapTrident() {
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("KafkaLogSpout-mapTrident", new KafkaLogSpout())
                .flatMap(new FlatMapFunction() {
                    @Override
                    public Iterable<Values> execute(final TridentTuple input) {
                        List<Values> valuesList = new ArrayList<Values>();
                        for (Object tupleValue : input.getValues()) {
                            Map tupleMap = JSON.parseObject(tupleValue.toString(), Map.class);
                            System.out.println(JSON.toJSONString(tupleMap, true));
                            valuesList.add(new Values(tupleMap.get("logtime"), tupleMap.get("method"), tupleMap.get("name"), tupleMap.get("operater"), tupleMap.get("parameter"), tupleMap.get("result"), tupleMap.get("typename"), tupleMap.get("usetime")));
                        }
                        return valuesList;
                    }
                }, new Fields("logtime", "method", "name", "operater", "parameter", "result", "typename", "usetime"))
                .groupBy(new Fields("typename", "method"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream();
        startTopology("LogTopology-mapTrident", new HashMap(), tridentTopology.build());
    }

    public static void main(String[] args) {
        new LogTopology().topology();
        //new LogTopology().trident();
        //new LogTopology().mapTrident();
    }
}

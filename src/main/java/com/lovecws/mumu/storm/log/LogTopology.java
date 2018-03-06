package com.lovecws.mumu.storm.log;

import com.lovecws.mumu.storm.log.bolt.LogAnalyzer;
import com.lovecws.mumu.storm.log.bolt.LogParser;
import com.lovecws.mumu.storm.log.bolt.LogStorage;
import com.lovecws.mumu.storm.log.spout.KafkaLogSpout;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 日志分析
 * @date 2018-03-06 13:58
 */
public class LogTopology {

    public void topology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaLogSpout", new KafkaLogSpout());
        topologyBuilder.setBolt("LogParser", new LogParser()).shuffleGrouping("KafkaLogSpout");
        topologyBuilder.setBolt("LogStorage", new LogStorage()).globalGrouping("LogParser");
        topologyBuilder.setBolt("LogAnalyzer", new LogAnalyzer()).globalGrouping("LogParser");
        StormTopology topology = topologyBuilder.createTopology();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("LogTopology", new HashMap(), topology);
    }

    public static void main(String[] args) {
        new LogTopology().topology();
    }
}

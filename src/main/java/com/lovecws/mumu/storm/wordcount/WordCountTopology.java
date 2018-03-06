package com.lovecws.mumu.storm.wordcount;

import com.lovecws.mumu.storm.wordcount.blot.WordCounter;
import com.lovecws.mumu.storm.wordcount.blot.WordNormalizer;
import com.lovecws.mumu.storm.wordcount.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public void topology() {
        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalizer", new Fields("word"));
        StormTopology topology = builder.createTopology();

        //配置
        Config conf = new Config();
        conf.put("fileName", "/words.txt");
        conf.setDebug(false);

        //运行拓扑
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountTopology", conf, topology);
    }

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        new WordCountTopology().topology();
    }
}
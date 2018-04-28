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
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;

import java.util.Timer;
import java.util.TimerTask;

public class WordCountTopology {

    public void topology() {
        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader(), 2).setNumTasks(2);
        builder.setBolt("word-normalizer", new WordNormalizer(), 4).shuffleGrouping("word-reader").setNumTasks(4);
        builder.setBolt("word-counter", new WordCounter(), 6).fieldsGrouping("word-normalizer", new Fields("word")).setNumTasks(6);
        StormTopology topology = builder.createTopology();

        //配置
        Config conf = new Config();
        conf.put("fileName", "/words.txt");
        conf.setDebug(false);
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(10000);

        //运行拓扑
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountTopology", conf, topology);
    }

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        new Thread(new Runnable() {
            @Override
            public void run() {
                new Timer().scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println(Thread.currentThread().getName()+"timer task");
                    }
                },10000,1000);
            }
        }).start();
        new WordCountTopology().topology();
    }
}
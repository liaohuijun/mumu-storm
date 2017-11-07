package com.lovecws.mumu.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 11:07
 */
public class WordCountTrident {

    public static void main(String[] args) {
        TridentTopology tridentTopology = new TridentTopology();
        FixedBatchSpout fixedBatchSpout = new FixedBatchSpout(new Fields("line"), 3, Arrays.asList("lovecws", "babymm", "baby", "mumu"));
        tridentTopology.newStream("word-reader-stream", fixedBatchSpout)
                .each(new Fields("line"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields("sum"))
                .newValuesStream();
        StormTopology stormTopology = tridentTopology.build();

        Config conf = new Config();
        conf.put("fileName", "/words.txt");
        conf.setDebug(false);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("WordCountTrident", conf, stormTopology);
    }
}

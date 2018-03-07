package com.lovecws.mumu.storm.log.spout;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kafka spot
 * @date 2018-03-06 14:02
 */
public class KafkaLogSpout extends BaseRichSpout {

    private SpoutOutputCollector collector = null;
    private KafkaConsumer consumer = null;

    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.11.25:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaQuickStartConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<Integer, String>(props);
    }

    @Override
    public void nextTuple() {
        consumer.subscribe(Collections.singleton("mmsnsPortalLogTopic"));
        ConsumerRecords<Integer, String> records = consumer.poll(100);
        for (ConsumerRecord<Integer, String> record : records) {
            collector.emit(Arrays.asList(record.value()), UUID.randomUUID().toString().replace("-", ""));
        }
    }

    @Override
    public void ack(final Object msgId) {
        System.out.println("ack:" + msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(final Object msgId) {
        System.out.println("fail:" + msgId);
        super.fail(msgId);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    @Override
    public void close() {
        consumer.close();
    }
}

package com.lovecws.mumu.storm.log.spout;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.11.25:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.132.33:9092,172.31.132.34:9092,172.31.132.35:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DNS_gjStorm_Analyse_RE");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "5242880");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList("DNS_gjStorm_Analyse_RE_test"));
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<Integer, String> records = consumer.poll(10);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(record.value());
            //collector.emit(Arrays.asList(record.value()), UUID.randomUUID().toString().replace("-", ""));
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

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.11.25:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.132.33:9092,172.31.132.34:9092,172.31.132.35:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DNS_gjStorm_Analyse_RE_test");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer<Integer, String>(props);
        for (int i = 0; i < 10; i++) {
            Future future = kafkaProducer.send(new ProducerRecord("DNS_gjStorm_Analyse_RE_test", i, "lovecws" + DateFormatUtils.format(new Date(),"yyyyMMddHHmmss")));
            try {
                System.out.println(future.get().toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        kafkaProducer.close();
    }
}

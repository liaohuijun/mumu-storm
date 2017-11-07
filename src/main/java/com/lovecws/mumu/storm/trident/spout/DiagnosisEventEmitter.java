package com.lovecws.mumu.storm.trident.spout;

import com.lovecws.mumu.storm.trident.DiagnosisEvent;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 9:16
 */
public class DiagnosisEventEmitter implements ITridentSpout.Emitter<Long>, Serializable {

    private AtomicInteger atomicInteger = new AtomicInteger();

    @Override
    public void emitBatch(final TransactionAttempt transactionAttempt, final Long aLong, final TridentCollector tridentCollector) {
        try {
            TimeUnit.SECONDS.sleep(10);
            System.out.println("\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10000; i++) {
            List<Object> list = new ArrayList<Object>();
            double lat = new Double(-30 + (int) (Math.random() * 75));
            double lng = new Double(-120 + (int) (Math.random() * 70));
            long time = System.currentTimeMillis() + new Random().nextInt(1000000);
            String code = String.valueOf(320 + new Random().nextInt(7));
            list.add(new DiagnosisEvent(lng, lat, time, code));
            tridentCollector.emit(list);
        }
    }

    @Override
    public void success(final TransactionAttempt transactionAttempt) {
        atomicInteger.incrementAndGet();
    }

    @Override
    public void close() {

    }
}

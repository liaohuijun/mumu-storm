package com.lovecws.mumu.storm.diagnosis.spout;

import org.apache.storm.trident.spout.ITridentSpout;

import java.io.Serializable;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-11-07 9:15
 */
public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>,Serializable{
    @Override
    public Long initializeTransaction(final long l, final Long aLong, final Long x1) {
        return null;
    }

    @Override
    public void success(final long l) {

    }

    @Override
    public boolean isReady(final long l) {
        return true;
    }

    @Override
    public void close() {

    }
}

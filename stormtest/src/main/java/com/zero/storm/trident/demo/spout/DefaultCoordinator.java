package com.zero.storm.trident.demo.spout;

import org.apache.storm.trident.spout.ITridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
        LOG.info("Initializing Transaction [" + txid + "]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful Transaction [" + txid + "]");
    }
}

package com.dummy;

import org.apache.zookeeper.recipes.lock.LockListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LockCallback implements LockListener {

    private static final Logger logger = LoggerFactory.getLogger(LockCallback.class);
    private String dir;

    public LockCallback(String dir) {
        this.dir = dir;
    }

    public void lockAcquired() {
        logger.info("Acquired lock on partition {}", dir);
    }

    public void lockReleased() {
        // logger.info("Released lock (or locking request) on partition {}", dir);
    }

}

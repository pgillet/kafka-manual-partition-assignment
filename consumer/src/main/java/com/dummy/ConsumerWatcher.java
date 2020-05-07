package com.dummy;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWatcher.class);

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info(watchedEvent.toString());
    }
}

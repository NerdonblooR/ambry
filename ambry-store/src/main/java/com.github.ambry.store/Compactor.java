package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by weichen on 10/29/16.
 */
public class Compactor implements Runnable {

    final AtomicBoolean running = new AtomicBoolean(true);

    private final StoreMetrics metrics;
    private final String dataDir;
    private final Log log;
    private final PersistentIndex index;
    private final StoreKeyFactory factory;

    private final Object lock = new Object();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public Compactor(StoreConfig config, StoreMetrics metrics, String dataDir, Log log, PersistentIndex index,
                     StoreKeyFactory factory) {
            this.metrics = metrics;
            this.dataDir = dataDir;
            this.log = log;
            this.index = index;
            this.factory = factory;
        }

    @Override
    public void run() {
        try {
            while (running.get()) {
                try {
                    doCompaction();
                } catch (StoreException e) {
                    if (e.getErrorCode() != StoreErrorCodes.Store_Shutting_Down) {
                        logger.error("Caught store exception: ", e);
                    } else {
                        logger.trace("Caught exception during compaction", e);
                    }
                }
            }
        } finally {}
    }

    boolean doCompaction()
            throws StoreException {
            try {
                //Lock Blob
                //Create a new Log
                //Copy messages over
                //Recreate index
                //Replace old log file
                //Unlock
            } catch (Exception e) {
                throw e;
            } finally {
            }
        return false;
    }
}

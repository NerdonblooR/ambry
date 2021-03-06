/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {

    private Log log;
    private PersistentIndex index;
    private final String dataDir;
    private final Scheduler scheduler;
    private Logger logger = LoggerFactory.getLogger(getClass());
    /* A lock that prevents concurrent writes to the log */
    private Object lock = new Object();
    private boolean started;
    private StoreConfig config;
    private long capacityInBytes;
    private static String LockFile = ".lock";
    private FileLock fileLock;
    private StoreKeyFactory factory;
    private MessageStoreRecovery recovery;
    private MessageStoreHardDelete hardDelete;
    private StoreMetrics metrics;
    private Time time;
    private BlobCompactor compactor;
    //metric for scheduling compaction
    private AtomicLong deletedBytes;
    // threshold of deleted blob size in bytes to trigger compaction
    //compaction only triggered when current deleted size larger than this threshold
    private long compactThreshold;
    // threshold of get rates within last minute,
    // compaction only triggered when current rate smaller than this threshold
    private double hotnessThreshold;
    private final boolean enableHotnessAwareCompaction = true;

    private String sID;


    public BlobStore(String storeId, StoreConfig config, Scheduler scheduler, MetricRegistry registry, String dataDir,
                     long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
                     Time time) {
        this.metrics = new StoreMetrics(storeId, registry);
        this.dataDir = dataDir;
        this.scheduler = scheduler;
        this.config = config;
        this.capacityInBytes = capacityInBytes;
        this.factory = factory;
        this.recovery = recovery;
        this.hardDelete = hardDelete;
        this.time = time;
        this.compactThreshold = (long) (config.storeCompactionThreshold * capacityInBytes);
        this.hotnessThreshold = config.storeHotnessThreshold;
        this.sID = storeId;
    }

    @Override
    public void start()
            throws StoreException {
        synchronized (lock) {
            if (started) {
                throw new StoreException("Store already started", StoreErrorCodes.Store_Already_Started);
            }
            final Timer.Context context = metrics.storeStartTime.time();
            try {
                // Check if the data dir exist. If it does not exist, create it
                File dataFile = new File(dataDir);
                if (!dataFile.exists()) {
                    logger.info("Store : {} data directory not found. creating it", dataDir);
                    boolean created = dataFile.mkdir();
                    if (!created) {
                        throw new StoreException("Failed to create directory for data dir " + dataDir,
                                StoreErrorCodes.Initialization_Error);
                    }
                }
                if (!dataFile.isDirectory() || !dataFile.canRead()) {
                    throw new StoreException(dataFile.getAbsolutePath() + " is either not a directory or is not readable",
                            StoreErrorCodes.Initialization_Error);
                }

                // lock the directory
                fileLock = new FileLock(new File(dataDir, LockFile));
                if (!fileLock.tryLock()) {
                    throw new StoreException("Failed to acquire lock on file " + dataDir +
                            ". Another process or thread is using this directory.", StoreErrorCodes.Initialization_Error);
                }
                log = new Log(dataDir, capacityInBytes, metrics);
                index = new PersistentIndex(dataDir, scheduler, log, config, factory, recovery, hardDelete, metrics, time);
                // set the log end offset to the recovered offset from the index after initializing it
                log.setLogEndOffset(index.getCurrentEndOffset());
                metrics.initializeCapacityUsedMetric(log, capacityInBytes);
                compactor = new BlobCompactor();
                started = true;
            } catch (Exception e) {
                throw new StoreException("Error while starting store for dir " + dataDir, e,
                        StoreErrorCodes.Initialization_Error);
            } finally {
                this.deletedBytes = getDeletedSizeInByte();
                context.stop();
            }
        }
    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions)
            throws StoreException {
        checkStarted();
        // allows concurrent gets
        final Timer.Context context = metrics.getResponse.time();
        try {
            List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>(ids.size());
            Map<StoreKey, MessageInfo> indexMessages = new HashMap<StoreKey, MessageInfo>(ids.size());
            for (StoreKey key : ids) {
                BlobReadOptions readInfo = index.getBlobReadInfo(key, storeGetOptions);
                readOptions.add(readInfo);
                indexMessages.put(key, readInfo.getMessageInfo());
            }

            MessageReadSet readSet = log.getView(readOptions);
            // We ensure that the metadata list is ordered with the order of the message read set view that the
            // log provides. This ensures ordering of all messages across the log and metadata from the index.
            List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>(readSet.count());
            for (int i = 0; i < readSet.count(); i++) {
                messageInfoList.add(indexMessages.get(readSet.getKeyAt(i)));
            }
            return new StoreInfo(readSet, messageInfoList);
        } catch (StoreException e) {
            throw e;
        } catch (IOException e) {
            throw new StoreException("IO error while trying to fetch blobs from store " + dataDir, e,
                    StoreErrorCodes.IOError);
        } catch (Exception e) {
            throw new StoreException("Unknown exception while trying to fetch blobs from store " + dataDir, e,
                    StoreErrorCodes.Unknown_Error);
        } finally {
            context.stop();
        }
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite)
            throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.putResponse.time();
        try {
            if (messageSetToWrite.getMessageSetInfo().size() == 0) {
                throw new IllegalArgumentException("Message write set cannot be empty");
            }
            long indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
            // if any of the keys already exist in the store, we fail
            for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
                if (index.findKey(info.getStoreKey()) != null) {
                    throw new StoreException("Key already exists in store", StoreErrorCodes.Already_Exist);
                }
            }

            synchronized (lock) {
                // Validate that log end offset was not changed. If changed, check once again for existing
                // keys in store
                long currentIndexEndOffset = index.getCurrentEndOffset();
                if (currentIndexEndOffset != indexEndOffsetBeforeCheck) {
                    FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
                    for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
                        if (index.findKey(info.getStoreKey(), fileSpan) != null) {
                            throw new StoreException("Key already exists on filespan check", StoreErrorCodes.Already_Exist);
                        }
                    }
                }
                long writeStartOffset = log.getLogEndOffset();
                messageSetToWrite.writeTo(log);
                logger.trace("Store : {} message set written to log", dataDir);
                List<MessageInfo> messageInfo = messageSetToWrite.getMessageSetInfo();
                ArrayList<IndexEntry> indexEntries = new ArrayList<IndexEntry>(messageInfo.size());
                for (MessageInfo info : messageInfo) {
                    IndexValue value = new IndexValue(info.getSize(), writeStartOffset, (byte) 0, info.getExpirationTimeInMs());
                    IndexEntry entry = new IndexEntry(info.getStoreKey(), value);
                    indexEntries.add(entry);
                    writeStartOffset += info.getSize();
                }
                FileSpan fileSpan = new FileSpan(indexEntries.get(0).getValue().getOffset(), log.getLogEndOffset());
                index.addToIndex(indexEntries, fileSpan);
                logger.trace("Store : {} message set written to index ", dataDir);
            }
        } catch (StoreException e) {
            throw e;
        } catch (IOException e) {
            throw new StoreException("IO error while trying to put blobs to store " + dataDir, e, StoreErrorCodes.IOError);
        } catch (Exception e) {
            throw new StoreException("Unknown error while trying to put blobs to store " + dataDir, e,
                    StoreErrorCodes.Unknown_Error);
        } finally {
            context.stop();
            //getBlobOffset();
        }
    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete)
            throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.deleteResponse.time();
        try {
            List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
            long indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
            long dBytes = deletedBytes.get();
            for (MessageInfo info : infoList) {
                IndexValue value = index.findKey(info.getStoreKey());
                if (value == null) {
                    throw new StoreException("Cannot delete id " + info.getStoreKey() + " since it is not present in the index.",
                            StoreErrorCodes.ID_Not_Found);
                } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
                    throw new StoreException(
                            "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                            StoreErrorCodes.ID_Deleted);
                }
                //increment deleted size
                dBytes += value.getSize();
            }
            synchronized (lock) {
                long currentIndexEndOffset = index.getCurrentEndOffset();
                if (indexEndOffsetBeforeCheck != currentIndexEndOffset) {
                    FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
                    dBytes = deletedBytes.get();
                    for (MessageInfo info : infoList) {
                        IndexValue value = index.findKey(info.getStoreKey(), fileSpan);
                        if (value != null && value.isFlagSet(IndexValue.Flags.Delete_Index)) {
                            throw new StoreException(
                                    "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                                    StoreErrorCodes.ID_Deleted);
                        }
                        //increment deleted size
                        dBytes += value.getSize();
                    }
                }
                long writeStartOffset = log.getLogEndOffset();
                messageSetToDelete.writeTo(log);
                logger.trace("Store : {} delete mark written to log", dataDir);
                for (MessageInfo info : infoList) {
                    FileSpan fileSpan = new FileSpan(writeStartOffset, writeStartOffset + info.getSize());
                    index.markAsDeleted(info.getStoreKey(), fileSpan);
                    writeStartOffset += info.getSize();
                }
                logger.trace("Store : {} delete has been marked in the index ", dataDir);

                if (shouldTriggerCompaction(dBytes)) {
                    scheduler.scheduleHelper("Do compaction", compactor, 0, 0, null, true);
                } else {
                    deletedBytes.set(dBytes);
                }

            }
        } catch (StoreException e) {
            throw e;
        } catch (IOException e) {
            throw new StoreException("IO error while trying to delete blobs from store " + dataDir, e,
                    StoreErrorCodes.IOError);
        } catch (Exception e) {
            throw new StoreException("Unknown error while trying to delete blobs from store " + dataDir, e,
                    StoreErrorCodes.Unknown_Error);
        } finally {
            context.stop();
            //compactor.compactLog();
        }
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries)
            throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.findEntriesSinceResponse.time();
        try {
            return index.findEntriesSince(token, maxTotalSizeOfEntries);
        } finally {
            context.stop();
        }
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys)
            throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.findMissingKeysResponse.time();
        try {
            return index.findMissingKeys(keys);
        } finally {
            context.stop();
        }
    }

    @Override
    public boolean isKeyDeleted(StoreKey key)
            throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.isKeyDeletedResponse.time();
        try {
            IndexValue value = index.findKey(key);
            if (value == null) {
                throw new StoreException("Key " + key + " not found in store. Cannot check if it is deleted",
                        StoreErrorCodes.ID_Not_Found);
            }
            return value.isFlagSet(IndexValue.Flags.Delete_Index);
        } finally {
            context.stop();
        }
    }

    @Override
    public long getSizeInBytes() {
        return log.getLogEndOffset();
    }

    @Override
    public void shutdown()
            throws StoreException {
        synchronized (lock) {
            checkStarted();
            try {
                logger.info("Store : " + dataDir + " shutting down");
                index.close();
                log.close();
                started = false;
            } catch (Exception e) {
                logger.error("Store : " + dataDir + " shutdown of store failed for directory ", e);
            } finally {
                try {
                    fileLock.destroy();
                } catch (IOException e) {
                    logger.error("Store : " + dataDir + " IO Exception while trying to close the file lock", e);
                }
            }
        }
    }

    private void checkStarted()
            throws StoreException {
        if (!started) {
            throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
        }
    }

    /**
     * perform index scan for offset of all blobs stored in the partition
     *
     * @return Map{String:BlobID, Long:Offset} contains all Blob Ids and their offsets in a store
     */
    public Map<String, Long> getBlobOffset() {
        //better to key offset sorted
        Map<String, Long> offsetMap = new HashMap<String, Long>();
        for (IndexSegment entry : index.indexes.values()) {
            for (Map.Entry<StoreKey, IndexValue> subEntry : entry.index.entrySet()) {
                offsetMap.put(subEntry.getKey().getID(), subEntry.getValue().getOriginalMessageOffset());
                //For debugging
                String id = subEntry.getKey().getID();
                long size = subEntry.getValue().getSize();
                long offset = subEntry.getValue().getOffset();
                long originalOffset = subEntry.getValue().getOriginalMessageOffset();
                System.out.println("ID: " + String.valueOf(id) + " size: " + String.valueOf(size) + " offset: " +
                        String.valueOf(offset) + " originalOffset: " + String.valueOf(originalOffset));
            }
        }
        return offsetMap;
    }


    /**
     * get the total size of deleted blobs in the partition
     */
    private AtomicLong getDeletedSizeInByte() {
        long logEndOffset = log.getLogEndOffset();
        long totalSize = 0;
        for (IndexSegment entry : index.indexes.values()) {
            for (Map.Entry<StoreKey, IndexValue> subEntry : entry.index.entrySet()) {
                totalSize += subEntry.getValue().getSize();
            }
        }
        return new AtomicLong(logEndOffset - totalSize);
    }


    private boolean shouldTriggerCompaction(long deletedBytes) {
        double readRate = metrics.getResponse.getOneMinuteRate();
        double writeRate = metrics.putResponse.getOneMinuteRate();
        if (readRate + writeRate > hotnessThreshold){
            System.out.println("Store " + sID + ": " + "Partition is Hot, Defer the compaction...");
        }
        return (!enableHotnessAwareCompaction || (readRate + writeRate < hotnessThreshold)) && (deletedBytes > compactThreshold);
    }


    class BlobCompactor implements Runnable {
        private static final String Compacted_Log_File_Name = "log_compacted";

        public void compactLog() {
            //Lock Blob
            synchronized (lock) {
                //Create a new Log
                try {
                    logger.info("Store : BEGIN COMPACTION");
                    long startTime = System.currentTimeMillis();
                    //Copy messages over
                    EnumSet<StoreGetOptions> storeGetOptions = EnumSet.allOf(StoreGetOptions.class);
                    List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>();
                    for (Map.Entry<Long, IndexSegment> entry : index.indexes.entrySet()) {
                        for (StoreKey key : entry.getValue().index.keySet()) {
                            BlobReadOptions readInfo = index.getBlobReadInfo(key, storeGetOptions, false);
                            readOptions.add(readInfo);
                        }
                    }
                    MessageReadSet readSet = log.getView(readOptions);

                    ArrayList<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
                    Log compactedLog = new Log(dataDir, capacityInBytes, metrics, Compacted_Log_File_Name);
                    long writeStartOffset = 0;
                    for (int i = 0; i < readSet.count(); i++) {
                        MessageInfo info = readOptions.get(i).getMessageInfo();
                        IndexValue value = new IndexValue(info.getSize(), writeStartOffset, (byte) 0, info.getExpirationTimeInMs());
                        if (info.isDeleted()) {
                            value.setFlag(IndexValue.Flags.Delete_Index);
                        }
                        IndexEntry entry = new IndexEntry(info.getStoreKey(), value);
                        indexEntries.add(entry);

                        String id = entry.getKey().getID();
                        long size = entry.getValue().getSize();
                        long offset = entry.getValue().getOffset();
                        long originalOffset = entry.getValue().getOriginalMessageOffset();
                        //System.out.println("WRITING OFFSET " + String.valueOf(writeStartOffset) + " ID: " + String.valueOf(id) + " size: " + String.valueOf(size) + " offset: " +
                        //String.valueOf(offset) + " originalOffset: " + String.valueOf(originalOffset));

                        writeStartOffset += readSet.writeTo(i, compactedLog.getFileChannel(), 0, readSet.sizeInBytes(i));
                    }

                    FileSpan fileSpan = new FileSpan(indexEntries.get(0).getValue().getOffset(), compactedLog.getLogEndOffset());
                    index.addToIndex(indexEntries, fileSpan, false);

                    compactedLog.close();

                    File logFile = log.getFile();
                    File compactedLogFile = compactedLog.getFile();
                    String logPath = logFile.getAbsolutePath();


                    String tempFileName = "tmp";
                    File tempFile = new File(dataDir, tempFileName);
                    logFile.renameTo(tempFile);
                    compactedLogFile.renameTo(new File(logPath));
                    tempFile.delete();
                    log.refresh();
                    log.setLogEndOffset(writeStartOffset);
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    logger.info("Store : END COMPACTION");
                    System.out.println("Store " + sID + ": " +
                            "Compaction runtime: " + String.valueOf(elapsedTime) + "ms");

                } catch (Exception e) {

                    System.out.println("Caught exception for compaction: " + e.getStackTrace());

                }
                //clear deleted byte size
                deletedBytes.set(0);

            }

        }

        @Override
        public void run() {

            compactLog();

        }
    }
}
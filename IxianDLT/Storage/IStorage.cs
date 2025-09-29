// Copyright (C) 2017-2025 Ixian
// This file is part of Ixian DLT - www.github.com/ixian-platform/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using DLT.Meta;
using IXICore;
using IXICore.Meta;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace DLT
{
    namespace Storage
    {
        public abstract class IStorage
        {
            protected string pathBase;
            // Threading
            private Thread thread = null;
            protected bool running = false;
            private ThreadLiveCheck TLC;
            private long lastCleanupPass = Clock.getTimestamp();

            protected enum QueueStorageCode
            {
                insertTransaction,
                insertBlock,
                updateTxAppliedFlag

            }
            protected struct QueueStorageMessage
            {
                public QueueStorageCode code;
                public int retryCount;
                public object data;
            }

            // Maintain a queue of sql statements
            protected readonly List<QueueStorageMessage> queueStatements = new List<QueueStorageMessage>();

            protected IStorage(string dataFolderBlocks)
            {
                pathBase = dataFolderBlocks;
            }


            public virtual bool prepareStorage()
            {
                running = true;
                if (!prepareStorageInternal())
                {
                    running = false;
                    return false;
                }
                // Start thread
                TLC = new ThreadLiveCheck();
                thread = new Thread(new ThreadStart(threadLoop));
                thread.Name = "Storage_Thread";
                thread.Start();

                return true;
            }

            public virtual void stopStorage()
            {
                running = false;
            }
            protected virtual void threadLoop()
            {
                QueueStorageMessage active_message = new QueueStorageMessage();

                bool pending_statements = false;

                while (running || pending_statements == true)
                {
                    bool message_found = false;
                    pending_statements = false;
                    TLC.Report();
                    try
                    {
                        lock (queueStatements)
                        {
                            int statements_count = queueStatements.Count();
                            if (statements_count > 0)
                            {
                                if (statements_count > 1)
                                {
                                    pending_statements = true;
                                }
                                QueueStorageMessage candidate = queueStatements[0];
                                active_message = candidate;
                                message_found = true;
                            }
                        }

                        if (message_found)
                        {
                            if (active_message.code == QueueStorageCode.insertTransaction)
                            {
                                insertTransactionInternal((Transaction)active_message.data);
                            }
                            else if (active_message.code == QueueStorageCode.insertBlock)
                            {
                                insertBlockInternal((Block)active_message.data);
                            }
                            lock (queueStatements)
                            {
                                queueStatements.RemoveAt(0);
                            }
                        }
                        else
                        {
                            long cur_time = Clock.getTimestamp();
                            if (cur_time - lastCleanupPass > 60)
                            {
                                lastCleanupPass = cur_time;
                                // this is only enabled on Rocks for now
                                if (this is RocksDBStorage)
                                {
                                    cleanupCache();
                                }
                            }
                            // Sleep for 50ms to yield CPU schedule slot
                            Thread.Sleep(50);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occurred in storage thread loop: " + e);
                        if (message_found)
                        {
                            debugDumpCrashObject(active_message);
                            active_message.retryCount += 1;
                            if(active_message.retryCount > 10)
                            {
                                lock (queueStatements)
                                {
                                    queueStatements.RemoveAt(0);
                                }
                                Logging.error("Too many retries, aborting...");
                                shutdown();
                                throw new Exception("Too many storage retries. Aborting storage thread.");
                            }
                        }
                    }
                    Thread.Yield();
                }
                shutdown();
                Logging.info("Storage stopped.");
            }

            private void debugDumpCrashObject(QueueStorageMessage message)
            {
                Logging.error("Crashed on message: (code: {0}, retry count: {1})", message.code.ToString(), message.retryCount);
                if (message.retryCount == 1 || message.retryCount >= 10)
                {
                    if (message.code == QueueStorageCode.insertBlock)
                    {
                        debugDumpCrashBlock((Block)message.data);
                    }
                    else if (message.code == QueueStorageCode.insertTransaction)
                    {
                        debugDumpCrashTX((Transaction)message.data);
                    }
                    else
                    {
                        Logging.error("Message is 'updateTXAppliedFlag'.");
                    }
                }
            }

            private void debugDumpCrashBlock(Block b)
            {
                Logging.error("Block #{0}, checksum: {1}.", b.blockNum, Base58Check.Base58CheckEncoding.EncodePlain(b.blockChecksum));
                Logging.error("Transactions: {0}, signatures: {1}, timestamp: {2}.", b.transactions.Count, b.signatures.Count, b.timestamp);
                Logging.error("Complete block: {0}", Base58Check.Base58CheckEncoding.EncodePlain(b.getBytes()));
            }

            private void debugDumpCrashTX(Transaction tx)
            {
                Logging.error("Transaction {0}, checksum: {1}", tx.getTxIdString(), Base58Check.Base58CheckEncoding.EncodePlain(tx.checksum));
                Logging.error("Type: {0}, amount: {1}", tx.type, tx.amount);
                Logging.error("Complete transaction: {0}", Base58Check.Base58CheckEncoding.EncodePlain(tx.getBytes(true, true)));
            }

            public virtual bool redactBlockStorage(ulong removeBlocksBelow)
            {
                // Only redact on non-history nodes
                if (Config.storeFullHistory == true)
                {
                    return false;
                }

                ulong lowestBlock = getLowestBlockInStorage();
                for (ulong b = lowestBlock; b < removeBlocksBelow; b++)
                {
                    removeBlock(b);
                }
                return true;
            }
    

            public virtual int getQueuedQueryCount()
            {
                lock (queueStatements)
                {
                    return queueStatements.Count;
                }
            }

            public virtual void insertBlock(Block block)
            {
                if (this is RocksDBStorage)
                {
                    insertBlockInternal(block);
                    return;
                }
                // Make a copy of the block for the queue storage message processing
                QueueStorageMessage message = new QueueStorageMessage
                {
                    code = QueueStorageCode.insertBlock,
                    retryCount = 0,
                    data = new Block(block)
                };

                lock (queueStatements)
                {
                    queueStatements.Add(message);
                }
            }


            public virtual void insertTransaction(Transaction transaction)
            {
                if (this is RocksDBStorage)
                {
                    insertTransactionInternal(transaction);
                    return;
                }
                // Make a copy of the transaction for the queue storage message processing
                QueueStorageMessage message = new QueueStorageMessage
                {
                    code = QueueStorageCode.insertTransaction,
                    retryCount = 0,
                    data = new Transaction(transaction)
                };

                lock (queueStatements)
                {

                    queueStatements.Add(message);
                }
            }

            // Used when on-disk storage must be upgraded
            public virtual bool needsUpgrade() { return false; }
            public virtual bool isUpgrading() { return false; }
            public virtual int upgradePercentage() { return 0; }
            public virtual ulong upgradeBlockNum() { return 0; }
            //
            // Insert
            protected abstract bool insertBlockInternal(Block block);
            protected abstract bool insertTransactionInternal(Transaction transaction);
            //
            public abstract ulong getLowestBlockInStorage();
            public abstract ulong getHighestBlockInStorage();
            // Get - Block
            /// <summary>
            /// Retrieves a Block by its block height from the underlying storage (database).
            /// </summary>
            /// <param name="blocknum">Block height of the block you wish to retrieve.</param>
            /// <returns>Null if the Block does not exist in storage.</returns>
            public abstract Block getBlock(ulong blocknum);
            public abstract byte[] getBlockBytes(ulong blocknum, bool asBlockHeader);
            // Get - Transaction
            /// <summary>
            /// Retrieves a Transaction by its txid.
            /// </summary>
            /// <param name="txid">Transaction ID of the required Transaction.</param>
            /// <param name="block_num">Block height of the Block where the Transaction can be found.</param>
            /// <returns>Null if this transaction can't be found in storage.</returns>
            public abstract Transaction getTransaction(byte[] txid, ulong block_num);
            public abstract byte[] getTransactionBytes(byte[] txid, ulong block_num);
            /// <summary>
            /// Retrieves all Transactions from the specified block.
            /// </summary>
            /// <param name="block_num">Block from which to read Transactions.</param>
            /// <returns>Collection with matching Transactions.</returns>
            public abstract IEnumerable<Transaction> getTransactionsInBlock(ulong block_num, short tx_type = -1);
            public abstract IEnumerable<byte[]> getTransactionsBytesInBlock(ulong block_num, short tx_type = -1);
            //
            // Remove
            public abstract bool removeBlock(ulong block_num);
            public abstract bool removeTransaction(byte[] txid, ulong block_num);

            public abstract (byte[] blockChecksum, IxiNumber totalSignerDifficulty) getBlockTotalSignerDifficulty(ulong blocknum);
            //
            // Prepare and cleanup
            protected abstract bool prepareStorageInternal();
            protected abstract void shutdown();
            protected abstract void cleanupCache();
            public abstract void deleteData();

            private static IStorage autoDetectStorageEngine(string dataFolderBlocks, IMemoryInfoProvider memoryInfoProvider)
            {
                bool hasRocksDatabase = false;

                if (RocksDBStorage.hasRocksDBData(dataFolderBlocks))
                {
                    hasRocksDatabase = true;
                }

                if (!hasRocksDatabase && SQLiteStorage.hasSQLiteData(dataFolderBlocks))
                {
                    Logging.info("Using SQLite.");
                    return new SQLiteStorage(dataFolderBlocks);
                }

                // Default to RocksDB
                Logging.info("Using RocksDB.");
                return new RocksDBStorage(dataFolderBlocks, memoryInfoProvider);
            }

            // instantiation for the proper implementation class
            public static IStorage create(string name, string dataFolderBlocks, IMemoryInfoProvider memoryInfoProvider)
            {
                Logging.info("Block storage provider: {0}", name);
                Logging.info("Available disk space: {0}GB", Platform.getAvailableDiskSpace(dataFolderBlocks) >> 30);
                switch(name)
                {
                    case "Auto": return autoDetectStorageEngine(dataFolderBlocks, memoryInfoProvider);
                    case "SQLite": return new SQLiteStorage(dataFolderBlocks);
                    case "RocksDB": return new RocksDBStorage(dataFolderBlocks, memoryInfoProvider);
                    default: throw new Exception(String.Format("Unknown blocks storage provider: {0}", name));
                }
            }
        }
    }
}

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
using RocksDbSharp;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DLT
{
    namespace Storage
    {
        class RocksDBInternal
        {
            class _storage_Index
            {
                public ColumnFamilyHandle rocksIndexHandle;
                private RocksDb db;
                public _storage_Index(string cf_name, RocksDb db)
                {
                    this.db = db;
                    rocksIndexHandle = db.GetColumnFamily(cf_name);
                }

                public static byte[] combineKeys(ReadOnlySpan<byte> key1, ReadOnlySpan<byte> key2 = default)
                {
                    if (key1.Length > ushort.MaxValue)
                        throw new ArgumentOutOfRangeException(nameof(key1), "Key1 length cannot exceed 65,535 bytes.");

                    if (key2 == default)
                        key2 = ReadOnlySpan<byte>.Empty;

                    int size = 2 + key1.Length + key2.Length;

                    var combined = GC.AllocateUninitializedArray<byte>(size);
                    BinaryPrimitives.WriteUInt16BigEndian(combined, (ushort)key1.Length);
                    key1.CopyTo(combined.AsSpan(2));
                    key2.CopyTo(combined.AsSpan(2 + key1.Length));

                    return combined;
                }

                public void addIndexEntry(ReadOnlySpan<byte> key, ReadOnlySpan<byte> index, ReadOnlySpan<byte> data, WriteBatch writeBatch = null)
                {
                    byte[] keyWithSuffix = combineKeys(key, index);
                    if (writeBatch != null)
                    {
                        writeBatch.Put(keyWithSuffix, data, rocksIndexHandle);
                    }
                    else
                    {
                        db.Put(keyWithSuffix, data, rocksIndexHandle);
                    }
                }

                public void delIndexEntry(ReadOnlySpan<byte> key, ReadOnlySpan<byte> index, WriteBatch writeBatch = null)
                {
                    byte[] keyWithSuffix = combineKeys(key, index);
                    if (writeBatch != null)
                    {
                        writeBatch.Delete(keyWithSuffix, rocksIndexHandle);
                    }
                    else
                    {
                        db.Remove(keyWithSuffix, rocksIndexHandle);
                    }
                }

                public byte[] getEntry(ReadOnlySpan<byte> key, ReadOnlySpan<byte> index)
                {
                    var keyWithSuffix = combineKeys(key, index);
                    return db.Get(keyWithSuffix, rocksIndexHandle);
                }

                public IEnumerable<(ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> value)> getEntriesForKey(ReadOnlyMemory<byte> key,
                                                                                                              ReadOnlyMemory<byte> index = default)
                {
                    var keyWithLen = combineKeys(key.Span, index.Span);
                    var iter = db.NewIterator(rocksIndexHandle);

                    try
                    {
                        for (iter.Seek(keyWithLen); iter.Valid(); iter.Next())
                        {
                            var k = iter.Key();
                            if (!k.AsSpan(0, keyWithLen.Length).SequenceEqual(keyWithLen))
                                yield break;

                            var v = iter.Value();
                            var indexSpan = k.AsMemory().Slice(2 + key.Length);
                            var valueMem = v.AsMemory();

                            yield return (indexSpan, valueMem);
                        }
                    }
                    finally
                    {
                        iter.Dispose();
                    }
                }
            }

            public string dbPath { get; private set; }
            private RocksDb database = null;
            // global column families
            private ColumnFamilyHandle rocksCFBlocks;
            private ColumnFamilyHandle rocksCFTransactions;
            private ColumnFamilyHandle rocksCFMeta;
            // index column families
            // block
            private _storage_Index idxBlocksChecksum;
            // transaction
            private _storage_Index idxTXAppliedType;
            private _storage_Index idxAddressTXs;
            private readonly object rockLock = new object();

            private readonly byte[] BLOCKS_KEY_HEADER = new byte[] { 0 };
            private readonly byte[] BLOCKS_KEY_TXS = new byte[] { 1 };
            private readonly byte[] BLOCKS_KEY_SIGNERS = new byte[] { 2 };
            private readonly byte[] BLOCKS_KEY_SIGNERS_COMPACT = new byte[] { 3 };

            private readonly byte[] BLOCKS_KEY_PRIMARY_INDEX = new byte[] { 0 };

            private readonly byte[] TRANSACTIONS_KEY_TX = new byte[] { 0 };
            //private readonly byte[] TRANSACTIONS_KEY_PROOF = new byte[] { 1 };

            public ulong minBlockNumber { get; private set; }
            public ulong maxBlockNumber { get; private set; }
            public int dbVersion { get; private set; }
            public bool isOpen
            {
                get
                {
                    return database != null;
                }
            }
            public DateTime lastUsedTime { get; private set; }
            // Caches (shared with other rocksDb
            private Cache blockCache = null;

            public RocksDBInternal(string dbPath, Cache blockCache)
            {
                minBlockNumber = 0;
                maxBlockNumber = 0;
                dbVersion = 0;

                this.dbPath = dbPath;
                this.blockCache = blockCache;
            }

            public void openDatabase()
            {
                if (database != null)
                {
                    throw new Exception(String.Format("Rocks Database '{0}' is already open.", dbPath));
                }
                lock (rockLock)
                {
                    var rocksOptions = new DbOptions()
                        .SetCreateIfMissing(true)
                        .SetCreateMissingColumnFamilies(true)
                        .SetKeepLogFileNum(2)
                        .SetMaxLogFileSize(1 * 1024 * 1024)
                        .SetRecycleLogFileNum(10)
                        .IncreaseParallelism(Environment.ProcessorCount)
                        .SetMaxBackgroundCompactions(Environment.ProcessorCount)
                        .SetMaxBackgroundFlushes(Math.Min(4, Environment.ProcessorCount / 2))
                        .SetAllowMmapReads(false)
                        .SetAllowMmapWrites(false)
                        .SetTargetFileSizeBase(256 * 1024 * 1024)
                        .SetTargetFileSizeMultiplier(2)
                        .SetCompression(Compression.Zstd)
                        .SetLevelCompactionDynamicLevelBytes(true)
                        .SetCompactionReadaheadSize(4 * 1024 * 1024);

                    // blocks
                    var blocksBbto = new BlockBasedTableOptions();
                    blocksBbto.SetBlockCache(blockCache.Handle);
                    blocksBbto.SetBlockSize(128 * 1024);
                    blocksBbto.SetCacheIndexAndFilterBlocks(true);
                    blocksBbto.SetPinL0FilterAndIndexBlocksInCache(true);
                    blocksBbto.SetFilterPolicy(BloomFilterPolicy.Create(16, true));
                    blocksBbto.SetWholeKeyFiltering(true);
                    blocksBbto.SetFormatVersion(6);

                    // transactions
                    var txBbto = new BlockBasedTableOptions();
                    txBbto.SetBlockCache(blockCache.Handle);
                    txBbto.SetBlockSize(64 * 1024);
                    txBbto.SetCacheIndexAndFilterBlocks(true);
                    txBbto.SetPinL0FilterAndIndexBlocksInCache(true);
                    txBbto.SetFilterPolicy(BloomFilterPolicy.Create(16, true));
                    txBbto.SetWholeKeyFiltering(true);
                    txBbto.SetFormatVersion(6);

                    // meta
                    var metaBbto = new BlockBasedTableOptions();
                    metaBbto.SetBlockCache(blockCache.Handle);
                    metaBbto.SetBlockSize(4 * 1024);
                    metaBbto.SetCacheIndexAndFilterBlocks(true);
                    metaBbto.SetPinL0FilterAndIndexBlocksInCache(true);
                    metaBbto.SetFilterPolicy(BloomFilterPolicy.Create(14, true));
                    metaBbto.SetWholeKeyFiltering(true);
                    metaBbto.SetFormatVersion(6);

                    // index CFs
                    var blocksIndexBbto = new BlockBasedTableOptions();
                    blocksIndexBbto.SetBlockCache(blockCache.Handle);
                    blocksIndexBbto.SetBlockSize(16 * 1024);
                    blocksIndexBbto.SetCacheIndexAndFilterBlocks(true);
                    blocksIndexBbto.SetPinL0FilterAndIndexBlocksInCache(true);
                    blocksIndexBbto.SetFilterPolicy(BloomFilterPolicy.Create(14, true));
                    blocksIndexBbto.SetWholeKeyFiltering(true);
                    blocksIndexBbto.SetFormatVersion(6);

                    var txIndexBbto = new BlockBasedTableOptions();
                    txIndexBbto.SetBlockCache(blockCache.Handle);
                    txIndexBbto.SetBlockSize(32 * 1024);
                    txIndexBbto.SetCacheIndexAndFilterBlocks(true);
                    txIndexBbto.SetPinL0FilterAndIndexBlocksInCache(true);
                    txIndexBbto.SetFilterPolicy(BloomFilterPolicy.Create(14, true));
                    txIndexBbto.SetWholeKeyFiltering(false);
                    txIndexBbto.SetFormatVersion(6);

                    var columnFamilies = new ColumnFamilies
                    {
                        { "blocks", new ColumnFamilyOptions()
                            .SetBlockBasedTableFactory(blocksBbto)
                            .SetWriteBufferSize(32UL << 20)
                            .SetMaxWriteBufferNumber(2)
                            .SetMinWriteBufferNumberToMerge(1)
                            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(34))
                        },
                        { "transactions", new ColumnFamilyOptions()
                            .SetBlockBasedTableFactory(txBbto)
                            .SetWriteBufferSize(128UL << 20)
                            .SetMaxWriteBufferNumber(4)
                            .SetMinWriteBufferNumberToMerge(2)
                            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(36))
                        },
                        { "meta", new ColumnFamilyOptions()
                            .SetBlockBasedTableFactory(metaBbto)
                            .OptimizeForPointLookup(128)
                            .SetWriteBufferSize(64UL << 10)
                            .SetMaxWriteBufferNumber(1)
                        },
                        { "index_blocks_checksum_meta", new ColumnFamilyOptions()
                            .SetBlockBasedTableFactory(blocksIndexBbto)
                            .SetWriteBufferSize(2UL << 20)
                            .SetMaxWriteBufferNumber(2)
                            .SetMinWriteBufferNumberToMerge(1)
                            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(10))
                        },
                        { "index_tx_applied_type", new ColumnFamilyOptions()
                            .SetBlockBasedTableFactory(txIndexBbto)
                            .SetWriteBufferSize(8UL << 20)
                            .SetMaxWriteBufferNumber(4)
                            .SetMinWriteBufferNumberToMerge(2)
                            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(10))
                        },
                        { "index_address_txs", new ColumnFamilyOptions()
                            .SetBlockBasedTableFactory(txIndexBbto)
                            .SetWriteBufferSize(32UL << 20)
                            .SetMaxWriteBufferNumber(6)
                            .SetMinWriteBufferNumberToMerge(3)
                            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(35))
                        }
                    };

                    database = RocksDb.Open(rocksOptions, dbPath, columnFamilies);

                    // initialize column family handles
                    rocksCFBlocks = database.GetColumnFamily("blocks");
                    rocksCFTransactions = database.GetColumnFamily("transactions");
                    rocksCFMeta = database.GetColumnFamily("meta");

                    // initialize indexes
                    idxBlocksChecksum = new _storage_Index("index_blocks_checksum_meta", database);
                    idxTXAppliedType = new _storage_Index("index_tx_applied_type", database);
                    idxAddressTXs = new _storage_Index("index_address_txs", database);

                    // read initial meta values
                    string versionStr = database.Get("db_version", rocksCFMeta);
                    if (versionStr == null || versionStr == "")
                    {
                        database.Put("db_version", dbVersion.ToString(), rocksCFMeta);
                    }
                    else
                    {
                        dbVersion = int.Parse(versionStr);
                    }

                    byte[] minBlockBytes = database.Get(Encoding.UTF8.GetBytes("min_block"), rocksCFMeta);
                    if (minBlockBytes == null)
                    {
                        minBlockNumber = 0;
                        database.Put(Encoding.UTF8.GetBytes("min_block"), BitConverter.GetBytes(minBlockNumber), rocksCFMeta);
                    }
                    else
                    {
                        minBlockNumber = BitConverter.ToUInt64(minBlockBytes);
                    }

                    byte[] maxBlockBytes = database.Get(Encoding.UTF8.GetBytes("max_block"), rocksCFMeta);
                    if (maxBlockBytes == null)
                    {
                        maxBlockNumber = 0;
                        database.Put(Encoding.UTF8.GetBytes("max_block"), BitConverter.GetBytes(maxBlockNumber), rocksCFMeta);
                    }
                    else
                    {
                        maxBlockNumber = BitConverter.ToUInt64(maxBlockBytes);
                    }

                    Logging.info("RocksDB: Opened Database {0}: Blocks {1} - {2}, version {3}", dbPath, minBlockNumber, maxBlockNumber, dbVersion);
                    Logging.trace("RocksDB: Stats: {0}", database.GetProperty("rocksdb.stats"));
                    lastUsedTime = DateTime.Now;
                }
            }

            public void logStats()
            {
                if (database != null)
                {
                    if (blockCache != null)
                    {
                        Logging.info("RocksDB: Common Cache Bytes Used: {0}", blockCache.GetUsage());
                    }

                    Logging.info("RocksDB: Stats [rocksdb.block-cache-usage] '{0}': {1}", dbPath, database.GetProperty("rocksdb.block-cache-usage"));
                    Logging.info("RocksDB: Stats for '{0}': {1}", dbPath, database.GetProperty("rocksdb.dbstats"));
                }
            }

            public void closeDatabase()
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return;
                    }

                    // free all blocks column families
                    rocksCFBlocks = null;
                    rocksCFMeta = null;
                    rocksCFTransactions = null;

                    // free all indexes
                    idxBlocksChecksum = null;
                    idxTXAppliedType = null;
                    idxAddressTXs = null;

                    database.Dispose();
                    database = null;
                }
            }

            private byte[] getBlockMetaBytes(int sigCount, IxiNumber totalSignerDifficulty, byte[] powField)
            {
                byte[] sigCountBytes = sigCount.GetIxiVarIntBytes();
                byte[] tsdBytes = totalSignerDifficulty.getBytes().GetIxiBytes();
                byte[] powFieldBytes = powField.GetIxiBytes();
                byte[] blockMetaBytes = new byte[sigCountBytes.Length + tsdBytes.Length + powFieldBytes.Length];
                Buffer.BlockCopy(sigCountBytes, 0, blockMetaBytes, 0, sigCountBytes.Length);
                Buffer.BlockCopy(tsdBytes, 0, blockMetaBytes, sigCountBytes.Length, tsdBytes.Length);
                Buffer.BlockCopy(powFieldBytes, 0, blockMetaBytes, sigCountBytes.Length + tsdBytes.Length, powFieldBytes.Length);

                return blockMetaBytes;
            }

            private (int sigCount, IxiNumber totalSignerDifficulty, byte[] powField) parseBlockMetaBytes(byte[] bytes)
            {
                int offset = 0;
                var iwo = bytes.GetIxiVarUInt(offset);
                int sigCount = (int)iwo.num;
                offset += iwo.bytesRead;

                var bwo = bytes.ReadIxiBytes(offset);
                IxiNumber totalSignerDifficulty = new IxiNumber(bwo.bytes);
                offset += bwo.bytesRead;

                bwo = bytes.ReadIxiBytes(offset);
                byte[] powField = bwo.bytes;
                offset += bwo.bytesRead;

                return (sigCount, totalSignerDifficulty, powField);
            }

            private void updateBlockIndexes(WriteBatch writeBatch, Block sb)
            {
                writeBatch.Put(_storage_Index.combineKeys(sb.blockChecksum, BLOCKS_KEY_TXS), sb.getTransactionIDsBytes(), rocksCFBlocks);
                writeBatch.Put(_storage_Index.combineKeys(sb.blockChecksum, BLOCKS_KEY_SIGNERS), sb.getSignaturesBytes(true, false), rocksCFBlocks);
                writeBatch.Put(_storage_Index.combineKeys(sb.blockChecksum, BLOCKS_KEY_SIGNERS_COMPACT), sb.getSignaturesBytes(true, true), rocksCFBlocks);

                var blockNumBytes = sb.blockNum.GetBytesBE();

                byte[] blockMetaBytes = getBlockMetaBytes(sb.getFrozenSignatureCount(), sb.getTotalSignerDifficulty(), sb.powField);
                idxBlocksChecksum.addIndexEntry(blockNumBytes, sb.blockChecksum, blockMetaBytes, writeBatch);

                idxBlocksChecksum.addIndexEntry(blockNumBytes, BLOCKS_KEY_PRIMARY_INDEX, sb.blockChecksum, writeBatch);
            }

            private static byte[] typeAndTxIDToBytes(short type, ReadOnlySpan<byte> txid)
            {
                byte[] buffer = GC.AllocateUninitializedArray<byte>(2 + txid.Length);
                BinaryPrimitives.WriteInt16BigEndian(buffer.AsSpan(0, 2), type);
                txid.CopyTo(buffer.AsSpan(2));

                return buffer;
            }

            private static byte[] blockHeightAndTxIDToBytes(ulong blockHeight, ReadOnlySpan<byte> txid)
            {
                byte[] buffer = GC.AllocateUninitializedArray<byte>(8 + txid.Length);
                BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(0, 8), blockHeight);
                txid.CopyTo(buffer.AsSpan(8));

                return buffer;
            }

            private void updateTXIndexes(WriteBatch writeBatch, Transaction st)
            {
                byte[] tx_id_bytes = st.id;

                foreach (var from in st.fromList)
                {
                    idxAddressTXs.addIndexEntry(new Address(st.pubKey.addressNoChecksum, from.Key).addressNoChecksum, blockHeightAndTxIDToBytes(st.applied, tx_id_bytes), Array.Empty<byte>(), writeBatch);
                }

                foreach (var to in st.toList)
                {
                    idxAddressTXs.addIndexEntry(to.Key.addressNoChecksum, blockHeightAndTxIDToBytes(st.applied, tx_id_bytes), Array.Empty<byte>(), writeBatch);
                }

                idxTXAppliedType.addIndexEntry(st.applied.GetBytesBE(), typeAndTxIDToBytes((short)st.type, tx_id_bytes), Array.Empty<byte>(), writeBatch);
            }

            private void updateMinMax(WriteBatch writeBatch, ulong blockNum)
            {
                if (minBlockNumber == 0 || blockNum < minBlockNumber)
                {
                    minBlockNumber = blockNum;
                    writeBatch.Put(Encoding.UTF8.GetBytes("min_block"), BitConverter.GetBytes(minBlockNumber), rocksCFMeta);
                }
                if (maxBlockNumber == 0 || blockNum > maxBlockNumber)
                {
                    maxBlockNumber = blockNum;
                    writeBatch.Put(Encoding.UTF8.GetBytes("max_block"), BitConverter.GetBytes(maxBlockNumber), rocksCFMeta);
                }
            }

            public bool insertBlock(Block block)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return false;
                    }
                    lastUsedTime = DateTime.Now;
                    using (WriteBatch writeBatch = new WriteBatch())
                    {
                        writeBatch.Put(_storage_Index.combineKeys(block.blockChecksum, BLOCKS_KEY_HEADER), block.getBytes(true, true, true, true, true), rocksCFBlocks);
                        updateBlockIndexes(writeBatch, block);
                        updateMinMax(writeBatch, block.blockNum);
                        database.Write(writeBatch);
                    }
                }
                return true;
            }

            public bool insertTransaction(Transaction transaction)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return false;
                    }
                    lastUsedTime = DateTime.Now;
                    using (WriteBatch writeBatch = new WriteBatch())
                    {
                        writeBatch.Put(_storage_Index.combineKeys(transaction.id, TRANSACTIONS_KEY_TX), transaction.getBytes(true, true), rocksCFTransactions);
                        updateTXIndexes(writeBatch, transaction);
                        database.Write(writeBatch);
                    }
                }
                return true;
            }

            public Block getBlock(ulong blockNum)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    if (blockNum < minBlockNumber || blockNum > maxBlockNumber)
                    {
                        return null;
                    }

                    lastUsedTime = DateTime.Now;

                    var blockNumBytes = blockNum.GetBytesBE();

                    var blockChecksum = idxBlocksChecksum.getEntry(blockNumBytes, BLOCKS_KEY_PRIMARY_INDEX);
                    if (blockChecksum == null)
                    {
                        return null;
                    }

                    return getBlockByHash(blockChecksum, null);
                }
            }

            public byte[] getBlockBytes(ulong blocknum, bool asBlockHeader)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    if (blocknum < minBlockNumber || blocknum > maxBlockNumber)
                    {
                        return null;
                    }

                    lastUsedTime = DateTime.Now;

                    var blockHash = idxBlocksChecksum.getEntry(blocknum.GetBytesBE(), BLOCKS_KEY_PRIMARY_INDEX);
                    if (blockHash == null)
                    {
                        return null;
                    }

                    byte[] blockBytes = database.Get(_storage_Index.combineKeys(blockHash, BLOCKS_KEY_HEADER), rocksCFBlocks);
                    if (blockBytes != null)
                    {
                        if (asBlockHeader)
                        {
                            byte[] sigBytes = database.Get(_storage_Index.combineKeys(blockHash, BLOCKS_KEY_SIGNERS_COMPACT), rocksCFBlocks);

                            byte[] mergedBytes = new byte[blockBytes.Length + sigBytes.Length];
                            Buffer.BlockCopy(blockBytes, 0, mergedBytes, 0, blockBytes.Length);
                            Buffer.BlockCopy(sigBytes, 0, mergedBytes, blockBytes.Length, sigBytes.Length);
                            return mergedBytes;
                        }
                        else
                        {
                            byte[] sigBytes = database.Get(_storage_Index.combineKeys(blockHash, BLOCKS_KEY_SIGNERS), rocksCFBlocks);
                            byte[] txIDBytes = database.Get(_storage_Index.combineKeys(blockHash, BLOCKS_KEY_TXS), rocksCFBlocks);

                            byte[] mergedBytes = new byte[blockBytes.Length + sigBytes.Length + txIDBytes.Length];
                            Buffer.BlockCopy(blockBytes, 0, mergedBytes, 0, blockBytes.Length);
                            Buffer.BlockCopy(sigBytes, 0, mergedBytes, blockBytes.Length, sigBytes.Length);
                            Buffer.BlockCopy(txIDBytes, 0, mergedBytes, blockBytes.Length + sigBytes.Length, txIDBytes.Length);
                            return mergedBytes;
                        }
                    }
                    return null;
                }
            }

            public Block getBlockByHash(ReadOnlySpan<byte> checksum)
            {
                if (database == null)
                {
                    return null;
                }

                lastUsedTime = DateTime.Now;
                return getBlockByHash(checksum, null);
            }

            private Block getBlockByHash(ReadOnlySpan<byte> checksum, ReadOnlySpan<byte> blockMetaBytes)
            {
                lock (rockLock)
                {
                    byte[] blockBytes = database.Get(_storage_Index.combineKeys(checksum, BLOCKS_KEY_HEADER), rocksCFBlocks);
                    if (blockBytes != null)
                    {
                        byte[] txIDsBytes = database.Get(_storage_Index.combineKeys(checksum, BLOCKS_KEY_TXS), rocksCFBlocks);
                        Block b = new Block(checksum.ToArray(), blockBytes, txIDsBytes);
                        (int sigCount, IxiNumber totalSignerDifficulty, byte[] powField) blockMeta;
                        if (blockMetaBytes != null)
                        {
                            blockMeta = parseBlockMetaBytes(blockMetaBytes.ToArray());
                        }
                        else
                        {
                            blockMeta = parseBlockMetaBytes(idxBlocksChecksum.getEntry(b.blockNum.GetBytesBE(), b.blockChecksum));
                        }

                        b.totalSignerDifficulty = blockMeta.totalSignerDifficulty;
                        b.powField = blockMeta.powField;
                        b.signatureCount = blockMeta.sigCount;

                        byte[] sigBytes = database.Get(_storage_Index.combineKeys(b.blockChecksum, BLOCKS_KEY_SIGNERS), rocksCFBlocks);
                        if (sigBytes == null)
                        {
                            sigBytes = database.Get(_storage_Index.combineKeys(b.blockChecksum, BLOCKS_KEY_SIGNERS_COMPACT), rocksCFBlocks);
                        }
                        b.setSignaturesFromBytes(sigBytes, false);
                        b.fromLocalStorage = true;
                        return b;
                    }
                    return null;
                }
            }


            private Transaction getTransactionInternal(ReadOnlySpan<byte> txid)
            {
                var tx_bytes = getTransactionBytesInternal(txid);
                if (tx_bytes != null)
                {
                    Transaction t = new Transaction(txid.ToArray(), tx_bytes);
                    t.fromLocalStorage = true;
                    return t;
                }
                return null;
            }

            private byte[] getTransactionBytesInternal(ReadOnlySpan<byte> txid)
            {
                lastUsedTime = DateTime.Now;
                return database.Get(_storage_Index.combineKeys(txid, TRANSACTIONS_KEY_TX), rocksCFTransactions);
            }

            public Transaction getTransaction(byte[] txid)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    return getTransactionInternal(txid);
                }
            }

            public byte[] getTransactionBytes(byte[] txid)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return null;
                    }
                    return getTransactionBytesInternal(txid);
                }
            }

            public IEnumerable<Transaction> getTransactionsByAddress(byte[] addr, ulong blockNum = 0)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    IEnumerable<(ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> value)> entries;
                    if (blockNum == 0)
                    {
                        entries = idxAddressTXs.getEntriesForKey(addr);
                    }
                    else
                    {
                        entries = idxAddressTXs.getEntriesForKey(addr, blockNum.GetBytesBE());
                    }
                    foreach (var i in entries)
                    {
                        txs.Add(getTransactionInternal(i.index.Span.Slice(8)));
                    }
                    return txs;
                }
            }

            public IEnumerable<Transaction> getTransactionsInBlock(ulong blockNum, short txType = -1)
            {
                lock (rockLock)
                {
                    List<Transaction> txs = new List<Transaction>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    IEnumerable<(ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> value)> entries;
                    if (txType == -1)
                    {
                        entries = idxTXAppliedType.getEntriesForKey(blockNum.GetBytesBE());
                    }
                    else
                    {
                        entries = idxTXAppliedType.getEntriesForKey(blockNum.GetBytesBE(), txType.GetBytesBE());
                    }

                    foreach (var txid in entries)
                    {
                        var tx = getTransactionInternal(txid.index.Span.Slice(2));
                        txs.Add(tx);
                    }
                    return txs;
                }
            }

            public IEnumerable<byte[]> getTransactionsBytesInBlock(ulong blockNum, short txType = -1)
            {
                lock (rockLock)
                {
                    List<byte[]> txs = new List<byte[]>();
                    if (database == null)
                    {
                        return null;
                    }
                    lastUsedTime = DateTime.Now;
                    IEnumerable<(ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> value)> entries;
                    if (txType == -1)
                    {
                        entries = idxTXAppliedType.getEntriesForKey(blockNum.GetBytesBE());
                    }
                    else
                    {
                        entries = idxTXAppliedType.getEntriesForKey(blockNum.GetBytesBE(), txType.GetBytesBE());
                    }

                    foreach (var txid in entries)
                    {
                        var tx = getTransactionBytesInternal(txid.index.Span.Slice(2));
                        txs.Add(tx);
                    }
                    return txs;
                }
            }

            public bool removeBlock(ulong blockNum)
            {
                lock (rockLock)
                {
                    byte[] blockChecksum = getBlockTotalSignerDifficulty(blockNum).blockChecksum;
                    if (blockChecksum != null)
                    {
                        lastUsedTime = DateTime.Now;
                        var blockNumBytes = blockNum.GetBytesBE();

                        // Delete all transactions applied on this block height
                        foreach (var tx_id_bytes in idxTXAppliedType.getEntriesForKey(blockNumBytes))
                        {
                            removeTransactionInternal(tx_id_bytes.index.Span.Slice(2));
                        }

                        using (WriteBatch writeBatch = new WriteBatch())
                        {
                            writeBatch.Delete(_storage_Index.combineKeys(blockChecksum, BLOCKS_KEY_HEADER), rocksCFBlocks);
                            writeBatch.Delete(_storage_Index.combineKeys(blockChecksum, BLOCKS_KEY_SIGNERS), rocksCFBlocks);
                            writeBatch.Delete(_storage_Index.combineKeys(blockChecksum, BLOCKS_KEY_SIGNERS_COMPACT), rocksCFBlocks);
                            writeBatch.Delete(_storage_Index.combineKeys(blockChecksum, BLOCKS_KEY_TXS), rocksCFBlocks);

                            idxBlocksChecksum.delIndexEntry(blockNumBytes, blockChecksum, writeBatch);

                            idxBlocksChecksum.delIndexEntry(blockNumBytes, BLOCKS_KEY_PRIMARY_INDEX, writeBatch);

                            database.Write(writeBatch);
                        }
                        return true;
                    }
                    return false;
                }
            }

            private bool removeTransactionInternal(ReadOnlySpan<byte> tx_id_bytes)
            {
                Transaction tx = getTransactionInternal(tx_id_bytes);
                if (tx != null)
                {
                    using (WriteBatch writeBatch = new WriteBatch())
                    {
                        writeBatch.Delete(_storage_Index.combineKeys(tx_id_bytes, TRANSACTIONS_KEY_TX), rocksCFTransactions);

                        // remove it from indexes
                        foreach (var f in tx.fromList.Keys)
                        {
                            idxAddressTXs.delIndexEntry(new Address(tx.pubKey.addressNoChecksum, f).addressNoChecksum, blockHeightAndTxIDToBytes(tx.applied, tx_id_bytes), writeBatch);
                        }
                        foreach (var t in tx.toList.Keys)
                        {
                            idxAddressTXs.delIndexEntry(t.addressNoChecksum, blockHeightAndTxIDToBytes(tx.applied, tx_id_bytes), writeBatch);
                        }
                        idxTXAppliedType.delIndexEntry(tx.applied.GetBytesBE(), typeAndTxIDToBytes((short)tx.type, tx_id_bytes), writeBatch);

                        database.Write(writeBatch);
                    }
                    return true;
                }
                return false;
            }

            public bool removeTransaction(byte[] txid)
            {
                lock (rockLock)
                {
                    lastUsedTime = DateTime.Now;
                    var tx_id_bytes = txid;
                    return removeTransactionInternal(tx_id_bytes);
                }
            }

            public (byte[] blockChecksum, IxiNumber totalSignerDifficulty) getBlockTotalSignerDifficulty(ulong blockNum)
            {
                lock (rockLock)
                {
                    if (database == null)
                    {
                        return (null, null);
                    }
                    lastUsedTime = DateTime.Now;

                    var blockNumBytes = blockNum.GetBytesBE();

                    var blockChecksum = idxBlocksChecksum.getEntry(blockNumBytes, BLOCKS_KEY_PRIMARY_INDEX);
                    if (blockChecksum == null)
                    {
                        return (null, null);
                    }
                    var blockMeta = idxBlocksChecksum.getEntry(blockNumBytes, blockChecksum);
                    if (blockMeta == null)
                    {
                        return (null, null);
                    }
                    return (blockChecksum, parseBlockMetaBytes(blockMeta).totalSignerDifficulty);
                }
            }

            public void compact()
            {
                if (database != null)
                {
                    try
                    {
                        Logging.info("RocksDB: Performing compaction on database '{0}'.", dbPath);
                        lock (rockLock)
                        {
                            database.CompactRange(null, null, rocksCFBlocks);
                            database.CompactRange(null, null, rocksCFTransactions);
                            database.CompactRange(null, null, rocksCFMeta);
                            database.CompactRange(null, null, idxBlocksChecksum.rocksIndexHandle);
                            database.CompactRange(null, null, idxAddressTXs.rocksIndexHandle);
                            database.CompactRange(null, null, idxTXAppliedType.rocksIndexHandle);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("RocksDB: Error while performing regular maintenance on '{0}': {1}", dbPath, e.Message);
                    }
                }
            }
        }

        public class RocksDBStorage : IStorage
        {
            private readonly Dictionary<ulong, RocksDBInternal> openDatabases = new Dictionary<ulong, RocksDBInternal>();
            public uint closeAfterSeconds = 60;

            private int maxOpenDatabases = 50;
            private long minDiskSpace = 1L * 1024L * 1024L * 1024L;

            // Runtime stuff
            private Cache commonBlockCache = null;
            private Queue<RocksDBInternal> reopenCleanupList = new Queue<RocksDBInternal>();
            private DateTime lastReopenOptimize = DateTime.Now;


            private ulong highestBlockNum = 0;
            private ulong lowestBlockNum = 0;
            private ulong maxDatabaseCache;

            public RocksDBStorage(string dataFolderBlocks, ulong maxDatabaseCache) : base(dataFolderBlocks)
            {
                this.maxDatabaseCache = maxDatabaseCache;
            }

            private RocksDBInternal getDatabase(ulong blockNum, bool onlyExisting = false)
            {
                // Open or create the db which should contain blockNum
                ulong baseBlockNum = blockNum / Config.maxBlocksPerDatabase;
                RocksDBInternal db = null;
                lock (openDatabases)
                {
                    if (openDatabases.ContainsKey(baseBlockNum))
                    {
                        db = openDatabases[baseBlockNum];
                        if (!db.isOpen)
                        {
                            Logging.info("RocksDB: Database {0} is not opened - opening.", baseBlockNum);
                            db.openDatabase();
                        }
                    }
                    else
                    {
                        if (!hasSufficientDiskSpace())
                        {
                            throw new InvalidOperationException("RocksDB: Error opening database, free disk space is below 1GB.");
                        }

                        string db_path = Path.Combine(pathBase, "0000", baseBlockNum.ToString());
                        if (onlyExisting)
                        {
                            if (!Directory.Exists(db_path))
                            {
                                Logging.info("RocksDB: Open of '{0} requested with onlyExisting = true, but it does not exist.", db_path);
                                return null;
                            }
                        }

                        Logging.info("RocksDB: Opening a database for blocks {0} - {1}.", baseBlockNum * Config.maxBlocksPerDatabase, (baseBlockNum * Config.maxBlocksPerDatabase) + Config.maxBlocksPerDatabase - 1);
                        db = new RocksDBInternal(db_path, commonBlockCache);
                        openDatabases.Add(baseBlockNum, db);
                        db.openDatabase();

                        if (openDatabases.Count > maxOpenDatabases)
                        {
                            closeOldestDatabase();
                        }
                    }
                }
                return db;
            }

            public static bool hasRocksDBData(string pathBase)
            {
                if (Directory.Exists(Path.Combine(pathBase, "0000", "0")))
                {
                    return true;
                }
                return false;
            }

            protected override bool prepareStorageInternal()
            {
                // Files structured like:
                //  'pathBase\<startOffset>', where <startOffset> is the nominal lowest block number in that database
                //  the actual lowest block in that database may be higher than <startOffset>
                // <startOffset> is aligned to `maxBlocksPerDB` blocks

                // check that the base path exists, or create it
                if (!Directory.Exists(pathBase))
                {
                    try
                    {
                        Directory.CreateDirectory(pathBase);
                        Directory.CreateDirectory(Path.Combine(pathBase, "0000"));
                    }
                    catch (Exception e)
                    {
                        Logging.error("Unable to prepare block database path '{0}': {1}", pathBase, e.Message);
                        return false;
                    }
                }
                // Prepare cache
                commonBlockCache = Cache.CreateLru(maxDatabaseCache);
                // DB optimization
                if (Config.optimizeDBStorage)
                {
                    Logging.info("RocksDB: Performing pre-start DB compaction and optimization.");
                    foreach (string db in Directory.GetDirectories(Path.Combine(pathBase, "0000")))
                    {
                        Logging.info("RocksDB: Optimizing [{0}].", db);
                        RocksDBInternal temp_db = new RocksDBInternal(db, commonBlockCache);
                        try
                        {
                            temp_db.openDatabase();
                            temp_db.compact();
                            temp_db.closeDatabase();
                        }
                        catch (Exception e)
                        {
                            Logging.warn("RocksDB: Error while opening database {0}: {1}", db, e.Message);
                        }
                    }
                    Logging.info("RocksDB: Pre-start optimnization complete.");
                }

                Logging.info("Last storage block number is: #{0}", getHighestBlockInStorage());

                return true;
            }

            private bool hasSufficientDiskSpace()
            {
                var availSpace = Platform.getAvailableDiskSpace(pathBase);
                if (availSpace == -1)
                {
                    Logging.warn("Could not read available disk space.");
                    return true;
                }
                return availSpace > minDiskSpace;
            }

            protected override void cleanupCache()
            {
                lock (openDatabases)
                {
                    Logging.info("RocksDB Registered database list:");
                    List<ulong> toDrop = new List<ulong>();
                    foreach (var db in openDatabases)
                    {
                        Logging.info("RocksDB: [{0}]: open: {1}, last used: {2}",
                            db.Value.dbPath,
                            db.Value.isOpen,
                            db.Value.lastUsedTime
                            );

                        if (db.Value.isOpen
                            && (DateTime.Now - db.Value.lastUsedTime).TotalSeconds >= closeAfterSeconds)
                        {
                            if (db.Key == getHighestBlockInStorage() / Config.maxBlocksPerDatabase)
                            {
                                // never close the latest database
                                continue;
                            }
                            Logging.info("RocksDB: Closing '{0}' due to inactivity.", db.Value.dbPath);
                            db.Value.closeDatabase();
                            toDrop.Add(db.Key);
                            reopenCleanupList.Enqueue(db.Value);
                        }
                    }

                    foreach (ulong dbnum in toDrop)
                    {
                        openDatabases.Remove(dbnum);
                    }

                    if ((DateTime.Now - lastReopenOptimize).TotalSeconds > 60.0)
                    {
                        int reopenListCount = reopenCleanupList.Count;
                        for (int i = 0; i < reopenListCount; i++)
                        {
                            var db = reopenCleanupList.Dequeue();
                            if (openDatabases.Values.Any(x => x.dbPath == db.dbPath))
                            {
                                Logging.info("RocksDB: Database [{0}] was still in use, skipping until it is closed.", db.dbPath);
                                continue;
                            }

                            Logging.info("RocksDB: Compacting closed database [{0}].", db.dbPath);
                            try
                            {
                                db.openDatabase();
                            }
                            catch (Exception)
                            {
                                // these were attempted too quickly and RocksDB internal still has some pointers open
                                reopenCleanupList.Enqueue(db);
                                Logging.info("RocksDB: Database [{0}] was locked by another process, will try again later.", db.dbPath);
                            }
                            if (db.isOpen)
                            {
                                db.compact();
                                db.closeDatabase();
                                Logging.info("RocksDB: Compacting succeeded");
                            }
                        }
                        lastReopenOptimize = DateTime.Now;
                    }

                    // check disk status and close databases if we're running low
                    bool sufficientDiskSpace = hasSufficientDiskSpace();
                    if (!sufficientDiskSpace && openDatabases.Where(x => x.Value.isOpen).Count() > 0)
                    {
                        Logging.error("RocksDB: Disk free space is low, closing all databases, to prevent data corruption.");
                        closeDatabases();
                    }
                }
            }

            private void closeOldestDatabase()
            {
                var baseBlockHeight = getHighestBlockInStorage() / Config.maxBlocksPerDatabase;
                var oldestDb = openDatabases.OrderBy(x => x.Value.lastUsedTime).Where(x => x.Value.isOpen && x.Key != baseBlockHeight).First();
                oldestDb.Value.closeDatabase();
                openDatabases.Remove(oldestDb.Key);
                reopenCleanupList.Enqueue(oldestDb.Value);
            }

            public override void deleteData()
            {
                Directory.Delete(pathBase, true);
            }

            private void closeDatabases()
            {
                foreach (var db in openDatabases.Values)
                {
                    Logging.info("RocksDB: Shutdown, closing '{0}'", db.dbPath);
                    db.closeDatabase();
                }
            }

            protected override void shutdown()
            {
                lock (openDatabases)
                {
                    closeDatabases();
                }
            }

            public override ulong getHighestBlockInStorage()
            {
                if (highestBlockNum > 0)
                {
                    return highestBlockNum;
                }

                // find our absolute highest block db
                long latest_db = -1;
                foreach (var d in Directory.EnumerateDirectories(Path.Combine(pathBase, "0000")))
                {
                    string[] dir_parts = d.Split(Path.DirectorySeparatorChar);
                    string final_dir = dir_parts[dir_parts.Length - 1];
                    if (long.TryParse(final_dir, out long db_base))
                    {
                        if (db_base > latest_db)
                        {
                            latest_db = db_base;
                        }
                    }
                }
                lock (openDatabases)
                {
                    for (long i = latest_db; i >= 0; i--)
                    {
                        var db = getDatabase((ulong)i * Config.maxBlocksPerDatabase, true);
                        if (db != null && db.maxBlockNumber > 0)
                        {
                            highestBlockNum = db.maxBlockNumber;
                            return highestBlockNum;
                        }
                    }
                    return 0;
                }
            }

            public override ulong getLowestBlockInStorage()
            {
                if (lowestBlockNum > 0)
                {
                    return lowestBlockNum;
                }

                // find our absolute lowest block db
                ulong oldest_db = ulong.MaxValue;
                foreach (var d in Directory.EnumerateDirectories(Path.Combine(pathBase, "0000")))
                {
                    string[] dir_parts = d.Split(Path.DirectorySeparatorChar);
                    string final_dir = dir_parts[dir_parts.Length - 1];
                    if (ulong.TryParse(final_dir, out ulong db_base))
                    {
                        if (db_base < oldest_db)
                        {
                            oldest_db = db_base;
                        }
                    }
                }
                if (oldest_db == ulong.MaxValue)
                {
                    return 0; // empty db
                }
                lock (openDatabases)
                {
                    var db = getDatabase(oldest_db, true);
                    lowestBlockNum = db.minBlockNumber;
                    return lowestBlockNum;
                }
            }

            protected override bool insertBlockInternal(Block block)
            {
                lock (openDatabases)
                {
                    var db = getDatabase(block.blockNum);
                    if (db.insertBlock(block))
                    {
                        if (block.blockNum > getHighestBlockInStorage())
                        {
                            highestBlockNum = block.blockNum;
                        }
                        return true;
                    }
                    return false;
                }
            }

            protected override bool insertTransactionInternal(Transaction transaction)
            {
                lock (openDatabases)
                {
                    var db = getDatabase(transaction.applied);
                    return db.insertTransaction(transaction);
                }
            }

            public override Block getBlock(ulong blockNum)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return null;
                    }
                    var db = getDatabase(blockNum, true);
                    return db.getBlock(blockNum);
                }
            }

            public override byte[] getBlockBytes(ulong blockNum, bool asBlockHeader)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return null;
                    }
                    var db = getDatabase(blockNum, true);
                    return db.getBlockBytes(blockNum, asBlockHeader);
                }
            }

            public override Transaction getTransaction(byte[] txid, ulong blockNum = 0)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();

                    if (blockNum != 0)
                    {
                        if (blockNum > highestBlockNum)
                        {
                            Logging.warn("Tried to get transaction in block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                            return null;
                        }

                        var db = getDatabase(blockNum, true);

                        if (db == null)
                        {
                            throw new Exception(string.Format("Cannot access database for block {0}", blockNum));
                        }

                        return db.getTransaction(txid);
                    }
                    else
                    {
                        bool found = false;
                        ulong db_blocknum = IxiVarInt.GetIxiVarUInt(txid, 1).num;

                        if (db_blocknum == 0)
                        {
                            Logging.error("Invalid txid {0} - generated at block height 0.", Transaction.getTxIdString(txid));
                            return null;
                        }

                        if (db_blocknum > highestBlockNum)
                        {
                            Logging.warn("Tried to get transaction generated on block {0} but the highest block in storage is {1}", db_blocknum, highestBlockNum);
                            return null;
                        }

                        // TODO Improve getRedactedWindowSize(0) with block height helpers to determine block version and correct window size
                        if (highestBlockNum > db_blocknum + ConsensusConfig.getRedactedWindowSize(0))
                        {
                            highestBlockNum = db_blocknum + ConsensusConfig.getRedactedWindowSize(0);
                        }

                        while (!found)
                        {
                            var db = getDatabase(db_blocknum, true);
                            if (db == null)
                            {
                                throw new Exception(string.Format("Cannot access database for block {0}", db_blocknum));
                            }

                            Transaction tx = db.getTransaction(txid);
                            if (tx != null)
                            {
                                return tx;
                            }
                            else
                            {
                                if (db_blocknum + Config.maxBlocksPerDatabase <= highestBlockNum)
                                {
                                    db_blocknum += Config.maxBlocksPerDatabase;
                                }
                                else
                                {
                                    // Transaction not found in any database
                                    return null;
                                }
                            }
                        }
                    }
                    return null;
                }
            }

            public override byte[] getTransactionBytes(byte[] txid, ulong blockNum = 0)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();

                    if (blockNum != 0)
                    {
                        if (blockNum > highestBlockNum)
                        {
                            Logging.warn("Tried to get transaction in block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                            return null;
                        }

                        var db = getDatabase(blockNum, true);
                        if (db == null)
                        {
                            throw new Exception(string.Format("Cannot access database for block {0}", blockNum));
                        }

                        return db.getTransactionBytes(txid);
                    }
                    else
                    {
                        bool found = false;
                        ulong db_blocknum = IxiVarInt.GetIxiVarUInt(txid, 1).num;

                        if (db_blocknum == 0)
                        {
                            Logging.error("Invalid txid {0} - generated at block height 0.", Transaction.getTxIdString(txid));
                            return null;
                        }

                        if (db_blocknum > highestBlockNum)
                        {
                            Logging.warn("Tried to get transaction generated on block {0} but the highest block in storage is {1}", db_blocknum, highestBlockNum);
                            return null;
                        }

                        // TODO Improve getRedactedWindowSize(0) with block height helpers to determine block version and correct window size
                        if (highestBlockNum > db_blocknum + ConsensusConfig.getRedactedWindowSize(0))
                        {
                            highestBlockNum = db_blocknum + ConsensusConfig.getRedactedWindowSize(0);
                        }

                        while (!found)
                        {
                            var db = getDatabase(db_blocknum, true);
                            if (db == null)
                            {
                                throw new Exception(string.Format("Cannot access database for block {0}", db_blocknum));
                            }

                            byte[] tx = db.getTransactionBytes(txid);
                            if (tx != null)
                            {
                                return tx;
                            }
                            else
                            {
                                if (db_blocknum + Config.maxBlocksPerDatabase <= highestBlockNum)
                                {
                                    db_blocknum += Config.maxBlocksPerDatabase;
                                }
                                else
                                {
                                    // Transaction not found in any database
                                    return null;
                                }
                            }
                        }
                    }
                    return null;
                }
            }

            public IEnumerable<Transaction> getTransactionsByAddress(byte[] addr, ulong superBlockNum, ulong blockNum = 0)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return null;
                    }
                    var db = getDatabase(superBlockNum, true);
                    return db.getTransactionsByAddress(addr, blockNum);
                }
            }

            public override IEnumerable<Transaction> getTransactionsInBlock(ulong blockNum, short tx_type = -1)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return null;
                    }
                    var db = getDatabase(blockNum, true);
                    return db.getTransactionsInBlock(blockNum, tx_type);
                }
            }

            public override IEnumerable<byte[]> getTransactionsBytesInBlock(ulong blockNum, short tx_type = -1)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return null;
                    }
                    var db = getDatabase(blockNum, true);
                    return db.getTransactionsBytesInBlock(blockNum, tx_type);
                }
            }

            public override bool removeBlock(ulong blockNum)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return false;
                    }
                    var db = getDatabase(blockNum, true);
                    return db.removeBlock(blockNum);
                }
            }

            public override bool removeTransaction(byte[] txid, ulong blockNum)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return false;
                    }
                    var db = getDatabase(blockNum, true);
                    return db.removeTransaction(txid);
                }
            }

            public override (byte[] blockChecksum, IxiNumber totalSignerDifficulty) getBlockTotalSignerDifficulty(ulong blockNum)
            {
                lock (openDatabases)
                {
                    ulong highestBlockNum = getHighestBlockInStorage();
                    if (blockNum > highestBlockNum)
                    {
                        Logging.warn("Tried to get block {0} but the highest block in storage is {1}", blockNum, highestBlockNum);
                        return (null, null);
                    }
                    var db = getDatabase(blockNum, true);
                    if (db == null)
                    {
                        return (null, null);
                    }
                    return db.getBlockTotalSignerDifficulty(blockNum);
                }
            }
        }
    }
}

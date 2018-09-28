﻿using DLT.Meta;
using DLT.Network;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    public class WsChunk
    {
        public ulong blockNum;
        public int chunkNum;
        public Wallet[] wallets;
    }

    class BlockSync
    {
        public bool synchronizing { get; private set; }
        List<Block> pendingBlocks = new List<Block>();
        readonly List<WsChunk> pendingWsChunks = new List<WsChunk>();

        List<List<WsChunk>> sendingWsChunks = new List<List<WsChunk>>();

        ulong syncTargetBlockNum;
        int maxBlockRequests = 25;
        bool receivedAllMissingBlocks = false;

        ulong wsSyncStartBlock;
        string syncNeighbor;
        HashSet<int> missingWsChunks = new HashSet<int>();

        bool canPerformWalletstateSync = false;


        public BlockSync()
        {
            synchronizing = false;
            receivedAllMissingBlocks = false;
        }

        public void onUpdate()
        {
            if (synchronizing == false) return;
            if (syncTargetBlockNum == 0)
            {
                // we haven't connected to any clients yet
                return;
            }

            Logging.info(String.Format("BlockSync: {0} blocks received, {1} walletstate chunks pending.",
                pendingBlocks.Count, pendingWsChunks.Count));
            if(!Config.recoverFromFile && wsSyncStartBlock == 0)
            {
                startWalletStateSync();
                Thread.Sleep(1000);
                return;
            }
            if (Config.recoverFromFile || wsSyncStartBlock > 0)
            {
                // Request missing blocks if needed
                if (receivedAllMissingBlocks == false)
                {
                    if (requestMissingBlocks())
                    {
                        // If blocks were requested, wait for next iteration
                        return;
                    }
                }
            }
            // Check if we can perform the walletstate synchronization
            if (canPerformWalletstateSync)
            {
                performWalletStateSync();
            }
            else
            {
                // Proceed with rolling forward the chain
                rollForward();
            }
        }

        private bool requestMissingBlocks()
        {
            if (syncTargetBlockNum == 0)
            {
                return false;
            }

            ulong firstBlock = Node.blockChain.redactedWindow > syncTargetBlockNum ? 1 : syncTargetBlockNum - Node.blockChain.redactedWindow + 1;
            ulong lastBlock = syncTargetBlockNum;
            List<ulong> missingBlocks = new List<ulong>(Enumerable.Range(0, (int)(lastBlock - firstBlock + 1)).Select(x => (ulong)x + firstBlock));

            int count = 0;
            lock (pendingBlocks)
            {
                foreach (Block b in pendingBlocks)
                {
                    missingBlocks.RemoveAll(x => x == b.blockNum);
                }

                // whatever is left in missingBlocks is what we need to request
                Logging.info(String.Format("{0} blocks are missing before node is synchronized...", missingBlocks.Count()));
                if (missingBlocks.Count() == 0)
                {
                    receivedAllMissingBlocks = true;
                    return false;
                }

                foreach (ulong blockNum in missingBlocks)
                {
                    // First check if the missing block can be found in storage
                    Block block = Node.blockChain.getBlock(blockNum);
                    if (block != null)
                    {
                        Node.blockSync.onBlockReceived(block);
                        continue;
                    }

                    // Didn't find the block in storage, request it from the network
                    ProtocolMessage.broadcastGetBlock(blockNum);
                    count++;
                    if (count >= maxBlockRequests) break;
                }
            }
            if (count > 0)
                return true;
            return false;
        }

        private void performWalletStateSync()
        {
            Logging.info(String.Format("WS SYNC block: {0}", wsSyncStartBlock));
            if (wsSyncStartBlock > 0)
            {
                Logging.info(String.Format("We are synchronizing to block #{0}.", wsSyncStartBlock));
                requestWalletChunks();
                if (missingWsChunks.Count == 0)
                {
                    Logging.info("All WalletState chunks have been received. Applying");
                    lock (pendingWsChunks)
                    {
                        if (pendingWsChunks.Count > 0)
                        {
                            Node.walletState.clear();
                            foreach (WsChunk c in pendingWsChunks)
                            {
                                Logging.info(String.Format("Applying chunk {0}.", c.chunkNum));
                                Node.walletState.setWalletChunk(c.wallets);
                            }
                            pendingWsChunks.Clear();
                        }
                    }
                }
                else // misingWsChunks.Count > 0
                {
                    return;
                }
                Logging.info(String.Format("Verifying complete walletstate as of block #{0}", wsSyncStartBlock));

                canPerformWalletstateSync = false;
            }
            else // wsSyncStartBlock == 0
            {
                Logging.info("WalletState is already synchronized. Skipping.");
            }
        }

        private void rollForward()
        {
            lock (pendingBlocks)
            {
                ulong lowestBlockNum = 1;
                if (Node.blockChain.redactedWindowSize < syncTargetBlockNum)
                {
                    lowestBlockNum = syncTargetBlockNum - Node.blockChain.redactedWindowSize + 1;
                }
                if (Node.blockChain.Count > 0)
                {
                    pendingBlocks.RemoveAll(x => x.blockNum < Node.blockChain.getLastBlockNum() - 5);
                }

                // Loop until we have no more pending blocks
                // TODO: handle potential edge cases
                while (pendingBlocks.Count() > 0)
                {

                    ulong next_to_apply = Node.blockChain.getLastBlockNum() + 1;
                    if (next_to_apply < lowestBlockNum)
                    {
                        next_to_apply = lowestBlockNum;
                    }

                    Block b = pendingBlocks.Find(x => x.blockNum == next_to_apply);
                    if (b == null)
                    {
                        Logging.info(String.Format("Requesting missing block #{0}", next_to_apply));
                        ProtocolMessage.broadcastGetBlock(next_to_apply);
                        Thread.Sleep(500);
                        return;
                    }



                    ulong targetBlock = next_to_apply - 5;

                    if (targetBlock < lowestBlockNum)
                    {
                        targetBlock = lowestBlockNum;
                    }

                    Block tb = pendingBlocks.Find(x => x.blockNum == targetBlock);
                    if (tb != null)
                    {
                        Node.blockChain.refreshSignatures(tb, true);
                        if (tb.blockChecksum == Node.blockChain.getBlock(tb.blockNum).blockChecksum && Node.blockProcessor.verifyBlockBasic(tb) == BlockVerifyStatus.Valid)
                        {
                            Node.blockProcessor.handleSigFreezedBlock(tb);
                        }
                        pendingBlocks.RemoveAll(x => x.blockNum == tb.blockNum);
                    }


                    Logging.info(String.Format("Applying pending block #{0}. Left to apply: {1}.",
                        b.blockNum, syncTargetBlockNum - Node.blockChain.getLastBlockNum()));

                    // wallet state is correct as of wsConfirmedBlockNumber, so before that we call
                    // verify with a parameter to ignore WS tests, but do all the others
                    BlockVerifyStatus b_status = Node.blockProcessor.verifyBlock(b, !Config.recoverFromFile);

                    if (b_status == BlockVerifyStatus.Indeterminate)
                    {
                        Logging.info(String.Format("Waiting for missing transactions from block #{0}...", b.blockNum));
                        return;
                    }
                    if (b_status == BlockVerifyStatus.Invalid)
                    {
                        Logging.warn(String.Format("Block #{0} is invalid. Discarding and requesting a new one.", b.blockNum));
                        pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                        ProtocolMessage.broadcastGetBlock(b.blockNum);
                        return;
                    }

                    // TODO: carefully verify this
                    // Apply transactions when rolling forward from a recover file without a synced WS
                    if (Config.recoverFromFile)
                    {
                        Node.blockProcessor.applyAcceptedBlock(b);
                        string wsChecksum = Node.walletState.calculateWalletStateChecksum();
                        if (wsChecksum != b.walletStateChecksum)
                        {
                            Logging.error(String.Format("After applying block #{0}, walletStateChecksum is incorrect!. Block's WS: {1}, actualy WS: {2}", b.blockNum, b.walletStateChecksum, wsChecksum));
                            synchronizing = false;
                            return;
                        }
                    }
                    bool sigFreezeCheck = Node.blockProcessor.verifySignatureFreezeChecksum(b);
                    if (Node.blockChain.Count <= 5 || sigFreezeCheck)
                    {
                        Logging.info(String.Format("Appending block #{0} to blockChain.", b.blockNum));
                        //Node.blockProcessor.distributeStakingRewards(b);
                        TransactionPool.setAppliedFlagToTransactionsFromBlock(b); // TODO TODO TODO this is a hack, do it properly
                        Node.blockChain.appendBlock(b);
                    }
                    else if (Node.blockChain.Count > 5 && !sigFreezeCheck)
                    {
                        // invalid sigfreeze, waiting for the correct block
                        return;
                    }

                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);

                }
                if (wsSyncStartBlock > 0)
                {
                    if (Node.blockChain.getLastBlockNum() == wsSyncStartBlock)
                    {
                        verifyLastBlock();
                        return;
                    }
                }
                else if (Node.blockChain.getLastBlockNum() == syncTargetBlockNum)
                {
                    verifyLastBlock();
                    return;
                }
            }
        }

        private void startWalletStateSync()
        {
            HashSet<string> all_neighbors = new HashSet<string>(NetworkClientManager.getConnectedClients().Concat(NetworkServer.getConnectedClients()));
            if (all_neighbors.Count < 1)
            {
                Logging.info(String.Format("Wallet state synchronization from storage."));
                return;
            }

            Random r = new Random();
            syncNeighbor = all_neighbors.ElementAt(r.Next(all_neighbors.Count));
            Console.ForegroundColor = ConsoleColor.Cyan;
            Logging.info(String.Format("Starting wallet state synchronization from {0}", syncNeighbor));
            Console.ResetColor();
            ProtocolMessage.syncWalletStateNeighbor(syncNeighbor);
        }

        // Verify the last block we have
        private bool verifyLastBlock()
        {
            Block b = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
            if(b.walletStateChecksum != Node.walletState.calculateWalletStateChecksum())
            {
                // TODO TODO TODO resync?
                Logging.error(String.Format("Wallet state synchronization failed, last block's WS checksum does not match actual WS Checksum, last block #{0}, wsSyncStartBlock: #{1}, block's WS: {2}, actual WS: {3}", Node.blockChain.getLastBlockNum(), wsSyncStartBlock, b.walletStateChecksum, Node.walletState.calculateWalletStateChecksum()));
                return false;
            }

            stopSyncStartBlockProcessing();

            return true;
        }

        private void stopSyncStartBlockProcessing()
        {
            // if we reach here, we are synchronized
            synchronizing = false;
            syncTargetBlockNum = 0;

            Node.blockProcessor.firstBlockAfterSync = true;
            Node.blockProcessor.resumeOperation();
        }


        private void requestWalletChunks()
        {
            lock(missingWsChunks)
            {
                int count = 0;
                foreach(int c in missingWsChunks)
                {
                    ProtocolMessage.getWalletStateChunkNeighbor(syncNeighbor, c);
                    count += 1;
                    if (count > maxBlockRequests) break;
                }
                Logging.info(String.Format("{0} WalletState chunks are missing before WalletState is synchronized...", missingWsChunks.Count));
            }
        }

        // Called when receiving a walletstate synchronization request
        public bool startOutgoingWSSync(RemoteEndpoint endpoint)
        {
            if(synchronizing == true)
            {
                Logging.warn("Unable to perform outgoing walletstate sync until own blocksync is complete.");
                return false;
            }

            pendingWsChunks.Clear();

            /*   if(pendingWsChunks.Count > 0)
               {
                     Logging.info("Unable to perform outgoing walletstate sync, because another outgoing sync is still in progress.");
                     return false;
               }
               */

            lock (pendingWsChunks)
            {
              //  List<WsChunk> sendingWs = sendingWsChunks.Find()

                pendingWsChunks.AddRange(Node.walletState.getWalletStateChunks(Config.walletStateChunkSplit));
                //sendingWsChunks.Add();
            }
            Logging.info("Started outgoing WalletState Sync.");
            return true;
        }

        public void outgoingSyncComplete()
        {
            lock (pendingWsChunks)
            {
                pendingWsChunks.Clear();
            }
            Logging.info("Outgoing WalletState Sync finished.");
        }

        // passing endpoint through here is an ugly hack, which should be removed once network code is refactored.
        public void onRequestWalletChunk(int chunk_num, RemoteEndpoint endpoint)
        {
            if(synchronizing == true)
            {
                Logging.warn("Neighbor is requesting WalletState chunks, but we are synchronizing!");
                return;
            }
            if (chunk_num >= 0 && chunk_num < pendingWsChunks.Count)
            {
                ProtocolMessage.sendWalletStateChunk(endpoint, pendingWsChunks[chunk_num]);
            } else
            {
                Logging.warn(String.Format("Neighbor requested an invalid WalletState chunk: {0}, but the pending array only has 0-{1}.",
                    chunk_num, pendingWsChunks.Count));
            }
        }

        public void onWalletChunkReceived(WsChunk chunk)
        {
            if(synchronizing == false)
            {
                Logging.warn("Received WalletState chunk, but we are not synchronizing!");
                return;
            }
            lock(missingWsChunks)
            {
                if(missingWsChunks.Contains(chunk.chunkNum))
                {
                    pendingWsChunks.Add(chunk);
                    missingWsChunks.Remove(chunk.chunkNum);
                }
            }
        }

        public void onBlockReceived(Block b)
        {
            if (synchronizing == false) return;
            lock (pendingBlocks)
            {
                // ignore any block num higher than confirmed WS
                if (wsSyncStartBlock > 0 && b.blockNum > wsSyncStartBlock)
                {
                    pendingBlocks.RemoveAll(x => x.blockNum == b.blockNum);
                    return;
                }

                int idx = pendingBlocks.FindIndex(x => x.blockNum == b.blockNum);
                if (idx > -1)
                {
                    pendingBlocks[idx] = b;
                }
                else // idx <= -1
                {
                    pendingBlocks.Add(b);
                }
            }
        }
        
        public void startSync()
        {
            // clear out current state
            lock (pendingBlocks)
            {
                lock (pendingWsChunks)
                {
                    pendingBlocks.Clear();
                    pendingWsChunks.Clear();
                    Node.walletState.clear();
                }
            }
            synchronizing = true;
            // select sync partner for walletstate
            wsSyncStartBlock = 0;
            receivedAllMissingBlocks = false;
        }

        public void onWalletStateHeader(ulong ws_block, long ws_count)
        {
            if(synchronizing == true && wsSyncStartBlock == 0)
            {
                long chunks = ws_count / Config.walletStateChunkSplit;
                if(ws_count % Config.walletStateChunkSplit > 0)
                {
                    chunks += 1;
                }
                Logging.info(String.Format("Starting Wallet State synchronization. Starting block: #{0}. Wallets: {1} ({2} chunks)", 
                    ws_block, ws_count, chunks));
                wsSyncStartBlock = ws_block;
                lock (missingWsChunks)
                {
                    missingWsChunks.Clear();
                    for (int i = 0; i < chunks; i++)
                    {
                        missingWsChunks.Add(i);
                    }
                }

                // We can perform the walletstate sync now
                canPerformWalletstateSync = true;
            }
        }

        public void onHelloDataReceived(ulong block_height, string block_checksum, string walletstate_checksum, int consensus)
        {
            Logging.info("SYNC HEADER DATA");
            Logging.info(string.Format("\t|- Block Height:\t\t#{0}", block_height));
            Logging.info(string.Format("\t|- Block Checksum:\t\t{0}", block_checksum));
            Logging.info(string.Format("\t|- WalletState checksum:\t{0}", walletstate_checksum));
            Logging.info(string.Format("\t|- Currently reported consensus:\t{0}", consensus));

            if (synchronizing)
            {
                if(block_height > syncTargetBlockNum)
                {
                    Logging.info(String.Format("Sync target increased from {0} to {1}.",
                        syncTargetBlockNum, block_height));
                    syncTargetBlockNum = block_height;
                }
            } else
            {
                if(Node.blockProcessor.operating == false)
                {
                    // This should happen when node first starts up.
                    // Node: some of the chain might be loaded from disk, so we might need to walk through that. This case is ignored for now.
                    Logging.info(String.Format("Network synchronization started. Target block height: #{0}.", block_height));

                    // TODO: this can get very slow depending on number of transactions.
                    // Find a better way to handle it
                    Storage.redactBlockStorage(block_height); // Redact block storage if needed

                    syncTargetBlockNum = block_height;
                    startSync();
                }
            }
        }
    }
}

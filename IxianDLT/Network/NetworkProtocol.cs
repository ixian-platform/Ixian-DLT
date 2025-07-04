﻿// Copyright (C) 2017-2025 Ixian
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
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.RegNames;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;

namespace DLT
{
    namespace Network
    {
        public class ProtocolMessage
        {
            // Unified protocol message parsing
            public static void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
            {
                if (endpoint == null)
                {
                    Logging.error("Endpoint was null. parseProtocolMessage");
                    return;
                }

                try
                {
                    switch (code)
                    {
                        case ProtocolMessageCode.hello:
                            handleHello(data, endpoint);
                            break;

                        case ProtocolMessageCode.helloData:
                            handleHelloData(data, endpoint);
                            break;

                        case ProtocolMessageCode.getTransaction3:
                            TransactionProtocolMessages.handleGetTransaction3(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionData2:
                            TransactionProtocolMessages.handleTransactionData2(data, endpoint);
                            break;

                        case ProtocolMessageCode.bye:
                            CoreProtocolMessage.processBye(data, endpoint);
                            break;

                        case ProtocolMessageCode.blockData2:
                            BlockProtocolMessages.handleBlockData2(data, endpoint);
                            break;

                        case ProtocolMessageCode.syncWalletState:
                            WalletStateProtocolMessages.handleSyncWalletState(data, endpoint);
                            break;

                        case ProtocolMessageCode.walletState:
                            WalletStateProtocolMessages.handleWalletState(data, endpoint);
                            break;

                        case ProtocolMessageCode.getWalletStateChunk:
                            WalletStateProtocolMessages.handleGetWalletStateChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.walletStateChunk:
                            WalletStateProtocolMessages.handleWalletStateChunk(data, endpoint);
                            break;

                        case ProtocolMessageCode.updatePresence:
                            PresenceProtocolMessages.handleUpdatePresence(data, endpoint);
                            break;

                        case ProtocolMessageCode.keepAlivePresence:
                            PresenceProtocolMessages.handleKeepAlivePresence(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPresence2:
                            PresenceProtocolMessages.handleGetPresence2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getKeepAlives:
                            CoreProtocolMessage.processGetKeepAlives(data, endpoint);
                            break;

                        case ProtocolMessageCode.keepAlivesChunk:
                            PresenceProtocolMessages.handleKeepAlivesChunk(data, endpoint);
                            break;

                        // return 10 random presences of the selected type
                        case ProtocolMessageCode.getRandomPresences:
                            PresenceProtocolMessages.handleGetRandomPresences(data, endpoint);
                            break;

                        case ProtocolMessageCode.getUnappliedTransactions:
                            TransactionProtocolMessages.handleGetUnappliedTransactions(data, endpoint);
                            break;

                        case ProtocolMessageCode.attachEvent:
                            NetworkEvents.handleAttachEventMessage(data, endpoint);
                            break;

                        case ProtocolMessageCode.detachEvent:
                            NetworkEvents.handleDetachEventMessage(data, endpoint);
                            break;

                        case ProtocolMessageCode.getNextSuperBlock:
                            BlockProtocolMessages.handleGetNextSuperBlock(data, endpoint);
                            break;

                        case ProtocolMessageCode.inventory2:
                            handleInventory2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSignatures2:
                            SignatureProtocolMessages.handleGetSignatures2(data, endpoint);
                            break;

                        case ProtocolMessageCode.signaturesChunk2:
                            SignatureProtocolMessages.handleSignaturesChunk2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getTransactions2:
                            TransactionProtocolMessages.handleGetTransactions2(data, endpoint);
                            break;

                        case ProtocolMessageCode.transactionsChunk3:
                            TransactionProtocolMessages.handleTransactionsChunk3(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockHeaders3:
                            BlockProtocolMessages.handleGetBlockHeaders3(data, endpoint);
                            break;

                        case ProtocolMessageCode.getRelevantBlockTransactions:
                            BlockProtocolMessages.handleGetRelevantBlockTransactions(data, endpoint);
                            break;

                        case ProtocolMessageCode.getPIT2:
                            BlockProtocolMessages.handleGetPIT2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlock3:
                            BlockProtocolMessages.handleGetBlock3(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBalance2:
                            WalletStateProtocolMessages.handleGetBalance2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getBlockSignatures2:
                            {
                                using (MemoryStream m = new MemoryStream(data))
                                {
                                    using (BinaryReader reader = new BinaryReader(m))
                                    {
                                        ulong block_num = reader.ReadIxiVarUInt();

                                        int checksum_len = (int)reader.ReadIxiVarUInt();
                                        byte[] checksum = reader.ReadBytes(checksum_len);

                                        SignatureProtocolMessages.handleGetBlockSignatures2(block_num, checksum, endpoint);
                                    }
                                }
                            }
                            break;

                        case ProtocolMessageCode.blockSignature2:
                            SignatureProtocolMessages.handleBlockSignature2(data, endpoint);
                            break;

                        case ProtocolMessageCode.getNameRecord:
                            handleGetNameRecord(data, endpoint);
                            break;

                        case ProtocolMessageCode.getSectorNodes:
                            handleGetSectorNodes(data, endpoint);
                            break;

                        default:
                            Logging.warn("Unknown protocol message: {0}, from {1} ({2})", code, endpoint.getFullAddress(), endpoint.serverWalletAddress);
                            break;
                    }

                }
                catch (Exception e)
                {
                    Logging.error("Error parsing network message. Details: {0}", e.ToString());
                }
            }

            public static void handleGetSectorNodes(byte[] data, RemoteEndpoint endpoint)
            {
                int offset = 0;
                var addressWithOffset = data.ReadIxiBytes(offset);
                offset += addressWithOffset.bytesRead;

                var maxRelayCountWithOffset = data.GetIxiVarUInt(offset);
                offset += maxRelayCountWithOffset.bytesRead;
                int maxRelayCount = (int)maxRelayCountWithOffset.num;

                if (maxRelayCount > 20)
                {
                    maxRelayCount = 20;
                }

                var relayList = RelaySectors.Instance.getSectorNodes(addressWithOffset.bytes, maxRelayCount);

                CoreProtocolMessage.sendSectorNodes(addressWithOffset.bytes, relayList, endpoint);
            }

            public static void handleGetNameRecord(byte[] data, RemoteEndpoint endpoint)
            {
                int offset = 0;
                var name = data.ReadIxiBytes(offset);
                offset += name.bytesRead;

                List<RegisteredNameDataRecord> nameData = Node.regNameState.getNameData(name.bytes);
                CoreProtocolMessage.sendRegisteredNameRecord(endpoint, name.bytes, nameData);
            }

            public static void handleHello(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        CoreProtocolMessage.processHelloMessageV6(endpoint, reader);
                    }
                }
            }

            public static void handleHelloData(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        if(!CoreProtocolMessage.processHelloMessageV6(endpoint, reader))
                        {
                            return;
                        }
                        char node_type = endpoint.presenceAddress.type;
                        if (node_type != 'M' && node_type != 'H')
                        {
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.expectingMaster, string.Format("Expecting master node."), "", true);
                            return;
                        }

                        ulong last_block_num = reader.ReadIxiVarUInt();

                        int bcLen = (int)reader.ReadIxiVarUInt();
                        byte[] block_checksum = reader.ReadBytes(bcLen);

                        endpoint.blockHeight = last_block_num;

                        if (Node.checkCurrentBlockDeprecation(last_block_num) == false)
                        {
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.deprecated, string.Format("This node deprecated or will deprecate on block {0}, your block height is {1}, disconnecting.", Config.nodeDeprecationBlock, last_block_num), last_block_num.ToString(), true);
                            return;
                        }

                        int block_version = (int)reader.ReadIxiVarUInt();

                        Node.blockProcessor.highestNetworkBlockNum = CoreProtocolMessage.determineHighestNetworkBlockNum();

                        ulong highest_block_height = IxianHandler.getHighestKnownNetworkBlockHeight();
                        if (last_block_num + 15 < highest_block_height)
                        {
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.tooFarBehind, string.Format("Your node is too far behind, your block height is {0}, highest network block height is {1}.", last_block_num, highest_block_height), highest_block_height.ToString(), true);
                            return;
                        }

                        // Process the hello data
                        Node.blockSync.onHelloDataReceived(last_block_num, block_checksum, block_version, null, null, 0, 0, true);
                        endpoint.helloReceived = true;
                        NetworkClientManager.recalculateLocalTimeDifference();
                    }
                }
            }

            static void requestNextBlock(ulong blockNum, byte[] blockHash, RemoteEndpoint endpoint)
            {
                InventoryItemBlock iib = new InventoryItemBlock(blockHash, blockNum);
                PendingInventoryItem pii = InventoryCache.Instance.add(iib, endpoint);
                if (!pii.processed
                    && pii.lastRequested == 0)
                {
                    pii.lastRequested = Clock.getTimestamp();
                    InventoryCache.Instance.processInventoryItem(pii);
                }
            }

            static void handleInventory2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        ulong item_count = reader.ReadIxiVarUInt();
                        if (item_count > (ulong)CoreConfig.maxInventoryItems)
                        {
                            Logging.warn("Received {0} inventory items, max items is {1}", item_count, CoreConfig.maxInventoryItems);
                            item_count = (ulong)CoreConfig.maxInventoryItems;
                        }

                        ulong last_accepted_block_height = IxianHandler.getLastBlockHeight();

                        ulong last_block_height = last_accepted_block_height;
                        if (last_block_height > 0 && Node.blockProcessor.localNewBlock != null)
                        {
                            last_block_height = last_block_height + 1;
                        }

                        ulong network_block_height = IxianHandler.getHighestKnownNetworkBlockHeight();

                        Dictionary<ulong, List<InventoryItemSignature>> sig_lists = new Dictionary<ulong, List<InventoryItemSignature>>();
                        List<InventoryItemKeepAlive> ka_list = new List<InventoryItemKeepAlive>();
                        List<byte[]> tx_list = new List<byte[]>();
                        bool updated_block_height = false;
                        for (ulong i = 0; i < item_count; i++)
                        {
                            ulong len = reader.ReadIxiVarUInt();
                            byte[] item_bytes = reader.ReadBytes((int)len);
                            InventoryItem item = InventoryCache.decodeInventoryItem(item_bytes);
                            if (item.type == InventoryItemTypes.transaction)
                            {
                                PendingTransactions.increaseReceivedCount(item.hash, endpoint.presence.wallet);
                            }
                            PendingInventoryItem pii = InventoryCache.Instance.add(item, endpoint);

                            // First update endpoint blockheights
                            switch (item.type)
                            {
                                case InventoryItemTypes.blockSignature:
                                    var iis = (InventoryItemSignature)item;
                                    if (iis.blockNum > endpoint.blockHeight)
                                    {
                                        endpoint.blockHeight = iis.blockNum;
                                        updated_block_height = true;
                                    }
                                    break;

                                case InventoryItemTypes.block:
                                    var iib = ((InventoryItemBlock)item);
                                    if (iib.blockNum > endpoint.blockHeight)
                                    {
                                        endpoint.blockHeight = iib.blockNum;
                                        updated_block_height = true;
                                    }
                                    break;
                            }

                            if (!pii.processed && pii.lastRequested == 0)
                            {
                                // first time we're seeing this inventory item
                                switch (item.type)
                                {
                                    case InventoryItemTypes.keepAlive:
                                        var iika = (InventoryItemKeepAlive)item;
                                        if (PresenceList.getPresenceByAddress(iika.address) != null)
                                        {
                                            ka_list.Add(iika);
                                            pii.lastRequested = Clock.getTimestamp();
                                        }
                                        else
                                        {
                                            InventoryCache.Instance.processInventoryItem(pii);
                                        }
                                        break;

                                    case InventoryItemTypes.transaction:
                                        tx_list.Add(item.hash);
                                        pii.lastRequested = Clock.getTimestamp();
                                        break;

                                    case InventoryItemTypes.blockSignature:
                                        var iis = (InventoryItemSignature)item;
                                        if (iis.blockNum + 4 < last_accepted_block_height)
                                        {
                                            InventoryCache.Instance.setProcessedFlag(iis.type, iis.hash, true);
                                            continue;
                                        }

                                        if (iis.blockNum + 4 < network_block_height)
                                        {
                                            InventoryCache.Instance.setProcessedFlag(iis.type, iis.hash, true);
                                            requestNextBlock(iis.blockNum, iis.blockHash, endpoint);
                                            continue;
                                        }

                                        if (iis.blockNum > last_block_height)
                                        {
                                            pii.lastRequested = Clock.getTimestamp();
                                            requestNextBlock(iis.blockNum, iis.blockHash, endpoint);
                                            continue;
                                        }

                                        if (!sig_lists.ContainsKey(iis.blockNum))
                                        {
                                            sig_lists.Add(iis.blockNum, new List<InventoryItemSignature>());
                                        }
                                        sig_lists[iis.blockNum].Add(iis);

                                        pii.lastRequested = Clock.getTimestamp();
                                        break;

                                    case InventoryItemTypes.block:
                                        var iib = ((InventoryItemBlock)item);
                                        if (iib.blockNum <= last_accepted_block_height)
                                        {
                                            InventoryCache.Instance.setProcessedFlag(iib.type, iib.hash, true);
                                            continue;
                                        }
                                        requestNextBlock(iib.blockNum, iib.hash, endpoint);
                                        break;

                                    default:
                                        InventoryCache.Instance.processInventoryItem(pii);
                                        break;
                                }
                            }
                        }

                        CoreProtocolMessage.broadcastGetKeepAlives(ka_list, endpoint);

                        if (updated_block_height)
                        {
                            Node.blockProcessor.highestNetworkBlockNum = CoreProtocolMessage.determineHighestNetworkBlockNum();
                            Node.blockSync.determineSyncTargetBlockNum();
                        }

                        if (Node.blockSync.synchronizing)
                        {
                            return;
                        }

                        CoreProtocolMessage.broadcastGetTransactions(tx_list, 0, endpoint);

                        foreach (var sig_list in sig_lists)
                        {
                            SignatureProtocolMessages.broadcastGetSignatures(sig_list.Key, sig_list.Value, endpoint);
                        }
                    }
                }
            }
        }
    }
}
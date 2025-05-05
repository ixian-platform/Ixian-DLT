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
using IXICore.Inventory;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DLT
{
    namespace Network
    {
        class PresenceProtocolMessages
        {
            private static void forwardKeepAlivePresence(byte[] ka_bytes, byte[] hash, Address address, long last_seen, byte[] device_id, char node_type, RemoteEndpoint endpoint)
            {
                if (node_type == 'M' || node_type == 'H')
                {
                    // Send this keepalive to all connected clients
                    CoreProtocolMessage.addToInventory(['M', 'H', 'W', 'R'], new InventoryItemKeepAlive(hash, last_seen, address, device_id), endpoint);
                }
                else
                {
                    // Send this keepalive to all connected non-clients
                    CoreProtocolMessage.addToInventory(['M', 'H', 'W'], new InventoryItemKeepAlive(hash, last_seen, address, device_id), endpoint);
                }

                // Send this keepalive message to all subscribed clients
                CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, address.addressNoChecksum, ProtocolMessageCode.keepAlivePresence, ka_bytes, address.addressNoChecksum, endpoint);
            }

            public static void handleKeepAlivesChunk(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int ka_count = (int)reader.ReadIxiVarUInt();

                        int max_ka_per_chunk = CoreConfig.maximumKeepAlivesPerChunk;
                        if(ka_count > max_ka_per_chunk)
                        {
                            ka_count = max_ka_per_chunk;
                        }

                        for (int i = 0; i < ka_count; i++)
                        {
                            if (m.Position == m.Length)
                            {
                                break;
                            }

                            int ka_len = (int)reader.ReadIxiVarUInt();
                            byte[] ka_bytes = reader.ReadBytes(ka_len);
                            
                            handleKeepAlivePresence(ka_bytes, endpoint);
                        }
                    }
                }
            }

            public static void handleGetPresence2(byte[] data, RemoteEndpoint endpoint)
            {
                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        int walletLen = (int)reader.ReadIxiVarUInt();
                        Address wallet = new Address(reader.ReadBytes(walletLen));
                        Presence p = PresenceList.getPresenceByAddress(wallet);
                        if (p != null)
                        {
                            lock (p)
                            {
                                byte[][] presence_chunks = p.getByteChunks();
                                foreach (byte[] presence_chunk in presence_chunks)
                                {
                                    endpoint.sendData(ProtocolMessageCode.updatePresence, presence_chunk, null);
                                }
                            }
                        }
                        else
                        {
                            // TODO blacklisting point
                            Logging.warn("Node has requested presence information about {0} that is not in our PL.", wallet.ToString());
                        }
                    }
                }
            }

            public static void handleKeepAlivePresence(byte[] data, RemoteEndpoint endpoint)
            {
                Address address = null;
                long last_seen = 0;
                byte[] device_id = null;
                char node_type;

                byte[] hash = CryptoManager.lib.sha3_512sqTrunc(data);

                InventoryCache.Instance.setProcessedFlag(InventoryItemTypes.keepAlive, hash, true);

                bool updated = PresenceList.receiveKeepAlive(data, out address, out last_seen, out device_id, out node_type, endpoint);

                // If a presence entry was updated, broadcast this message again
                if (updated && Node.isMasterNode() && !Node.blockSync.synchronizing)
                {
                    forwardKeepAlivePresence(data, hash, address, last_seen, device_id, node_type, endpoint);
                }
            }

            public static void handleGetRandomPresences(byte[] data, RemoteEndpoint endpoint)
            {
                if (!endpoint.isConnected())
                {
                    return;
                }

                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        char type = reader.ReadChar();

                        List<Presence> presences = PresenceList.getPresencesByType(type);
                        int presence_count = presences.Count();
                        if (presence_count > 10)
                        {
                            Random rnd = new Random();
                            presences = presences.Skip(rnd.Next(presence_count - 10)).Take(10).ToList();
                        }

                        foreach (Presence presence in presences)
                        {
                            byte[][] presence_chunks = presence.getByteChunks();
                            foreach (byte[] presence_chunk in presence_chunks)
                            {
                                endpoint.sendData(ProtocolMessageCode.updatePresence, presence_chunk, null);
                            }
                        }
                    }
                }
            }

            public static void handleUpdatePresence(byte[] data, RemoteEndpoint endpoint)
            {
                // Parse the data and update entries in the presence list
                Presence updated_presence = PresenceList.updateFromBytes(data, Node.blockChain.getMinSignerPowDifficulty(IxianHandler.getLastBlockHeight() + 1, IxianHandler.getLastBlockVersion(), Clock.getNetworkTimestamp()));

                // If a presence entry was updated, broadcast this message again
                if (updated_presence != null)
                {
                    foreach (var pa in updated_presence.addresses)
                    {
                        byte[] hash = CryptoManager.lib.sha3_512sqTrunc(pa.getBytes());
                        var iika = new InventoryItemKeepAlive(hash, pa.lastSeenTime, updated_presence.wallet, pa.device);

                        if (pa.type == 'M' || pa.type == 'H')
                        {
                            // Send this keepalive to all connected clients
                            CoreProtocolMessage.addToInventory(['M', 'H', 'W', 'R'], iika, endpoint);
                        }
                        else
                        {
                            // Send this keepalive to all connected non-clients
                            CoreProtocolMessage.addToInventory(['M', 'H', 'W'], iika, endpoint);
                        }
                    }

                    // Send this keepalive message to all subscribed clients
                    CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, updated_presence.wallet.addressNoChecksum, ProtocolMessageCode.updatePresence, data, updated_presence.wallet.addressNoChecksum, endpoint);
                }
            }
        }
    }
}
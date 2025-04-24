// Copyright (C) 2017-2024 Ixian OU
// This file is part of Ixian DLT - www.github.com/ProjectIxian/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.RegNames;
using IXICore.Streaming;
using System;
using System.Collections.Generic;

namespace UnitTests
{
    public class DummyIxianNode : IxianNode
    {
        public override FriendMessage addMessageWithType(byte[] id, FriendMessageType type, Address wallet_address, int channel, string message, bool local_sender = false, Address sender_address = null, long timestamp = 0, bool fire_local_notification = true, int payable_data_len = 0)
        {
            throw new NotImplementedException();
        }

        public override bool addTransaction(Transaction tx, List<Address> relayNodeAddresses, bool force_broadcast)
        {
            throw new NotImplementedException();
        }

        public override byte[] getBlockHash(ulong blockNum)
        {
            throw new NotImplementedException();
        }

        public override Block getBlockHeader(ulong blockNum)
        {
            throw new NotImplementedException();
        }

        public override ulong getHighestKnownNetworkBlockHeight()
        {
            throw new NotImplementedException();
        }

        public override Block getLastBlock()
        {
            throw new NotImplementedException();
        }

        public override ulong getLastBlockHeight()
        {
            return 1;
        }

        public override int getLastBlockVersion()
        {
            return Block.maxVersion;
        }

        public override IxiNumber getMinSignerPowDifficulty(ulong blockNum, int curBlockVersion, long curBlockTimestamp)
        {
            throw new NotImplementedException();
        }

        public override RegisteredNameRecord getRegName(byte[] name, bool useAbsoluteId)
        {
            throw new NotImplementedException();
        }

        public override long getTimeSinceLastBlock()
        {
            throw new NotImplementedException();
        }

        public override Wallet getWallet(Address id)
        {
            throw new NotImplementedException();
        }

        public override IxiNumber getWalletBalance(Address id)
        {
            throw new NotImplementedException();
        }

        public override bool isAcceptingConnections()
        {
            throw new NotImplementedException();
        }

        public override void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
        {
            throw new NotImplementedException();
        }

        public override bool receivedNewTransaction(Transaction tx)
        {
            throw new NotImplementedException();
        }

        public override void receiveStreamData(byte[] data, RemoteEndpoint endpoint, bool fireLocalNotification)
        {
            throw new NotImplementedException();
        }

        public override byte[] resizeImage(byte[] imageData, int width, int height, int quality)
        {
            throw new NotImplementedException();
        }

        public override void resubscribeEvents()
        {
            throw new NotImplementedException();
        }

        public override void shutdown()
        {
            throw new NotImplementedException();
        }

        public override void triggerSignerPowSolutionFound()
        {
            throw new NotImplementedException();
        }
    }
}

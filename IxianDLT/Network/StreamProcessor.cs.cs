using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Streaming;

namespace DLT.Network
{
    class StreamProcessor : CoreStreamProcessor
    {
        public StreamProcessor(PendingMessageProcessor pendingMessageProcessor, StreamCapabilities streamCapabilites, ISectorProvider sectorProvider) : base(pendingMessageProcessor, streamCapabilites, sectorProvider)
        {
        }

        // Called when receiving S2 data from clients
        public override ReceiveDataResponse? receiveData(byte[] bytes, RemoteEndpoint endpoint, bool fireLocalNotification = true, bool alert = true)
        {
            string endpoint_wallet_string = endpoint.presence.wallet.ToString();
            Logging.trace("Receiving S2 data from {0}", endpoint_wallet_string);

            StreamMessage message = new StreamMessage(bytes);


            ReceiveDataResponse? rdr = base.receiveData(bytes, endpoint, false);
            if (rdr == null)
            {
                return rdr;
            }

            SpixiMessage spixi_message = rdr.spixiMessage;
            Friend friend = rdr.friend;
            Address sender_address = rdr.senderAddress;
            Address real_sender_address = rdr.realSenderAddress;

            if (friend != null)
            {
                if (endpoint != null)
                {
                    // Update friend's last seen and relay if outgoing stream capabilities are disabled
                    if ((streamCapabilities & StreamCapabilities.Outgoing) == 0)
                    {
                        friend.updatedStreamingNodes = Clock.getNetworkTimestamp();
                        friend.relayNode = new Peer(endpoint.getFullAddress(true), endpoint.serverWalletAddress, Clock.getTimestamp(), Clock.getTimestamp(), Clock.getTimestamp(), 0);
                        friend.updatedStreamingNodes = friend.relayNode.lastSeen;
                        friend.lastSeenTime = friend.relayNode.lastSeen;
                        friend.online = true;
                    }
                }
            }

            return rdr;
        }
    }
}

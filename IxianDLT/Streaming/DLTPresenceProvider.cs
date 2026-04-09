using IXICore;
using IXICore.Streaming;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DLT.Streaming
{
    public class DLTSectorProvider : ISectorProvider
    {
        public DLTSectorProvider()
        {
        }

        /// <summary>
        /// Resolves locally sector nodes for the friend.
        /// </summary>
        /// <param name="friend">Friend for which to fetch sector nodes</param>
        /// <returns>List of sector nodes</returns>
        public async Task<List<Peer>> FetchSectorNodesAsync(Friend friend)
        {
            var sectorNodes = RelaySectors.Instance.getSectorNodes(friend.walletAddress.sectorPrefix, 20);

            var sectorPresences = sectorNodes.Select(PresenceList.getPresenceByAddress)
             .Where(p => p != null)
             .Select(x => new Peer(x.addresses.First().address, x.wallet, x.addresses.First().lastSeenTime, 0, 0, 0))
             .ToList();

            friend.sectorNodes = sectorPresences;

            return friend.sectorNodes;
        }
    }
}

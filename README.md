# Ixian DLT

**Ixian DLT** is the consensus and coordination layer of the [Ixian Platform](https://www.ixian.io).
It maintains the **global ledger of transactions, IXI Names, and base presences**, and provides secure, scalable, and
decentralized coordination for the entire Ixian ecosystem.

At its heart is a novel consensus algorithm called **Proof of Collaborative Work (PoCW)** - a custom consensus unique to Ixian.
It combines the security of Proof of Work with the efficiency and fairness of collaborative block signing.

---

## <img src="IxianDLT/IxianDLT.ico" alt="Ixian Logo" width="24" height="24"> Why Ixian DLT?

* 🤝 **Consensus for Everything** - Ixian DLT secures transactions and IXI Names.
* 🛠️ **Proof of Collaborative Work (PoCW)** - Nodes submit Proof of Work solutions to earn eligibility for block signing and
share in block rewards.
* 🕸️ **Base Presence Tracking** - DLT maintains a **global, signed map of all active DLT and S2 nodes**, defining sectors and
enabling secure client discovery across the Ixian Platform.
* 🌐 **IXI Names Registry** - A decentralized naming system where human-readable names ('alice.ixi') can be mapped to
cryptographic addresses, IPs, or metadata, with time-bound ownership.
* ⚡ **Scalable & Efficient** - Lightweight validation, optimized block structures, and low resource usage make Ixian suitable
for **microtransactions and IoT-scale adoption**.
* ♻️ **Resilient by Design** - Fully distributed, fault-tolerant, and free from central points of failure.

---

## 🛠️ Proof of Collaborative Work (PoCW)

Ixian's **PoCW** ensures fairness, efficiency, and strong security while avoiding the issues of traditional PoW.

### Block Production & Validation

1. A node produces a new **block candidate**.
2. Other nodes validate the block and **sign to attest its correctness**.
3. If a node has signed the winning block, it earns a share of the reward.

### Eligibility (Proof of Work Solutions)

* Not every node can sign every block.
* To qualify, a node must produce a valid **PoW solution** for one of the last 30 blocks (\~15 minutes).
* A solution is computed as 'SHA3_512(SHA3_512(block height || block hash || recipient address || active public key hash || nonce))'
and must meet minimum difficulty.

### Reward Shares

* Rewards are **proportional to the difficulty** of the PoW solution.
* Nodes may continue hashing after finding one solution to improve their submission and increase their share.

### Block Freezing

* The **minimum signatures required** for block acceptance is **adaptive**, based on the moving average of recent signature
counts. This keeps block production in sync with current participation and **reduces forks**.
* Once the adaptive minimum is reached, the block is **accepted** and new blocks can be built on top.
* Eligible nodes may continue signing until the block reaches **depth 5** (five successors). At that point, the signature set is
frozen.

### Signing Transaction

* Rewards are distributed via a **Signing Transaction** in a later block, after a maturity period.
* This delayed payout ensures fairness, prevents abuse, and stabilizes consensus.

---

## 📚 Documentation

* Developer Documentation: [https://docs.ixian.io](https://docs.ixian.io)

---

## 🔗 Related Repositories

* [Ixian-Core](https://github.com/ixian-platform/Ixian-Core) - SDK and shared functionality
* [Ixian-DLT](https://github.com/ixian-platform/Ixian-DLT) - Blockchain ledger and consensus layer (this repository)
* [Ixian-S2](https://github.com/ixian-platform/Ixian-S2) - Peer-to-peer streaming and messaging overlay
* [Spixi](https://github.com/ixian-platform/Spixi) - Secure messenger and wallet app
* [Ixian-LiteWallet](https://github.com/ixian-platform/Ixian-LiteWallet) - Lightweight CLI wallet
* [QuIXI](https://github.com/ixian-platform/QuIXI) - Quick integration toolkit for Ixian Platform

---

## 🌱 Development Branches

* **master** - Stable, production-ready releases
* **development** - Active development, may contain unfinished features

For reproducible builds, always use the latest **release tag** on `master`.

---

## 🤝 Contributing

We welcome contributions from developers, integrators, and builders.

1. Fork this repository
2. Create a feature branch ('feature/my-change')
3. Commit with clear, descriptive messages
4. Open a Pull Request for review

Join the community on **[Discord](https://discord.gg/pdJNVhv)**.

---

## 🌍 Community & Links

* **Website**: [www.ixian.io](https://www.ixian.io)
* **Docs**: [docs.ixian.io](https://docs.ixian.io)
* **Discord**: [discord.gg/pdJNVhv](https://discord.gg/pdJNVhv)
* **Telegram**: [t.me/ixian\_official\_ENG](https://t.me/ixian_official_ENG)
* **Bitcointalk**: [Forum Thread](https://bitcointalk.org/index.php?topic=4631942.0)
* **GitHub**: [ixian-platform](https://www.github.com/ixian-platform)

---

## 📜 License

Licensed under the [MIT License](LICENSE).

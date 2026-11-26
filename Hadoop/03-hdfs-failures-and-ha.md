# HDFS Failures and High Availability

## Table of Contents

1. [Types of Failures in HDFS](#1-types-of-failures-in-hdfs)
2. [How the NameNode Detects a DataNode Failure](#2-how-the-namenode-detects-a-datanode-failure)
3. [DataNode Failure: Temporary](#3-datanode-failure-temporary)
4. [DataNode Failure: Permanent](#4-datanode-failure-permanent)
5. [Other Failures HDFS Handles](#5-other-failures-hdfs-handles)
6. [NameNode Failure](#6-namenode-failure)
7. [NameNode Metadata: FsImage and Edit Log](#7-namenode-metadata-fsimage-and-edit-log)
8. [Secondary NameNode](#8-secondary-namenode)
9. [High Availability (HA): Standby NameNode](#9-high-availability-ha-standby-namenode)
10. [Journal Nodes](#10-journal-nodes)
11. [ZooKeeper and the Failover Controller](#11-zookeeper-and-the-failover-controller)
12. [Split-Brain and Fencing](#12-split-brain-and-fencing)
13. [Secondary vs Standby NameNode](#13-secondary-vs-standby-namenode)
14. [Quick Revision Summary](#14-quick-revision-summary)

---

## 1. Types of Failures in HDFS

| Failure | What Breaks | How Bad? |
|---|---|---|
| **DataNode failure (temporary)** | A worker is down for a short time | Low: replicas exist on other nodes |
| **DataNode failure (permanent)** | A worker is gone for good | Low: blocks are re-replicated elsewhere |
| **NameNode failure** | The master is down | **Critical**: no one knows where any block is |

> **Key idea:** DataNode failures are **expected and handled automatically** thanks to **replication**. The NameNode is the real danger, which is why **High Availability (HA)** exists.

---

## 2. How the NameNode Detects a DataNode Failure

DataNodes prove they're alive using **heartbeats**:

```text
DataNode ──"I'm alive" every 3 seconds──► NameNode
```

| Time Without a Heartbeat | DataNode Status | What the NameNode Does |
|---|---|---|
| 0 to ~30 seconds | **Live** | Nothing |
| ~30 seconds | **Stale** | Avoids sending new reads/writes to it (when stale detection is enabled) |
| **~10.5 minutes** | **Dead** | Starts **re-replicating** its blocks to other DataNodes |

> **Why wait ~10 minutes before declaring it dead?** Copying blocks is expensive (lots of network and disk traffic). If a node just rebooted, re-replicating terabytes for nothing would be wasteful. So HDFS gives the node time to come back first.

---

## 3. DataNode Failure: Temporary

The DataNode is **unavailable for a short time** due to a **transient issue**:

1. **Network outage**
2. **Software crash** (the DataNode process stopped)
3. **Node reboot** for maintenance

### How the NameNode handles it

1. **Detection:** heartbeats stop arriving, and the node is marked **stale**, then **dead** if it stays silent long enough.
2. **Under-replication:** once the node is declared dead, its blocks are marked **under-replicated**, and the NameNode creates new copies on healthy nodes.
3. **When the DataNode recovers:** it re-registers and sends a **block report**. Some blocks now have **too many replicas** (the original ones plus the new copies), so the NameNode **deletes the excess replicas** and updates the **metadata** to keep everything consistent.

```text
Before failure:   Block B1 → DN1, DN2, DN3          (3 replicas)
DN3 goes down:    Block B1 → DN1, DN2               (2: under-replicated)
Re-replication:   Block B1 → DN1, DN2, DN5          (3 again)
DN3 comes back:   Block B1 → DN1, DN2, DN3, DN5     (4: over-replicated)
Cleanup:          Block B1 → DN1, DN2, DN5          (extra copy deleted)
```

---

## 4. DataNode Failure: Permanent

The DataNode is **permanently lost**, due to:

1. **Hardware failure** (machine dies)
2. **Disk corruption**
3. **Decommissioning** (an admin removes the node from the cluster on purpose)

### How the NameNode handles it

1. **Detection:** heartbeats stop, and the node is marked **dead**.
2. **Replication:** every block that was on the lost node is **copied from the surviving replicas** to other DataNodes until the **replication factor** is restored.
3. **If the machine comes back later** (e.g. after the hardware is fixed), its old data is **not trusted**. It can rejoin the cluster **like a fresh node**: outdated or corrupt blocks are **ignored and cleaned up** to avoid data inconsistency. HDFS uses **block version numbers (generation stamps)** and **checksums** to know which copies are outdated or broken.

> **Tip: decommissioning is the graceful way.** If you know a node is going away, **decommission** it instead of just unplugging it. The NameNode first copies all its blocks elsewhere, and only then does the node leave, so the cluster never runs under-replicated.

### Key takeaways

- HDFS **handles failures gracefully**
- **Metadata is the backbone** of HDFS
- The **replication factor is strictly maintained**

---

## 5. Other Failures HDFS Handles

| Failure | How HDFS Handles It |
|---|---|
| **Corrupted block** (bad disk sector) | Every block has a **checksum**. When a client reads a block and the checksum doesn't match, it reads from another replica instead and reports the bad copy. The NameNode then replaces it with a healthy copy. |
| **One disk fails** on a DataNode | The DataNode can keep running on its remaining disks (configurable), and the lost blocks are re-replicated. |
| **Whole rack fails** | **Rack awareness** guarantees that replicas are spread across at least 2 racks, so a copy survives elsewhere. |

➡️ Rack awareness is explained in [02-hdfs-architecture.md](02-hdfs-architecture.md#8-rack-awareness)

---

## 6. NameNode Failure

The **NameNode is the most critical component** of HDFS because it manages the **metadata and namespace** of the file system.

If the NameNode goes down:

- The data is still **safe on the DataNodes**...
- ...but **nobody knows which blocks belong to which file** or **where they are**.
- So the **whole cluster is unusable** until the NameNode is back.

In a basic setup with a single NameNode, it is a **Single Point of Failure (SPOF)**. HDFS has two solutions, built on top of each other:

| Solution | Purpose |
|---|---|
| **Secondary NameNode** | Makes **recovery faster** (but still has downtime) |
| **Standby NameNode (HA)** | **Takes over automatically** (almost no downtime) |

To understand both, we first need to know how the NameNode stores its metadata.

---

## 7. NameNode Metadata: FsImage and Edit Log

The NameNode keeps metadata **in memory (RAM)** for speed, but also saves it to disk in two files so it isn't lost on restart:

| File | What It Is | Analogy |
|---|---|---|
| **FsImage** | A full **snapshot** of the file system metadata at a point in time | A saved game |
| **Edit Log** | A list of **every change made since** the last FsImage (create, delete, rename...) | All the moves made since the last save |

**On restart**, the NameNode loads the FsImage and then **replays every change in the Edit Log** to get back to the latest state.

**The problem:** the Edit Log keeps **growing forever**. After weeks of activity it becomes huge, and replaying it on restart could take **hours**.

**The solution:** **checkpointing**, meaning regularly merging the Edit Log into a new FsImage so the log starts fresh.

```text
Old FsImage + Edit Log  ──merge──►  New FsImage  (+ empty Edit Log)
```

> **Note:** On startup, the NameNode enters **Safe Mode** (read-only). It waits until DataNodes have reported enough blocks before allowing writes. Block **locations** are never saved to disk; they're rebuilt from DataNode **block reports** every time.

---

## 8. Secondary NameNode

The **Secondary NameNode** helps with **checkpointing** and keeping the **size of the NameNode's metadata (Edit Log) under control**.

### How it works

1. Periodically (by default every **1 hour**, or after **1 million** changes), it fetches the **FsImage** and **Edit Log** from the NameNode.
2. It **merges** them into a **new FsImage**.
3. It sends the new FsImage **back to the NameNode**, which starts a fresh, empty Edit Log.

### What it is NOT

> **Watch out:** Despite the name, the Secondary NameNode is **not a backup** and **not a failover mechanism**. If the NameNode dies, the Secondary **cannot take over**.

What it does is make **recovery faster**: since the metadata is regularly compacted, a restarted NameNode has a small Edit Log to replay.

- Works fine **if we can afford some downtime**
- Changes made since the last checkpoint **may be lost** if the NameNode's disk is lost too

---

## 9. High Availability (HA): Standby NameNode

**High Availability (HA)** means running **two NameNodes**:

| | Active NameNode | Standby NameNode |
|---|---|---|
| **Role** | Serves all client requests | Waits, fully up to date, ready to take over |
| **Metadata** | Writes every change | Applies every change as it happens |

The **Standby NameNode** is a **hot backup** of the Active NameNode:

- It keeps an **up-to-date copy of the namespace in memory**
- It receives **all the changes (edit logs)** from the Active NameNode
- It is in **constant connection** with the cluster

**Key features:**

- **High Availability:** it can take over the role of the **Active NameNode** whenever required
- **Failover mechanism:** ensures **no data loss** and **minimal downtime** (seconds, not hours)
- **Synchronization:** all edit logs are applied, so the Standby is **always up to date**

> **Note:** In HA mode, **DataNodes send heartbeats and block reports to both NameNodes**. That's how the Standby always knows where every block is, and can take over immediately.

> **Tip:** In an HA setup, the **Standby NameNode also does the checkpointing**, so **no Secondary NameNode is needed**.

### HA Architecture

```text
            ┌───────────────────────────────────────┐
            │   ZooKeeper  (decides who is Active)  │
            └─────▲─────────────────────────▲───────┘
                  │ health                  │ health
                ZKFC                      ZKFC
                  │                         │
        ┌─────────┴─────────┐     ┌─────────┴─────────┐
        │  Active NameNode  │     │ Standby NameNode  │
        └─────────┬─────────┘     └─────────▲─────────┘
     writes edits │                         │ reads edits
                  ▼                         │
        ┌───────────────────────────────────┴───────┐
        │  Journal Node 1 │ Journal Node 2 │ JN 3   │
        └───────────────────────────────────────────┘

   All DataNodes send heartbeats + block reports to BOTH NameNodes
```

The three building blocks are **Journal Nodes** (keep edits in sync), **ZKFC** (watches health), and **ZooKeeper** (decides who's in charge).

---

## 10. Journal Nodes

To make sure the **edits stay in sync** between the Active and Standby NameNodes, we use **Journal Nodes**:

1. The **Active NameNode** writes every edit to the **Journal Nodes**.
2. The **Standby NameNode** reads those edits from the Journal Nodes and applies them, so it stays **updated**.

### Why an odd number (usually 3 or 5)?

An edit counts as saved once a **majority (quorum)** of Journal Nodes have it:

| Journal Nodes | Majority Needed | Failures Tolerated |
|---|---|---|
| 3 | 2 | **1** |
| 5 | 3 | **2** |

> **Note:** This setup is called the **Quorum Journal Manager (QJM)**. Using 4 nodes gives no benefit over 3 (both tolerate only 1 failure), which is why the count is always odd.

---

## 11. ZooKeeper and the Failover Controller

The Active and Standby NameNodes **don't report to a master** the way DataNodes do. So who notices when the Active one dies? That's the job of the **Failover Controller** and **ZooKeeper**.

### ZKFC (ZooKeeper Failover Controller)

- A small process running **next to each NameNode**
- **Monitors the health** of its NameNode (is the process responding? OS issues? any other problem?)
- Reports that health to **ZooKeeper** through **heartbeat signals**

### ZooKeeper

- **Coordinates failover** between the two NameNodes
- **Monitors the NameNode state** and keeps track of **which one is Active**
- **Triggers a leader election** during failover, so the Standby becomes the new Active
- **Prevents split-brain scenarios** (see below)

### Automatic failover, step by step

1. The Active NameNode crashes.
2. Its **ZKFC** detects it (or ZooKeeper stops hearing from that ZKFC).
3. **ZooKeeper** releases the "Active" lock.
4. The Standby's **ZKFC** grabs the lock, and a **leader election** picks it as the new leader.
5. The old Active is **fenced** (blocked, see below).
6. The Standby becomes the **new Active NameNode** and starts serving clients, all within **seconds**.

---

## 12. Split-Brain and Fencing

**Split-brain** happens when **both NameNodes think they are Active** at the same time. For example, the old Active was just slow (not dead), and it "wakes up" after the Standby has taken over.

Two masters writing different changes would **corrupt the metadata**.

HDFS prevents this in two ways:

1. **ZooKeeper** makes sure only **one** NameNode holds the Active lock at a time.
2. **Fencing:** before the new Active takes over, the old Active is **cut off**. The Journal Nodes only accept writes from **one NameNode at a time**, and the old one can also be forcibly shut down.

> **Analogy:** Fencing is like **changing the locks** when a new manager takes over, so the old manager can't come back in and give conflicting orders.

---

## 13. Secondary vs Standby NameNode

A classic **interview question**:

| | Secondary NameNode | Standby NameNode |
|---|---|---|
| **Main job** | Checkpointing (merge FsImage + Edit Log) | Hot backup, ready to take over |
| **Can it replace the NameNode?** | **No** | **Yes**, automatically |
| **Metadata freshness** | Up to the last checkpoint (e.g. 1 hour old) | **Always up to date** |
| **Downtime on NameNode failure** | Minutes to hours (manual recovery) | **Seconds** (automatic failover) |
| **Needs ZooKeeper / Journal Nodes?** | No | Yes |
| **Used in** | Simple / non-HA clusters | **Production HA** clusters |

---

## 14. Quick Revision Summary

- DataNodes send **heartbeats every 3 seconds**; after about **10.5 minutes** of silence, a node is declared **dead**.
- **Temporary DataNode failure:** blocks are re-replicated; when the node returns, **excess replicas are deleted**.
- **Permanent DataNode failure:** blocks are re-replicated; if the node returns, its old data **isn't trusted** and it rejoins as a fresh node.
- **Corrupted blocks** are caught with **checksums** and replaced from healthy replicas.
- The **NameNode** is the **single point of failure** in a basic setup.
- Metadata is stored as **FsImage** (snapshot) + **Edit Log** (changes); **checkpointing** merges them.
- **Secondary NameNode** = checkpointing helper, **not a backup**.
- **Standby NameNode** = hot backup for **High Availability**, takes over in seconds.
- **Journal Nodes** (odd number, majority rule) keep the edits in sync between Active and Standby.
- **ZKFC + ZooKeeper** detect failures, run the **leader election**, and prevent **split-brain**, with **fencing** blocking the old Active.

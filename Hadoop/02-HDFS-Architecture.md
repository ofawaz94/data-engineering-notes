# HDFS Architecture

## Table of Contents

1. [What is HDFS?](#1-what-is-hdfs)
2. [Key Concepts](#2-key-concepts)
3. [Types of File Systems](#3-types-of-file-systems)
4. [The Essence of HDFS](#4-the-essence-of-hdfs)
5. [HDFS Architecture: Master–Worker](#5-hdfs-architecture-masterworker)
6. [Blocks in HDFS](#6-blocks-in-hdfs)
7. [Replication and DataNode Failure](#7-replication-and-datanode-failure)
8. [Rack Awareness](#8-rack-awareness)
9. [How Reading and Writing Works](#9-how-reading-and-writing-works)
10. [What HDFS Is Good and Bad At](#10-what-hdfs-is-good-and-bad-at)
11. [Quick Revision Summary](#11-quick-revision-summary)

---

## 1. What is HDFS?

**HDFS (Hadoop Distributed File System)** is the **storage layer** of the Hadoop ecosystem. It is designed to **store and manage large-scale datasets across a distributed environment**, making it highly **scalable** and **fault tolerant**.

Unlike a traditional file system, HDFS is **optimized for big data**, where files are **massive** and need to be **processed efficiently**.

> **Simple picture:** To you, HDFS looks like **one big hard drive** with normal folders and files (`/data/sales/2026.csv`). Behind the scenes, each file is actually **split into pieces and spread across many machines**.

---

## 2. Key Concepts

### 1. File System

A **file system** is a layer between our **software and hardware**. It decides **how data is stored, organized, and found** on the disk.

| Operating System | File System |
|---|---|
| Linux | ext4 (ext family) |
| Windows | NTFS, FAT32 |
| macOS | APFS |
| **Hadoop** | **HDFS**, a **distributed** file system |

### 2. Block

A **block** is the **smallest unit of data storage** in a file system. Files are cut into blocks before being stored.

| File System | Typical Block Size | Example |
|---|---|---|
| **NTFS** (Windows) | **4 KB** | A 10 KB file → 3 blocks (4 KB + 4 KB + 2 KB used) |
| **HDFS** | **128 MB** (fixed, configurable) | A 300 MB file → 3 blocks (128 + 128 + 44 MB) |

> HDFS blocks are **huge** compared to normal file systems because HDFS is built for **huge files**. Fewer, bigger blocks = less metadata to track.

### 3. Cluster and Node

- **Node:** a single machine (computer/server)
- **Cluster:** a group of nodes connected over a network, working together as one system

### 4. Process and Daemon Process

- **Process:** a **program in execution** (e.g. when you open Chrome, it becomes a process)
- **Daemon process:** a **background process** that runs **without any user intervention**, usually all the time

> In HDFS, the **NameNode** and **DataNode** are daemon processes: they run in the background on each machine as long as the cluster is up.

### 5. Metadata

**Metadata** is **data about data**, like the **index page of a book**: it doesn't contain the content itself, but tells you **where to find it**.

*In HDFS:* file names, folder structure, permissions, the list of blocks in each file, and **which DataNodes hold each block**.

### 6. Replication

**Replication** is the process of **making copies of data** on different machines. It is needed to achieve **fault tolerance**: if one machine dies, the data still exists elsewhere.

---

## 3. Types of File Systems

| | Standalone File System | Distributed File System |
|---|---|---|
| **Where files live** | On a **single machine** | **Across multiple machines** in a cluster |
| **Capacity** | Limited to one machine's disks | Grows by adding machines |
| **If the machine fails** | Data is unavailable or lost | Data survives (replicas on other machines) |
| **Examples** | NTFS, ext4, APFS | **HDFS**, Amazon S3, Google Cloud Storage |

---

## 4. The Essence of HDFS

HDFS does three things:

1. **Distributes** data across multiple nodes/machines
2. **Replicates** data to ensure fault tolerance
3. **Provides efficient access** for big data processing

---

## 5. HDFS Architecture: Master–Worker

HDFS follows a **Master–Worker** architecture (also called Master–Slave). We **specify which machine is the master** and which ones are the **workers**.

```text
                    ┌──────────────────────────┐
                    │   NameNode  (Master)     │
                    │   stores METADATA only   │
                    │  file.csv = B1, B2, B3   │
                    │  B1 → DN1, DN2, DN4 ...  │
                    └────────────┬─────────────┘
          heartbeats + block reports (workers → master)
          ┌──────────────┬───────┴──────┬──────────────┐
          ▼              ▼              ▼              ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
    │ DataNode1│   │ DataNode2│   │ DataNode3│   │ DataNode4│
    │  B1  B3  │   │  B1  B2  │   │  B2  B3  │   │  B1  B2  │
    └──────────┘   └──────────┘   └──────────┘   └──────────┘
         Workers store the ACTUAL DATA (blocks)
```

### NameNode (Master)

- Stores **only the metadata**, never the actual data
- Knows **which blocks make up each file** and **which DataNodes hold each block**
- Manages the namespace: creating, deleting, and renaming files and folders
- Keeps the metadata **in memory (RAM)** for fast lookups

### DataNode (Worker)

- Stores the **actual data blocks** on its local disks
- Serves **read and write** requests from clients
- Sends a **heartbeat** to the NameNode every **3 seconds** ("I'm alive")
- Sends a **block report** regularly ("here is the list of blocks I hold")

### How they communicate

Workers are **controlled by the master**: they report to the NameNode and take instructions from it (e.g. "copy block B2 to DataNode 3"). They don't coordinate among themselves.

> **Small correction:** DataNodes **do** send data to each other directly, for example when copying replicas during a write (see the pipeline in [section 9](#9-how-reading-and-writing-works)). But the **decisions** about what goes where always come from the NameNode.

> **The NameNode is critical:** without it, nobody knows where any block is, so the data is unreachable even though it's still on the DataNodes. This is why the NameNode is a **Single Point of Failure** in basic setups, and why **High Availability (HA)** exists.
> ➡️ Covered in [03-HDFS-Failures-and-HighAvailability.md](03-HDFS-Failures-and-HighAvailability.md)

---

## 6. Blocks in HDFS

A **block** is the **smallest unit of storage in HDFS**. Each file stored in HDFS is **divided into blocks**, and the blocks are **distributed across multiple DataNodes**.

- **Default block size = 128 MB** (it was 64 MB in Hadoop 1)

### Example: How a file is split

```text
File: sales.csv (500 MB)

 Block 1: 128 MB
 Block 2: 128 MB
 Block 3: 128 MB
 Block 4: 116 MB   ← last block only uses the space it needs
```

> **The last block is not padded.** A 1 MB file in HDFS uses **1 MB of disk**, not 128 MB. The block size is a **maximum**, not a fixed amount of space taken.

**Formula:** number of blocks = file size ÷ block size, **rounded up**
*Example:* 1 GB = 1024 MB → 1024 ÷ 128 = **8 blocks**

### Why such big blocks?

1. **Efficient for large files:** fewer blocks to track and fewer disk seeks, so more time is spent actually reading data.
2. **Less metadata:** the NameNode keeps an entry for every block in memory, so fewer blocks = less RAM needed.

### Changing the block size

The block size is **configurable** (setting: `dfs.blocksize`). For example, you could reduce it from 128 MB to **32 MB**:

| Smaller blocks (e.g. 32 MB) | Bigger blocks (e.g. 256 MB) |
|---|---|
| ✅ **More parallelism**: more blocks = more tasks can run at the same time | ✅ **Less metadata** load on the NameNode |
| ❌ **More metadata** load on the NameNode | ❌ **Less parallelism** for smaller files |

> **Why does block count affect parallelism?** In MapReduce/Spark, usually **1 block = 1 task**. A 1 GB file with 128 MB blocks = 8 blocks = **8 tasks in parallel**. With 32 MB blocks it would be 32 tasks.

---

## 7. Replication and DataNode Failure

**DataNode (worker) failure** is handled by **replication**.

- **Default replication factor = 3**: every block is stored on **3 different DataNodes** (1 original + 2 copies)
- Configurable with the setting `dfs.replication`

### What happens when a DataNode dies?

1. The DataNode **stops sending heartbeats**.
2. After about **10 minutes** of silence, the NameNode marks it as **dead**.
3. The NameNode checks which blocks are now **under-replicated** (fewer than 3 copies).
4. It tells healthy DataNodes to **copy those blocks** to other nodes until each block is back to **3 replicas**.

✅ The user never notices: data is still readable from the remaining replicas.

### Storage cost of replication

Replication factor 3 means you need **3× the storage**:
*Example:* storing **100 TB** of data needs **300 TB** of raw disk space.

> **Interview favorite:** "How much space does a 1 GB file take in HDFS?" → **3 GB** (with the default replication factor of 3), spread over 8 blocks × 3 = **24 block replicas**.

➡️ NameNode failures and High Availability are covered in [03-HDFS-Failures-and-HighAvailability.md](03-HDFS-Failures-and-HighAvailability.md)

---

## 8. Rack Awareness

In a data center, machines are grouped into **racks** (a cabinet of servers sharing one network switch). If a rack's switch or power fails, **every machine in that rack goes down together**.

HDFS is **rack-aware**: it places replicas so that **losing a whole rack doesn't lose data**.

### Default replica placement (replication factor 3)

```text
   Rack 1                 Rack 2
┌───────────┐         ┌───────────┐
│ DN1  [R1] │         │ DN4  [R2] │
│ DN2       │         │ DN5  [R3] │
│ DN3       │         │ DN6       │
└───────────┘         └───────────┘
```

1. **Replica 1:** on the local node (where the writer is), or a random node
2. **Replica 2:** on a node in a **different rack**
3. **Replica 3:** on a **different node in the same rack as replica 2**

**Why this way?** It balances **safety** (data survives a whole rack failing) with **speed** (only one copy has to cross between racks, which is the slower network path).

---

## 9. How Reading and Writing Works

### Writing a file

```text
Client ──1. "I want to write file.csv"──► NameNode
Client ◄─2. "Write block 1 to DN1, DN4, DN5"── NameNode
Client ──3. sends data──► DN1 ──► DN4 ──► DN5    (replication pipeline)
Client ◄─4. acknowledgment ─────────────────┘
```

1. The client asks the NameNode to create the file.
2. The NameNode replies with **which DataNodes** should store each block.
3. The client sends the block to the **first DataNode**, which forwards it to the second, which forwards it to the third (**pipeline**).
4. Once all replicas are written, an **acknowledgment** goes back to the client. This repeats for every block.

### Reading a file

1. The client asks the NameNode: "Where are the blocks of `file.csv`?"
2. The NameNode returns the **block locations** (closest DataNodes first).
3. The client reads the blocks **directly from the DataNodes**.

> ✅ **Key point:** the actual data **never passes through the NameNode**. It only hands out addresses, which keeps it from becoming a bottleneck.

---

## 10. What HDFS Is Good and Bad At

| ✅ Good at | ❌ Bad at |
|---|---|
| Very **large files** (GBs to TBs) | **Many small files** (see below) |
| **Reading whole files** from start to end (high throughput) | **Low-latency** access to a single record |
| **Write once, read many times** | **Editing** existing files (you can append, but not modify in the middle) |
| Running on **cheap hardware** | Real-time lookups (use **HBase** for that) |

### The Small Files Problem

Every file and block takes about **150 bytes of NameNode memory**, no matter how small the file is.

- **1 file of 1 GB** = 8 blocks → very little metadata
- **1 million files of 1 KB** (same ~1 GB total) = 1 million+ entries → a **huge** load on NameNode memory, and **1 million tiny tasks** for processing

> **Tip:** In real projects, **combine small files into bigger ones** (e.g. 128 MB–1 GB each). You'll see the same advice in Spark and Databricks ("avoid small files", `OPTIMIZE` / compaction).

---

## 11. Quick Revision Summary

- **HDFS** = Hadoop's **distributed file system**: one big logical drive spread across many machines.
- **Master–Worker:** the **NameNode** stores **metadata**, and **DataNodes** store the **actual blocks**.
- DataNodes send **heartbeats every 3 seconds** and **block reports** to the NameNode.
- Files are split into **128 MB blocks** (configurable). The last block only uses what it needs.
- Smaller blocks = **more parallelism** but **more NameNode metadata**.
- **Replication factor 3** by default → fault tolerance, but **3× storage**.
- **Rack awareness** spreads replicas across racks to survive rack failures.
- Data flows **directly between clients and DataNodes**, never through the NameNode.
- HDFS loves **big files, write once / read many**, and hates **lots of small files**.

# HDFS Read and Write

## Table of Contents

1. [The Network Layout: Racks and Switches](#1-the-network-layout-racks-and-switches)
2. [Data Write in HDFS](#2-data-write-in-hdfs)
3. [Read Request in HDFS](#3-read-request-in-hdfs)
4. [Write vs Read Compared](#4-write-vs-read-compared)
5. [Quick Revision Summary](#5-quick-revision-summary)

---

## 1. The Network Layout: Racks and Switches

Before looking at reads and writes, it helps to know how the machines are connected, because HDFS tries hard to **keep traffic short**.

```text
                         ┌──────────────┐
                         │  Core Switch │   connects all the racks
                         └──────┬───────┘
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
   ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
   │   Switch    │       │   Switch    │       │   Switch    │  rack switch
   ├─────────────┤       ├─────────────┤       ├─────────────┤
   │    DN1      │       │    DN5      │       │    DN9      │
   │    DN2      │       │    DN6      │       │    DN10     │
   │    DN3      │       │    DN7      │       │    DN11     │
   │    DN4      │       │    DN8      │       │    DN12     │
   └─────────────┘       └─────────────┘       └─────────────┘
       Rack 1                Rack 2                Rack 3
```

| Component | Role |
|---|---|
| **Rack** | A cabinet of machines (DataNodes) |
| **Rack switch** | Connects the machines **inside one rack** |
| **Core switch** | Connects **all the racks** together |

**Why it matters:** data moving **within the same rack** only goes through the rack switch, which is fast. Data moving **between racks** has to go through the core switch, which is slower and shared by everyone. So HDFS prefers the **closest** DataNode whenever it can.

| From → To | Path | Cost |
|---|---|---|
| Same machine | No network at all | Cheapest |
| Same rack | Rack switch only | Cheap |
| Different rack | Rack switch → core switch → rack switch | Most expensive |

➡️ Rack awareness and replica placement are explained in [02-HDFS-Architecture.md](02-HDFS-Architecture.md#8-rack-awareness)

---

## 2. Data Write in HDFS

**Scenario:** client **A** wants to save a file into HDFS.

> **Key rule:** The client always **talks to the NameNode first**, but the **actual data never goes through the NameNode**. It goes straight to the DataNodes.

### Step by step

**Step 1: Write request to the NameNode**

The client asks the NameNode: *"I want to write `sales.csv`."*

The NameNode checks:
- Does the client have **permission** to write in that folder?
- Does a file with that name **already exist**? (HDFS doesn't overwrite by default)

**Step 2: The NameNode replies with a list of DataNodes**

If everything is OK, the NameNode picks **which DataNodes** should store each block, using **rack awareness** (with replication factor 3):

| Replica | Where |
|---|---|
| 1st | The client's own node, or a nearby node |
| 2nd | A node in a **different rack** |
| 3rd | A **different node in the same rack as the 2nd** |

*Example reply:* "Write block 1 to **DN1 → DN6 → DN7**."

**Step 3: The client writes to the first DataNode only**

The client splits the file into **blocks** (128 MB) and sends block 1 to **DN1**, through the switches. It does **not** send 3 copies itself.

**Step 4: The replication pipeline**

The DataNodes pass the data **along a chain**:

```text
Client ──► DN1 ──► DN6 ──► DN7
          (Rack 1)  (Rack 2)  (Rack 2)
```

- DN1 stores the data and forwards it to DN6
- DN6 stores it and forwards it to DN7
- The data is sent in small **packets** (about 64 KB), so all 3 DataNodes are writing **at the same time**, not one after another

**Step 5: Acknowledgments go back**

```text
Client ◄── ack ── DN1 ◄── ack ── DN6 ◄── ack ── DN7
```

Each DataNode confirms it saved the data, and the confirmation travels back along the chain to the client.

**Step 6: Repeat for every block, then close the file**

Steps 2 to 5 repeat for block 2, block 3, and so on, each with its own list of DataNodes. When all blocks are written, the client tells the NameNode *"I'm done"*, and the NameNode **saves the file's metadata** (which blocks, on which DataNodes).

### Full picture

```text
          1. write request          ┌────────────┐
 ┌────────┐ ───────────────────────►│  NameNode  │  checks permissions,
 │ Client │ ◄───────────────────────│            │  picks DataNodes
 └───┬────┘   2. list of DataNodes  └────────────┘
     │
     │ 3. send block
     ▼
   ┌─────┐  4. forward  ┌─────┐  4. forward  ┌─────┐
   │ DN1 │ ───────────► │ DN6 │ ───────────► │ DN7 │
   └─────┘ ◄─────────── └─────┘ ◄─────────── └─────┘
             5. ack               5. ack
```

### What if a DataNode fails during the write?

1. The pipeline is **stopped**.
2. The failed DataNode is **removed** from the chain.
3. Writing **continues with the remaining DataNodes** (e.g. DN1 → DN7).
4. The NameNode notices the block has only 2 copies and **creates the 3rd copy later** on another node.

The client doesn't lose any data, and the write doesn't have to start over.

---

## 3. Read Request in HDFS

**Scenario:** a client wants to read a file that is stored as **2 blocks**, each with **3 replicas**:

| Block | Stored on |
|---|---|
| Block 1 | DN1, DN6, DN11 |
| Block 2 | DN3, DN5, DN12 |

### Step by step

**Step 1: Read request to the NameNode**

The client asks the NameNode: *"I want to read `sales.csv`."*

The NameNode checks:
- **Permissions:** is the client allowed to read this file?
- **Metadata:** which blocks make up the file, and where are they?

**Step 2: The NameNode returns the block locations**

It sends back the list of DataNodes for **each block**, **sorted by distance** from the client (closest first):

```text
Block 1 → DN1, DN6, DN11
Block 2 → DN3, DN5, DN12
```

**Step 3: The client reads directly from the closest DataNodes**

The client goes **straight to the DataNodes** through the switches, without the NameNode in the middle:

- **3.a:** reads **block 1 from DN1** (the closest copy of block 1)
- **3.b:** reads **block 2 from DN3** (the closest copy of block 2, in the same rack)

The client then **puts the blocks together in order** to rebuild the complete file.

```text
          1. read request           ┌────────────┐
 ┌────────┐ ───────────────────────►│  NameNode  │  checks permissions
 │ Client │ ◄───────────────────────│            │  + metadata
 └───┬────┘   2. block locations    └────────────┘
     │
     │ 3. read directly from the closest copies
     ├──────────► DN1  (block 1)
     └──────────► DN3  (block 2)
```

### We don't need to read from all the copies

Each block has 3 replicas, but the client reads **only one copy of each block**: the **closest** one. The other replicas are there for **safety**, not for reading.

### What if a DataNode fails during the read?

- If DN1 doesn't respond, the client simply moves to the **next DataNode in the list** (DN6) for block 1.
- If a block is **corrupted** (its **checksum** doesn't match), the client reads it from another replica and **reports the bad copy** to the NameNode, which replaces it.

The client gets the full file either way.

---

## 4. Write vs Read Compared

| | Write | Read |
|---|---|---|
| **First step** | Ask the NameNode where to write | Ask the NameNode where the blocks are |
| **NameNode checks** | Permissions, file doesn't already exist | Permissions, metadata |
| **NameNode returns** | A list of DataNodes to write to | A list of DataNodes holding each block |
| **Data flow** | Client → first DataNode → pipeline to the others | Client ← closest DataNode for each block |
| **Copies involved** | **All 3** replicas are written | Only **1** replica per block is read |
| **If a DataNode fails** | Pipeline continues without it; missing copy is made later | Read from the next DataNode in the list |
| **Data passes through the NameNode?** | **Never** | **Never** |

> **Interview tip:** A very common question is *"Does data go through the NameNode?"* The answer is **no**. The NameNode only handles **metadata** (who can access what, and where it is). This keeps it from becoming a bottleneck.

---

## 5. Quick Revision Summary

- Racks connect through a **core switch**; **same-rack traffic is cheaper** than cross-rack traffic.
- **Write:** client asks the NameNode → gets a list of DataNodes → sends each block to the **first DataNode** → the block flows along a **pipeline** to the others → **acks** come back → the NameNode saves the metadata.
- Replica placement follows **rack awareness**: 1st nearby, 2nd on a different rack, 3rd on the same rack as the 2nd.
- **Read:** client asks the NameNode → gets block locations **sorted by distance** → reads each block from the **closest DataNode** → puts the blocks together.
- The client reads **one copy per block**, not all of them.
- Failures are handled smoothly: **skip the failed DataNode** and use the other replicas.
- Actual data **never passes through the NameNode**.

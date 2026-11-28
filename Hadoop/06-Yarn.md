# YARN (Yet Another Resource Negotiator)

## Table of Contents

1. [What is YARN?](#1-what-is-yarn)
2. [Why YARN Was Created (Hadoop 1 vs Hadoop 2)](#2-why-yarn-was-created-hadoop-1-vs-hadoop-2)
3. [YARN Architecture at a Glance](#3-yarn-architecture-at-a-glance)
4. [Components of YARN](#4-components-of-yarn)
5. [How a Job Runs on YARN, Step by Step](#5-how-a-job-runs-on-yarn-step-by-step)
6. [YARN Schedulers](#6-yarn-schedulers)
7. [Fault Tolerance in YARN](#7-fault-tolerance-in-yarn)
8. [YARN and Data Locality](#8-yarn-and-data-locality)
9. [YARN Today](#9-yarn-today)

---

## 1. What is YARN?

**YARN** is the **resource management layer** of Hadoop. It decides **which job gets how much CPU and memory, and on which machine** in the cluster.

| Hadoop Core Part | Job |
|---|---|
| **HDFS** | Stores the data |
| **MapReduce** (or Spark, etc.) | Processes the data |
| **YARN** | Manages the cluster's resources and schedules the work |

> **Key idea:** YARN **separates resource management from data processing**. YARN only hands out resources; the application (MapReduce, Spark, ...) decides what to do with them.

---

## 2. Why YARN Was Created (Hadoop 1 vs Hadoop 2)

In **Hadoop 1**, MapReduce did **everything**: it processed the data **and** managed the cluster's resources, using two daemons:

| Hadoop 1 Daemon | Role |
|---|---|
| **JobTracker** (one, on the master) | Accepted jobs, scheduled every task, tracked progress, and handled failures |
| **TaskTracker** (one per worker) | Ran the map and reduce tasks in fixed **slots** |

### The problems with this design

- **Bottleneck:** one JobTracker did all the work, limiting how far the cluster could scale.
- **Single point of failure:** if the JobTracker died, **all running jobs** were lost.
- **MapReduce only:** the cluster could run nothing but MapReduce jobs.
- **Wasted resources:** slots were fixed as either "map slots" or "reduce slots". Free reduce slots couldn't be used by map tasks, even when they were idle.

### Hadoop 2: YARN splits the JobTracker's job in two

```text
            Hadoop 1                              Hadoop 2
┌─────────────────────────────┐      ┌─────────────────────────────┐
│         JobTracker          │      │   ResourceManager (YARN)    │
│  resource management        │ ───► │   resource management       │
│  + job scheduling/tracking  │      ├─────────────────────────────┤
└─────────────────────────────┘      │   ApplicationMaster         │
                                     │   (one per job) tracks job  │
                                     └─────────────────────────────┘
```

| | Hadoop 1 (MapReduce v1) | Hadoop 2+ (YARN) |
|---|---|---|
| **Resource management** | JobTracker | ResourceManager |
| **Job tracking** | JobTracker (for all jobs) | ApplicationMaster (one per job) |
| **Worker daemon** | TaskTracker | NodeManager |
| **Unit of resources** | Fixed map/reduce slots | Flexible **containers** (RAM + CPU) |
| **Engines supported** | MapReduce only | MapReduce, Spark, Tez, Flink, ... |

> **Note:** This is why YARN is often called the **"operating system" of Hadoop**: like an OS shares a computer's CPU and RAM between programs, YARN shares the **cluster's** CPU and RAM between jobs.

> **Important:** YARN is a resource manager, not a processing engine. It can run applications such as MapReduce, Spark, Tez, and Flink; each application brings its own execution logic.

---

## 3. YARN Architecture at a Glance

```text
                         ┌───────────────────────────┐
     Client ───────────► │      ResourceManager      │   (master)
     submits a job       │  ┌───────────┐ ┌────────┐ │
                         │  │ Scheduler │ │  Apps  │ │
                         │  │           │ │Manager │ │
                         │  └───────────┘ └────────┘ │
                         └─────▲───────────────▲─────┘
                   heartbeats  │               │  heartbeats
                 ┌─────────────┴─────┐   ┌─────┴─────────────┐
                 │   NodeManager 1   │   │   NodeManager 2   │   (workers)
                 │ ┌───────────────┐ │   │ ┌───────────────┐ │
                 │ │ App Master    │ │   │ │ Container     │ │
                 │ │ (Job A)       │ │   │ │ (Job A task)  │ │
                 │ └───────────────┘ │   │ └───────────────┘ │
                 │ ┌───────────────┐ │   │ ┌───────────────┐ │
                 │ │ Container     │ │   │ │ Container     │ │
                 │ │ (Job A task)  │ │   │ │ (Job A task)  │ │
                 │ └───────────────┘ │   │ └───────────────┘ │
                 └───────────────────┘   └───────────────────┘
```

| YARN | Runs on | Like in HDFS |
|---|---|---|
| **ResourceManager** | Master node | NameNode |
| **NodeManager** | Every worker node | DataNode |

> **Tip:** In a real cluster, the **NodeManager and the DataNode run on the same worker machines**. That is what makes data locality possible (see [section 8](#8-yarn-and-data-locality)).

---

## 4. Components of YARN

### 1. ResourceManager (RM)

The **master daemon** of YARN. It **allocates resources** for the whole cluster and makes sure they are **used efficiently across all jobs**. There is one active ResourceManager per cluster.

It has two main parts:

| Part | Role |
|---|---|
| **(a) Scheduler** | **Schedules jobs in the cluster**: decides which application gets which resources, based on capacity, queues and priorities. It only allocates; it does **not** monitor or restart tasks. |
| **(b) ApplicationsManager** | **Manages the applications in the cluster**: accepts submitted jobs, starts the **first container** for each job's **ApplicationMaster**, and restarts the ApplicationMaster if it fails. |

> **Watch out:** The **Applications*Manager*** (inside the RM, one for the whole cluster) and the **Application*Master*** (one per job) are different things with very similar names. A common interview trick.

### 2. NodeManager (NM)

The **worker daemon**. One runs on **every worker node**, like a **site manager** in charge of one site.

- **Reports everything to the ResourceManager** through regular **heartbeats**: "I'm alive, and this is how much RAM and CPU I have free."
- **Launches containers** when asked, and **monitors their resource usage**.
- **Kills a container** that uses more memory than it was given.

### 3. ApplicationMaster (AM)

A **per-job manager**: every job gets its **own** ApplicationMaster, so:

> **Number of ApplicationMasters = number of running jobs (applications)**

- **Negotiates resources with the ResourceManager** ("I need 10 containers of 2 GB each, ideally on these nodes").
- **Works with the NodeManagers** to start the tasks in those containers, then **executes and monitors** them.
- **Re-requests** a container if a task fails.
- Reports the job's progress and final status back to the client.

> **Note:** The ApplicationMaster is specific to the engine. MapReduce has its own AM (**MRAppMaster**), and Spark has its own. That is how YARN can run many different engines.

### 4. Container

A **resource allocation** on one node, normally defined by **memory** and **virtual CPU cores (vcores)**. A container is the environment in which an application component runs; it is not a virtual machine.

- **Executes the task** assigned to it (for example, one map task or one reduce task).
- Is **launched by the NodeManager**, at the request of the ApplicationMaster.
- The application uses it to run an **ApplicationMaster, task, or executor**.

> **Note:** The ApplicationMaster itself also runs **inside a container**: it's the **first container** a job gets.

> **Terminology:** a container is an allocation of resources, while the process inside it is the application code. This distinction matters when reading YARN logs and metrics.

### Summary table

| Component | How many | Role in one line |
|---|---|---|
| **ResourceManager** | 1 per cluster (+ standby for HA) | Owns and allocates the cluster's resources |
| **NodeManager** | 1 per worker node | Manages one node's resources and containers |
| **ApplicationMaster** | 1 per job | Manages one job: asks for resources, tracks tasks |
| **Container** | Many per node | A slice of RAM + CPU that runs one task |

---

## 5. How a Job Runs on YARN, Step by Step

```text
 Client          ResourceManager       NodeManager(s)       ApplicationMaster
   │  1. submit job     │                    │                      │
   │───────────────────►│                    │                      │
   │                    │ 2. launch AM in    │                      │
   │                    │    a container     │                      │
   │                    │───────────────────►│─────── starts ──────►│
   │                    │ 3. register + ask for containers          │
   │                    │◄──────────────────────────────────────────│
   │                    │ 4. grant containers (which nodes, size)   │
   │                    │──────────────────────────────────────────►│
   │                    │                    │ 5. launch tasks      │
   │                    │                    │◄─────────────────────│
   │                    │                    │ 6. tasks report      │
   │                    │                    │    progress          │
   │                    │                    │─────────────────────►│
   │                    │ 7. job done, AM unregisters, resources    │
   │                    │    are freed                              │
   │                    │◄──────────────────────────────────────────│
```

1. **Submit:** the client submits the job (the code, config, and input location) to the **ResourceManager**.
2. **Start the AM:** the **ApplicationsManager** finds a free container on some node, and that node's **NodeManager** starts the job's **ApplicationMaster** in it.
3. **Ask for resources:** the AM **registers** with the RM and requests containers (for example: "one per input block, 2 GB each").
4. **Grant:** the **Scheduler** decides and **grants containers** on specific nodes.
5. **Launch tasks:** the AM tells those **NodeManagers** to start the tasks in the granted containers.
6. **Monitor:** the containers run the tasks and report progress to the **AM**. If a task fails, the AM asks for a new container and retries it.
7. **Finish:** when all tasks are done, the AM reports success to the client, **unregisters** from the RM, and all its containers are **released** for other jobs.

> **Practical note:** container allocation is asynchronous. An AM can wait in `ACCEPTED` or `RUNNING` while the scheduler waits for capacity or a locality-preferred node.

---

## 6. YARN Schedulers

When many jobs compete for the same cluster, the **Scheduler** decides who goes first. YARN has 3 built-in schedulers:

| Scheduler | How it works | Good for | Downside |
|---|---|---|---|
| **FIFO** | First in, first out: applications are considered in arrival order | Small test clusters | A large job can delay smaller jobs behind it |
| **Capacity** | The cluster is split into **queues** with a guaranteed share each (e.g. Finance 40%, Marketing 60%). A queue can borrow idle capacity from others | Large shared clusters, many teams | Needs planning of queue sizes |
| **Fair** | Resources are **shared equally** among running jobs over time; a new job gets its fair share quickly | Mixed workloads with short and long jobs | Each job finishes a bit slower when the cluster is busy |

> **Tip:** In interviews, the safe summary is: **FIFO = simple, Capacity = guaranteed share per team, Fair = everyone gets an equal share.**

> **Configuration note:** the active scheduler is chosen by the cluster administrator. Do not assume a scheduler from application code; inspect `yarn-site.xml` or the ResourceManager UI/API.

---

## 7. Fault Tolerance in YARN

| What fails | What happens |
|---|---|
| **A task (container)** | The ApplicationMaster notices and **re-runs the task** in a new container, often on another node |
| **A NodeManager** | Its heartbeats stop, the RM marks the node as **lost**, and the AMs re-run that node's tasks elsewhere |
| **An ApplicationMaster** | The RM can **restart the AM** in a new container, subject to the application's configured maximum attempts |
| **The ResourceManager** | With **RM High Availability**, a **standby RM** takes over, coordinated by **ZooKeeper**, the same idea as NameNode HA |

> **Note:** RM High Availability works much like the NameNode HA you saw in [03-HDFS-Failures-and-HighAvailability.md](03-HDFS-Failures-and-HighAvailability.md): one **Active** RM, one or more **Standby** RMs, and ZooKeeper to elect the active one.

---

## 8. YARN and Data Locality

Remember the MapReduce rule: **move the code to the data**. YARN is the part that makes it happen.

When the ApplicationMaster asks for containers, it tells the RM **where the input blocks live** (it gets this from the HDFS NameNode). The Scheduler then tries, in order:

| Preference | Where the container runs | Network cost |
|---|---|---|
| **1. Node-local** | On the **same node** that holds the block | None |
| **2. Rack-local** | On another node **in the same rack** | Low (one switch) |
| **3. Off-rack** | On a node in **another rack** | Highest (through the core switch) |

```text
  HDFS: "Block 1 is on DN1, DN6, DN11"
           │
           ▼
  AM asks RM: "a container on DN1, DN6 or DN11, please"
           │
           ▼
  Scheduler grants a container on DN1  ──►  the task reads Block 1 from its own disk
```

➡️ See [05-MapReduce.md](05-MapReduce.md) for data locality and [02-HDFS-Architecture.md](02-HDFS-Architecture.md) for racks.

---

## 9. YARN Today

- YARN is **still widely used** in on-premise Hadoop clusters and in managed services like **Google Dataproc** and **Amazon EMR**.
- **Spark on YARN** is very common: Spark's **driver** acts as (or runs inside) the ApplicationMaster, and Spark's **executors** run inside YARN containers.
- In the cloud, **Kubernetes** is increasingly used instead of YARN to manage resources, and platforms like **Databricks** manage resources for you.
- The concepts—a central resource manager, worker agents, per-application coordination, and resource allocations—also appear in Kubernetes and Spark cluster managers, though their implementations differ.

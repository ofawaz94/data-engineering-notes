# YARN (Yet Another Resource Negotiator)

## Table of Contents

1. [What is YARN?](#1-what-is-yarn)
2. [Why YARN Was Created (Hadoop 1 vs Hadoop 2)](#2-why-yarn-was-created-hadoop-1-vs-hadoop-2)
3. [YARN Architecture at a Glance](#3-yarn-architecture-at-a-glance)
4. [Components of YARN](#4-components-of-yarn)
5. [Real-Life Analogy: A Construction Company](#5-real-life-analogy-a-construction-company)
6. [How a Job Runs on YARN, Step by Step](#6-how-a-job-runs-on-yarn-step-by-step)
7. [YARN Schedulers](#7-yarn-schedulers)
8. [Fault Tolerance in YARN](#8-fault-tolerance-in-yarn)
9. [YARN and Data Locality](#9-yarn-and-data-locality)
10. [YARN Today](#10-yarn-today)
11. [Interview Quick Answers](#11-interview-quick-answers)
12. [Quick Revision Summary](#12-quick-revision-summary)

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

- **Bottleneck:** one JobTracker did all the work, so clusters couldn't grow past about **4,000 nodes**.
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
| **Scalability** | About 4,000 nodes | 10,000+ nodes |

> **Note:** This is why YARN is often called the **"operating system" of Hadoop**: like an OS shares a computer's CPU and RAM between programs, YARN shares the **cluster's** CPU and RAM between jobs.

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

> **Tip:** In a real cluster, the **NodeManager and the DataNode run on the same worker machines**. That is what makes data locality possible (see [section 9](#9-yarn-and-data-locality)).

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

A **bundle of physical resources** (**RAM, CPU cores**, and disk) on one node. Think of it as a **team of workers with their tools**.

- **Executes the task** assigned to it (for example, one map task or one reduce task).
- Is **launched by the NodeManager**, at the request of the ApplicationMaster.
- **All containers of a job report to that job's ApplicationMaster.**

> **Note:** The ApplicationMaster itself also runs **inside a container**: it's the **first container** a job gets.

### Summary table

| Component | How many | Role in one line |
|---|---|---|
| **ResourceManager** | 1 per cluster (+ standby for HA) | Owns and allocates the cluster's resources |
| **NodeManager** | 1 per worker node | Manages one node's resources and containers |
| **ApplicationMaster** | 1 per job | Manages one job: asks for resources, tracks tasks |
| **Container** | Many per node | A slice of RAM + CPU that runs one task |

---

## 5. Real-Life Analogy: A Construction Company

| YARN | Construction Company |
|---|---|
| **ResourceManager** | The **head office** that owns all the workers and equipment and decides which project gets what |
| **Scheduler** | The **planning department** that divides the workers between projects |
| **ApplicationsManager** | The **front desk** that accepts new projects and assigns each one a project manager |
| **NodeManager** | The **site manager** at each building site, who reports what's available back to head office |
| **ApplicationMaster** | The **project manager** of one project, who asks head office for workers and supervises the work |
| **Container** | A **team of workers with their tools**, doing one piece of the work |

---

## 6. How a Job Runs on YARN, Step by Step

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

---

## 7. YARN Schedulers

When many jobs compete for the same cluster, the **Scheduler** decides who goes first. YARN has 3 built-in schedulers:

| Scheduler | How it works | Good for | Downside |
|---|---|---|---|
| **FIFO** | First in, first out: one job at a time, in order of arrival | Small test clusters | A big job blocks all the small jobs behind it |
| **Capacity** | The cluster is split into **queues** with a guaranteed share each (e.g. Finance 40%, Marketing 60%). A queue can borrow idle capacity from others | Large shared clusters, many teams (the default in Apache Hadoop) | Needs planning of queue sizes |
| **Fair** | Resources are **shared equally** among running jobs over time; a new job gets its fair share quickly | Mixed workloads with short and long jobs | Each job finishes a bit slower when the cluster is busy |

> **Tip:** In interviews, the safe summary is: **FIFO = simple, Capacity = guaranteed share per team, Fair = everyone gets an equal share.**

---

## 8. Fault Tolerance in YARN

| What fails | What happens |
|---|---|
| **A task (container)** | The ApplicationMaster notices and **re-runs the task** in a new container, often on another node |
| **A NodeManager** | Its heartbeats stop, the RM marks the node as **lost**, and the AMs re-run that node's tasks elsewhere |
| **An ApplicationMaster** | The RM's ApplicationsManager **restarts the AM** in a new container (a limited number of attempts, by default 2) |
| **The ResourceManager** | With **RM High Availability**, a **standby RM** takes over, coordinated by **ZooKeeper**, the same idea as NameNode HA |

> **Note:** RM High Availability works much like the NameNode HA you saw in [03-HDFS-Failures-and-HighAvailability.md](03-HDFS-Failures-and-HighAvailability.md): one **Active** RM, one or more **Standby** RMs, and ZooKeeper to elect the active one.

---

## 9. YARN and Data Locality

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

## 10. YARN Today

- YARN is **still widely used** in on-premise Hadoop clusters and in managed services like **Google Dataproc** and **Amazon EMR**.
- **Spark on YARN** is very common: Spark's **driver** acts as (or runs inside) the ApplicationMaster, and Spark's **executors** run inside YARN containers.
- In the cloud, **Kubernetes** is increasingly used instead of YARN to manage resources, and platforms like **Databricks** manage resources for you.
- The concepts (a central resource manager, worker agents, per-job managers, and containers of CPU + RAM) are the **same ideas** you'll find in Kubernetes and in Spark's cluster managers.

---

## 11. Interview Quick Answers

| Question | Short answer |
|---|---|
| **What is YARN?** | Hadoop's resource management layer: it allocates CPU and memory across the cluster and schedules jobs |
| **Why was YARN introduced?** | The Hadoop 1 JobTracker was a bottleneck and single point of failure, and only supported MapReduce |
| **Main components?** | ResourceManager (Scheduler + ApplicationsManager), NodeManager, ApplicationMaster, Container |
| **ApplicationsManager vs ApplicationMaster?** | ApplicationsManager: one per cluster, inside the RM, accepts jobs and starts AMs. ApplicationMaster: one per job, manages that job's tasks |
| **What is a container?** | A bundle of RAM + CPU on one node where a task runs |
| **How many ApplicationMasters are there?** | One per running application (job) |
| **Name the schedulers** | FIFO, Capacity, Fair |

---

## 12. Quick Revision Summary

- **YARN** = Yet Another Resource Negotiator, the **resource manager** of Hadoop (the "OS of the cluster").
- It **separates resource management from processing**, which lets Hadoop run **MapReduce, Spark, and more**.
- **Hadoop 1:** JobTracker + TaskTracker, a bottleneck, MapReduce only. **Hadoop 2:** ResourceManager + NodeManager + ApplicationMaster.
- **ResourceManager** (master): **Scheduler** (allocates resources) + **ApplicationsManager** (accepts jobs, starts AMs).
- **NodeManager** (every worker): sends **heartbeats** to the RM, launches and monitors **containers**.
- **ApplicationMaster** (one per job): **negotiates** containers with the RM, works with NMs to **run and monitor** tasks.
- **Container:** RAM + CPU on one node, runs one task, reports to the AM.
- **Schedulers:** FIFO, Capacity (queues per team), Fair (equal share).
- YARN makes **data locality** happen by placing tasks **node-local → rack-local → off-rack**.

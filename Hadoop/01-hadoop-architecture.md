# Hadoop Architecture

## Table of Contents

1. [What is Hadoop?](#1-what-is-hadoop)
2. [Properties of Hadoop](#2-properties-of-hadoop)
3. [Hadoop Core vs Hadoop Ecosystem](#3-hadoop-core-vs-hadoop-ecosystem)
4. [Hadoop Ecosystem: Main Components](#4-hadoop-ecosystem-main-components)
5. [Components Grouped by Role](#5-components-grouped-by-role)
6. [Two Key Characteristics of the Ecosystem](#6-two-key-characteristics-of-the-ecosystem)
7. [Hadoop Today](#7-hadoop-today)
8. [Quick Revision Summary](#8-quick-revision-summary)

---

## 1. What is Hadoop?

**Hadoop** is an **open-source framework** designed to store and process **massive amounts of data** in a **distributed and scalable** way, using a cluster of ordinary machines.

### Hadoop solves 2 problems

| Problem | Hadoop's Answer |
|---|---|
| **Massive storage**: data is too big for one machine | Split the data and spread it across many machines (**HDFS**) |
| **Fast processing**: one machine would take too long | Process the pieces **in parallel** on many machines at once (**MapReduce**) |

> **Background:** Hadoop was inspired by two Google papers: the **Google File System (2003)** and **MapReduce (2004)**. Doug Cutting and Mike Cafarella built it as an open-source version, and it was named after Doug Cutting's son's **toy elephant** 🐘, which is why the logo is an elephant.

---

## 2. Properties of Hadoop

| Property | Meaning |
|---|---|
| **1. Scalability** | Scales **horizontally**: need more power? Add more machines to the cluster. |
| **2. Fault Tolerance** | Keeps **copies (replicas)** of the data on different machines, so if one machine fails, no data is lost. |
| **3. Distributed Processing** | Processes the data **where it is stored** instead of moving it around. |
| **4. Cost Effectiveness** | Runs on **inexpensive, commodity hardware** instead of expensive specialized servers. |
| **5. Open Source** | Free to use and modify. |

> **Key idea: "Move the code to the data, not the data to the code."**
> Data is huge (TBs), but the processing code is tiny (KBs). Sending the small code to the machines that hold the data is much faster than moving terabytes over the network. This is called **data locality**.

---

## 3. Hadoop Core vs Hadoop Ecosystem

**Hadoop Core** is the minimum needed to run Hadoop. It has 3 main parts:

| Core Component | Role | Analogy |
|---|---|---|
| **HDFS** | Storage | The warehouse 🏬 |
| **MapReduce** | Processing | The workers 👷 |
| **YARN** | Resource management | The manager who assigns workers 📋 |

The **Hadoop Ecosystem** is everything built **around** the core: extra tools that make Hadoop easier to use (SQL, scheduling, data ingestion, ML...).

> **Hadoop 1 vs Hadoop 2:** In Hadoop 1, MapReduce did **both** processing **and** resource management. Hadoop 2 introduced **YARN** to take over resource management. That freed the cluster to run other engines too, such as **Spark**, not just MapReduce.

---

## 4. Hadoop Ecosystem: Main Components

The **Hadoop ecosystem** is a collection of open-source projects and tools that work together to **store, process, and analyze** large amounts of data.

```text
┌──────────────────────────────────────────────────────────────┐
│  Hive (SQL)   Pig (scripts)   Mahout (ML)   Oozie (workflows)│  ← Tools on top
├──────────────────────────────────────────────────────────────┤
│               MapReduce (processing engine)                  │  ← Processing
├──────────────────────────────────────────────────────────────┤
│               YARN (resource management)                     │  ← Resources
├──────────────────────────────────────────────────────────────┤
│        HDFS (file storage)        HBase (NoSQL database)     │  ← Storage
└──────────────────────────────────────────────────────────────┘
   Sqoop / Flume bring data in          ZooKeeper coordinates everything
```

### 1. HDFS (Hadoop Distributed File System)

**Distributed storage** for very large files. It splits each file into **blocks** and stores them (with replicas) across many machines.

➡️ Details in [02-hdfs-architecture.md](02-hdfs-architecture.md)

### 2. MapReduce

Processes data by **dividing the work into smaller tasks that run in parallel**:

- **Map:** each machine processes its own piece of the data
- **Reduce:** the partial results are combined into the final answer

➡️ Details in [05-mapreduce/notes.md](05-mapreduce/notes.md)

### 3. YARN (Yet Another Resource Negotiator)

**Decouples resource management from application execution.** YARN decides **which job gets how much CPU and memory, and on which machine**, while the application itself just does its work.

### 4. Hive

A **query engine, not a database**. Writing MapReduce means writing Java, which is long and hard, so people wanted **SQL with the power of MapReduce**. Hive lets you write SQL-like queries (**HiveQL**) and **translates them into MapReduce jobs** behind the scenes.

```sql
-- You write this...
SELECT country, COUNT(*) FROM users GROUP BY country;
-- ...and Hive turns it into MapReduce jobs for you.
```

> Hive also introduced the **Hive Metastore**: a catalog that stores table names, columns, and where the data files live. Spark and many modern tools still use it today.

### 5. Pig

Also an **abstraction over MapReduce**, but uses its own scripting language called **Pig Latin**: no Java and no SQL. Good for step-by-step data transformations (ETL).

### 6. Sqoop (SQL + Hadoop)

Moves data **between Hadoop and relational databases** (MySQL, Oracle, etc.):

- **Import:** RDBMS ➜ HDFS
- **Export:** HDFS ➜ RDBMS

### 7. Oozie

A **workflow scheduler**. You define workflows in **XML files** to **schedule and automate** your jobs (e.g. "run the Sqoop import, then the Hive query, then the export, every night at 2 AM"). Essential for scheduling **complex ETL workflows**.

> Oozie is not an abstraction of MapReduce like Hive or Pig. It doesn't process data itself; it **orchestrates** other jobs. A modern equivalent is **Apache Airflow**.

### 8. HBase

A **NoSQL, column-oriented database** that runs on top of HDFS and allows **real-time reads and writes**.

> **Why do we need it?** HDFS is great for reading large files from start to end, but it **can't quickly look up or update a single record**. HBase adds that ability (e.g. "get the profile of user #12345 right now").

### 9. Mahout

The **data science / machine learning** component. Provides ML libraries that implement algorithms for **clustering, classification, recommendation**, etc.

### 10. Flume

Collects **logs and event data** from various sources (web servers, apps) and delivers them into **HDFS or HBase**.

- Ingests **streaming data**
- Supports **real-time analysis for monitoring**

### 11. ZooKeeper

**Coordinates the distributed system** so all machines stay **consistent** and in agreement (e.g. which machine is the current master, shared configuration).

- **Critical for reliability**: used by HBase, HDFS High Availability, Kafka, and others.

> **Fun fact:** Many Hadoop tools are named after animals (Hadoop the elephant, Pig, Hive), and **ZooKeeper** is the one that "keeps the zoo in order".

---

## 5. Components Grouped by Role

| Role | Components |
|---|---|
| **Storage** | HDFS, HBase |
| **Processing** | MapReduce, Pig, Hive |
| **Resource Management** | YARN |
| **Data Ingestion** | Flume (streaming/logs), Sqoop (databases) |
| **Coordination** | ZooKeeper |
| **Workflow Management** | Oozie |
| **Machine Learning** | Mahout |

> **Memory trick for ingestion:** **S**qoop = **S**QL databases, **F**lume = **F**lowing (streaming) data.

---

## 6. Two Key Characteristics of the Ecosystem

### 1. Loosely Coupled

Components are **independent**: you can **remove or replace** a component and the rest keeps working.
*Example:* don't need Pig? Don't install it. Want Spark instead of MapReduce? Swap it in.

### 2. Easy Integration

Hadoop can **connect with other frameworks**, both big data ones (like **Spark**, **Kafka**) and traditional ones (like **MySQL** and other RDBMSs).

---

## 7. Hadoop Today

Hadoop is still very important to **understand**, because modern tools are built on its ideas. But many of its parts have been replaced:

| Hadoop Component | Status Today | Modern Replacement |
|---|---|---|
| **HDFS** | Still used on-premise | Cloud storage: **Amazon S3, Azure ADLS, Google Cloud Storage** |
| **MapReduce** | Rarely written by hand anymore | **Apache Spark** (much faster, works in memory) |
| **YARN** | Still widely used | Also **Kubernetes** |
| **Hive** | Still used, especially the Metastore | **Spark SQL**, Databricks, Trino |
| **Pig** | Mostly retired | Spark |
| **Sqoop** | Retired | Spark JDBC, Azure Data Factory, AWS DMS |
| **Oozie** | Mostly legacy | **Apache Airflow**, Databricks Workflows |
| **Flume** | Mostly legacy | **Apache Kafka** |
| **Mahout** | Rarely used | **Spark MLlib**, scikit-learn |

> ✅ **Why still learn Hadoop?** Concepts like **distributed storage, data locality, replication, parallel processing, and resource management** are exactly how Spark, Databricks, and cloud data platforms work today. They are also common **interview questions**.

---

## 8. Quick Revision Summary

- **Hadoop** = open-source framework for **distributed storage + parallel processing** on cheap machines.
- **Properties:** scalable, fault-tolerant, distributed processing, cost-effective, open source.
- **Core:** **HDFS** (storage) + **MapReduce** (processing) + **YARN** (resource management).
- **Ecosystem tools:** Hive & Pig (easier processing), Sqoop & Flume (ingestion), Oozie (scheduling), HBase (real-time NoSQL), Mahout (ML), ZooKeeper (coordination).
- The ecosystem is **loosely coupled** and **integrates** easily with other tools.
- Today, **Spark** has largely replaced MapReduce, and **cloud storage** often replaces HDFS, but the concepts are the same.

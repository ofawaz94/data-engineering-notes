# Big Data Fundamentals

## Table of Contents

1. [What is Big Data?](#1-what-is-big-data)
2. [The 5 V's of Big Data](#2-the-5-vs-of-big-data)
3. [Big Data and Distributed Systems](#3-big-data-and-distributed-systems)
4. [Designing a Good Big Data System](#4-designing-a-good-big-data-system)
5. [On-Premise Infrastructure vs Cloud Solutions](#5-on-premise-infrastructure-vs-cloud-solutions)
6. [Database vs Data Warehouse vs Data Lake](#6-database-vs-data-warehouse-vs-data-lake)
7. [ETL vs ELT](#7-etl-vs-elt)
8. [Quick Revision Summary](#8-quick-revision-summary)

---

## 1. What is Big Data?

**Big Data** refers to datasets that are **too large, too fast, or too complex** for traditional databases to store and process efficiently.

There is **no universal size threshold** for Big Data. A dataset becomes "big" when its volume, speed, or complexity exceeds what the available tools and team can handle effectively. This can happen at very different scales depending on the workload.

### Why did Big Data appear?

- Growth of the **internet**
- **Social media** (posts, likes, videos, comments)
- Better **data capturing** (logs, clickstreams, sensors)
- **IoT devices** (smart watches, cars, home devices) sending data all the time

### Why not just use a bigger machine? (Vertical Scaling)

There are two ways to get more power:

| | Vertical Scaling (Scale **Up**) | Horizontal Scaling (Scale **Out**) |
|---|---|---|
| **Idea** | Make **one** machine stronger (more RAM, CPU, disk) | Add **more** machines that work together |
| **Limit** | A machine has finite CPU, memory, and storage | Capacity can grow by adding machines, though coordination and cost still impose limits |
| **Cost** | Larger machines can become expensive | Many smaller machines can help, but networking and operations also cost money |
| **Failure** | A single machine can be a single point of failure | Work can continue after a node failure if the system is designed with redundancy |
| **Example** | Upgrading your laptop's RAM | A cluster of 100 servers |

**The problem with vertical scaling:** no matter how much you upgrade a laptop (or a server), you eventually hit a **hardware limit**. The same happens with a traditional **RDBMS**: as its tables keep growing, a single database server can't keep up.

> 💡 **Tip:** This is one reason Big Data tools (Hadoop, Spark, etc.) are designed to distribute storage and processing across multiple machines instead of relying only on one powerful machine.

Distributed processing has trade-offs: machines must exchange data over a network, and the system needs coordination and monitoring. For small datasets, a single server can be simpler and faster. A common distributed strategy is to **partition** data into pieces so different nodes can work on them in parallel.

---

## 2. The 5 V's of Big Data

The 5 V's are a framework for understanding the **challenges and characteristics** of Big Data.

### 1. Volume: *How much data?*

The **size** of the data being generated.

- ⚠️ **Myth:** Volume is **not** the only factor that makes data "big", and there is **no fixed size** that makes data "Big Data".

### 2. Velocity: *How fast?*

The **speed** at which data is generated **and** needs to be processed.

| Processing Type | Description | Example | Tools |
|---|---|---|---|
| **Real-time** | Processed instantly as it arrives | Credit card fraud check, bank transactions, live feeds | Kafka, Spark Streaming |
| **Near real-time** | Data arrives continuously but is processed every few minutes (e.g. 2–5 min) | Dashboards refreshed every few minutes | Spark Structured Streaming (micro-batches) |
| **Batch** | Data is collected over a period and processed together | Monthly credit card bill | Hadoop MapReduce, Spark |

### 3. Variety: *What forms?*

Data comes in different forms and we have to deal with all of them.

| Type | Description | Examples |
|---|---|---|
| **Structured** | Fixed schema of rows & columns | SQL tables, Excel sheets |
| **Semi-structured** | Has some structure (tags/keys) but no fixed, enforced schema | JSON, XML, many log files |
| **Unstructured** | No predefined structure | Images, videos, audio, PDFs, free text |

> 💡 **Note:** CSV is often treated as **structured** because it is organized into rows and columns. It can also be described as semi-structured when the file has no enforced schema or data types; classifications vary by context.

### 4. Veracity: *Can we trust it?*

The **trustworthiness or quality** of the data.

- Example: an `age` column containing a **negative value** (`-5`)
- Messy data: missing values, duplicates, wrong formats, typos

### 5. Value: *Is it useful?*

Data should provide **insights that help in making business decisions**. Collecting data that produces no value is just a cost.

> 💡 **Memory trick:** Volume → *how much*, Velocity → *how fast*, Variety → *what kind*, Veracity → *how accurate*, Value → *how useful*.
>
> Originally there were only **3 V's** (Volume, Velocity, Variety). Veracity and Value were added later.

---

## 3. Big Data and Distributed Systems

When the **size or complexity** of our data increases, we need **more resources**:

| Resource | What it is | Hardware |
|---|---|---|
| **Storage** | Where data is saved | HDD, SSD |
| **Memory** | Where data is held while being worked on | RAM |
| **Compute (Processing)** | What does the work | CPU cores |

### Monolithic vs Distributed

```text
Monolithic System   → a single machine (e.g. a laptop)
                    → limited resources, hard to increase

Distributed System  → multiple machines working together (a cluster of nodes)
                    → easy to increase resources by adding more nodes
```

- **Node:** a single machine (physical or virtual) participating in a system
- **Cluster:** a group of nodes coordinated to work on shared tasks

> ✅ **Key idea:** Many Big Data systems use distributed storage and processing to handle scale. Not every data workload needs a distributed system; a single well-sized server is often simpler and more efficient for smaller workloads.

---

## 4. Designing a Good Big Data System

A good Big Data system is designed to handle large-scale data **efficiently**, ensuring **scalability, reliability, and fault tolerance**.

### 1. Scalability

The system can grow as demand grows (e.g. traffic spike during an **Amazon sale**).

- **a) Storage scalability:** more data can be stored by adding more storage.
- **b) Processing / compute scalability:** larger datasets can be processed faster by adding more compute machines.

### 2. Reliability and Fault Tolerance

The system should **keep working even if some components fail**.

> 💡 **Tip:** In a large cluster, individual failures are expected. Systems use techniques such as **replication**, retries, and recovery. HDFS commonly uses a replication factor of 3 by default, though it can be configured; replication helps availability but does not replace backups.

### 3. Cost Effectiveness

The system should **balance performance and scalability with cost**.

### 4. Security and Data Privacy

Data must be **protected from unauthorized access** (encryption, access control, compliance with laws like GDPR).

---

## 5. On-Premise Infrastructure vs Cloud Solutions

When deploying a Big Data system, organizations must choose between **on-premise infrastructure** and **cloud solutions**.

### Simple Analogy

| On-Premise 🏢 | Cloud ☁️ |
|---|---|
| Like **buying** an office | Like **renting** a house or a coworking space |
| High initial investment (e.g. buy a 20-node cluster) | **Pay-as-you-go**: pay only for what you use |
| You configure and set up everything yourself | Provider (AWS / GCP / Azure) handles the hardware |

### Detailed Comparison

| Aspect | On-Premise | Cloud |
|---|---|---|
| **Deployment** | Hardware and software are hosted within the organization's facilities. | Resources and services are hosted on the provider's servers and accessed via the internet. |
| **Cost Model** | High upfront costs for hardware, maintenance, and IT staff. | Pay-as-you-go pricing, with minimal upfront costs. |
| **Scalability** | Limited by the organization's hardware capacity; scaling requires purchasing and installing new equipment. | Highly scalable: add resources on demand instantly. |
| **Maintenance** | The organization manages and maintains hardware, software, and updates. | The provider manages the underlying infrastructure; customers still manage their data, access, and often some software and configuration. |
| **Flexibility** | Fixed capacity with little room for dynamic needs. | Highly flexible, allowing resources to scale up or down. |
| **Security** | Offers direct control over physical infrastructure, but the organization must secure it and its data. | Security responsibilities are shared between provider and customer; controls and compliance support depend on the service and configuration. |
| **Disaster Recovery** | The organization plans and operates its own backups and recovery systems. | Providers offer redundancy and recovery options, but customers must configure and validate the recovery they need. |

### Types of Cloud

| Type | Description | Example |
|---|---|---|
| **Public Cloud** | Infrastructure owned by a provider and shared by many customers | AWS, Azure, GCP |
| **Private Cloud** | Infrastructure set up by a **single organization** for its own use | A bank's internal cloud |
| **Hybrid Cloud** | Mix of public + private | Public for **computation**, private for **sensitive data** |
| **Community Cloud** | Shared by **multiple organizations** with a common concern | Hospitals sharing a healthcare cloud |

> 💡 **Tip:** Cloud equivalents of Hadoop/Spark clusters you'll see a lot: **AWS EMR**, **GCP Dataproc**, **Azure HDInsight**, and **Databricks** (runs on all three).

---

## 6. Database vs Data Warehouse vs Data Lake

### 1. Database

A system that holds an **organized collection of data** with a **well-defined schema**.

- **Used for:** day-to-day operational data, e.g. orders, balance updates, inventory
- **Best for OLTP** (Online Transaction Processing): many small, fast reads/writes
- Mostly **structured data**, schema is well defined
- Often holds current operational data; history may also be retained depending on the application and database design
- Storage costs and query patterns may lead organizations to move older data to analytical storage, but this is not a rule for every database

**Examples:** MySQL, PostgreSQL, Oracle, SQL Server

### 2. Data Warehouse

Stores **much more (historical) data** than a database and is built for **analysis**.

**Q: Why can't we just use the database for analysis?**

1. There can be **multiple data sources** (not just one database)
2. **Lots of data** → storing it in a database is too expensive
3. Running **complex analytical queries** on the operational database would slow it down for the actual business operations

**Characteristics:**

- Often separates analytical workloads from operational systems; storage cost depends on the platform and usage
- Data is loaded using **ETL** (cleaned *before* storing)
- **Best for OLAP** (Online Analytical Processing): complex queries over large amounts of data
- Mostly structured data

**Examples:** Snowflake, Amazon Redshift, Google BigQuery, Azure Synapse

### 3. Data Lake

Used when we want insights from **huge amounts of data in any format**.

A **data lake** is a **centralized repository** that stores **all kinds of raw data** (structured, semi-structured, unstructured) **at scale, without any predefined structure**.

- Data is loaded using **ELT** (stored raw *first*, transformed *later*)
- Very **cheap** storage
- Used by data scientists, ML, and big data processing

**Examples:** HDFS, Amazon S3, Azure Data Lake Storage (ADLS), Google Cloud Storage

> ⚠️ **Watch out:** Without proper organization and governance, a data lake can turn into a **"data swamp"**: lots of data that nobody can find or trust.

### Comparison

| | Database | Data Warehouse | Data Lake |
|---|---|---|---|
| **Purpose** | Day-to-day operations | Analytics & reporting | Store everything raw for analytics/ML |
| **Workload** | OLTP | OLAP | Big data processing, ML |
| **Data type** | Structured | Mostly structured | Any (structured, semi, unstructured) |
| **Schema** | Schema-on-write | Schema-on-write | **Schema-on-read** |
| **Data age** | Recent / current | Historical | Historical + raw |
| **Storage cost** | High | Medium | Low |
| **Loading process** | Direct by apps | ETL | ELT |

### OLTP vs OLAP

| | OLTP | OLAP |
|---|---|---|
| **Stands for** | Online **Transaction** Processing | Online **Analytical** Processing |
| **Goal** | Run the business | Analyze the business |
| **Queries** | Short, simple (insert/update one order) | Long, complex (total sales per region per year) |
| **Users** | Applications, customers | Analysts, BI tools, data scientists |
| **Lives in** | Database | Data Warehouse |

### Schema-on-Write vs Schema-on-Read

- **Schema-on-write:** you define the structure **before** saving data. Data that doesn't fit is rejected. *(Databases, Warehouses)*
- **Schema-on-read:** you save the data **as-is** and apply a structure only **when you read it**. *(Data Lakes, Spark)*

> 💡 **Bonus: Data Lakehouse.** A modern architecture that combines the **cheap, flexible storage of a data lake** with the **reliability and performance of a data warehouse** (ACID transactions, schemas). Built on table formats like **Delta Lake**, **Apache Iceberg**, and **Apache Hudi**. Databricks is the best-known lakehouse platform.

---

## 7. ETL vs ELT

ETL and ELT describe the **order of data pipeline steps**. They are common patterns, not strict rules tied to one storage technology: the choice depends on the source data, governance needs, and where transformation is most practical.

### ETL: Extract → Transform → Load

*Used with data warehouses (OLAP). Example: **bank transactions**.*

1. **Extract:** collect data from multiple sources
2. **Transform:** clean, filter, and format the data for analytics
3. **Load:** store the cleaned data in the data warehouse

```text
Sources ──► Extract ──► Transform (staging server) ──► Load ──► Data Warehouse
```

### Why did ELT appear?

In the past, extracted data came in **known, structured formats**, so transforming it first was easy. As things evolved, data started coming in **many different formats** (JSON, logs, images, videos...) and in huge volumes. Transforming everything up front became slow and limiting, so the process changed to **ELT**.

### ELT: Extract → Load → Transform

*Used with data lakes. Example: **social media analysis**.*

1. **Extract:** collect data from many different sources
2. **Load:** store the data **raw, as-is** in the data lake
3. **Transform:** clean and process it **later**, only when needed, using the power of the big data platform (e.g. Spark)

```text
Sources ──► Extract ──► Load (raw) ──► Data Lake ──► Transform (when needed)
```

When data comes from many different sources, we need a **data ingestion framework** to collect and move it, e.g. **Azure Data Factory**, AWS Glue, Apache NiFi, or Kafka.

### Comparison

| | ETL | ELT |
|---|---|---|
| **Order** | Transform **before** loading | Transform **after** loading |
| **Target** | Often a Data Warehouse | Often a Data Lake / Lakehouse |
| **Data types** | Mostly structured | Any format |
| **Raw data kept?** | No, only the cleaned version | Yes, raw data is kept and can be re-processed |
| **Flexibility** | Must know the use case up front | Decide how to use the data later |
| **Example** | Bank transactions | Social media analysis |

> 💡 **Tip:** Keeping the raw data (ELT) is very valuable: if you find a bug in your transformation logic or a new business question comes up, you can **re-process the original data** instead of losing it forever.
>
> In practice this is often organized as the **Medallion Architecture**:
> **Bronze** (raw) → **Silver** (cleaned) → **Gold** (business-ready aggregates).

---

## 8. Quick Revision Summary

- **Big Data** = data too large / fast / complex for traditional systems.
- **Vertical scaling** has hardware limits → Big Data uses **horizontal scaling** (distributed systems / clusters).
- **5 V's:** Volume, Velocity, Variety, Veracity, Value.
- A good system is **scalable, fault-tolerant, cost-effective, and secure**.
- **On-prem** = buy & manage yourself; **Cloud** = rent, pay-as-you-go.
- **Database** (OLTP, recent data) → **Data Warehouse** (OLAP, structured history, ETL) → **Data Lake** (any raw data, ELT) → **Lakehouse** (best of both).
- **ETL** transforms before loading; **ELT** loads raw first and transforms later.

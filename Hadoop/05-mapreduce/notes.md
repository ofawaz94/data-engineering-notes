# MapReduce

## Table of Contents

1. [What is MapReduce?](#1-what-is-mapreduce)
2. [The Problem with Traditional (Centralized) Processing](#2-the-problem-with-traditional-centralized-processing)
3. [The Key Observation](#3-the-key-observation)
4. [The Solution: Send the Code to the Data](#4-the-solution-send-the-code-to-the-data)
5. [Two Core Concepts](#5-two-core-concepts)
6. [Real-Life Analogy: Elections](#6-real-life-analogy-elections)
7. [How This Leads to MapReduce](#7-how-this-leads-to-mapreduce)
8. [The Core Building Block: Key-Value Pairs](#8-the-core-building-block-key-value-pairs)
9. [The Full Flow, Step by Step](#9-the-full-flow-step-by-step)
10. [Business Example: Total Sales per City](#10-business-example-total-sales-per-city)
11. [MapReduce in SQL Terms](#11-mapreduce-in-sql-terms)
12. [Important Details Beyond the Diagram](#12-important-details-beyond-the-diagram)
13. [How Hadoop Runs a MapReduce Job](#13-how-hadoop-runs-a-mapreduce-job)
14. [Benefits](#14-benefits)
15. [Trade-offs to Keep in Mind](#15-trade-offs-to-keep-in-mind)
16. [MapReduce vs Spark](#16-mapreduce-vs-spark)
17. [Where You'll See This in Data Engineering](#17-where-youll-see-this-in-data-engineering)
18. [Is MapReduce Still Used? What to Know](#18-is-mapreduce-still-used-what-to-know)
19. [Interview Quick Answers](#19-interview-quick-answers)
20. [Self-Check Questions](#20-self-check-questions)
21. [Quick Revision Summary](#21-quick-revision-summary)

---

## 1. What is MapReduce?

**MapReduce** is a **programming model** designed for **processing large datasets** in a **distributed computing environment**.

Instead of one machine doing all the work, the job is **split into small tasks** that run **in parallel** on many machines, and the results are then **combined** into the final answer.

---

## 2. The Problem with Traditional (Centralized) Processing

In traditional processing, the **program runs on one central machine**:

```text
  Machine 1 ──┐
  (data)      │
  Machine 2 ──┼──── all data sent over the network ────►  Central Machine
  (data)      │                                           (runs the program)
  Machine 3 ──┘
  (data)
```

- All the data stored on other machines has to be **sent over the network** to the central machine **before any processing can start**.
- So **data is moving toward the code**.
- Data is often **huge** (gigabytes, terabytes), and moving it over the network is **slow and expensive**.
- The central machine becomes a **bottleneck**: it receives everything and processes everything **alone**.
- If it fails, the **whole job fails**: it is a **single point of failure**.

---

## 3. The Key Observation

| | Size |
|---|---|
| **The code** | Very small, usually **a few kilobytes** |
| **The data** | Very large, **gigabytes or more** |

So it's **far cheaper to move the small thing (the code)** than the big thing (the data).

---

## 4. The Solution: Send the Code to the Data

```text
            Small program (KBs)
        ┌──────────┼──────────┐
        ▼          ▼          ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │Machine 1│ │Machine 2│ │Machine 3│   each runs the program
   │ + data  │ │ + data  │ │ + data  │   on its OWN local data
   └────┬────┘ └────┬────┘ └────┬────┘
        └──────────┼──────────┘
                   ▼
       Only the small results are sent
          back and combined
```

1. **Copy the small program** to every machine that stores part of the data.
2. Each machine **runs the program on its own local piece** of data.
3. All machines work **at the same time, in parallel**.
4. Only the **small results** are sent back to be **combined**.

| Traditional Processing | MapReduce Approach |
|---|---|
| Data moves to the code | **Code moves to the data** |
| One machine does all the work | **Many machines work in parallel** |
| Huge network traffic | Only small results travel |
| One failure stops everything | One failure only affects its own part |

---

## 5. Two Core Concepts

### 1. Data Locality

**Run the computation on the same machine where the data is already stored**, so the data doesn't travel over the network.

> **Note:** This is why HDFS and MapReduce fit together: HDFS already splits files into **blocks** across the DataNodes, so each task runs **on a node that holds its block**. ➡️ See [02-hdfs-architecture.md](../02-hdfs-architecture.md)

### 2. Local Processing

Each machine **processes only its own portion** of the data **independently**, without waiting for the others.

---

## 6. Real-Life Analogy: Elections

- Votes **aren't shipped** from every polling station to one central office to be counted.
- **Each polling station counts its own votes locally.**
- Only the **totals** are sent to **headquarters**, which adds them up to get the final result.

---

## 7. How This Leads to MapReduce

| Phase | What Happens | In the Election Analogy |
|---|---|---|
| **1. Map** | Each machine processes its **local data** and produces **partial results** | Each polling station counts its own votes |
| **2. Shuffle** | Partial results are **grouped by key** and sent to the right machines | All counts for the same candidate are gathered together |
| **3. Reduce** | The partial results are **combined into the final answer** | Headquarters adds up the totals |

> **Note:** The **shuffle** is the **one step where data does move over the network**.

**Quick example (word count):**

```text
Map:      "big data is big"  →  (big,1) (data,1) (is,1) (big,1)
Shuffle:  big → [1,1]   data → [1]   is → [1]
Reduce:   big → 2       data → 1     is → 1
```

---

## 8. The Core Building Block: Key-Value Pairs

All data moves through MapReduce as **`(key, value)` pairs**.

| Pair | Key | Value |
|---|---|---|
| `("Cairo", 100)` | `Cairo` | `100` |
| `("Goku", 1)` | `Goku` | `1` |

- The **key** decides **which group** the data joins (all pairs with the same key end up together).
- The **value** is what gets **calculated** (added, counted, compared).

> **Note:** Choosing the right key is the main design decision in any MapReduce job. "Total sales per city" means the key is the **city**.

---

## 9. The Full Flow, Step by Step

**Input file:**

```text
Deer Bear River
Car Car River
Deer Car Bear
```

```text
 Input ──► Splitting ──► Mapping ──► (Combiner) ──► Shuffle & Sort ──► Reducing ──► Final Output
            HDFS blocks   parallel    local, optional  network + sort     one per key    back to HDFS
```

| Step | What Happens | Example Output |
|---|---|---|
| **1. Input Splitting** | The file is cut into **splits**, usually one per HDFS block (**128 MB** by default) | Split 1: `Deer Bear River` · Split 2: `Car Car River` · Split 3: `Deer Car Bear` |
| **2. Mapper Phase** | Each split becomes `(word, 1)` pairs. All splits run **in parallel**, with **no communication** between them | `(Deer,1) (Bear,1) (River,1)` |
| **3. Combiner Phase** *(optional)* | A **local mini-reduce** on the mapper's machine **before** sending, to shrink shuffle data | Split 2: `(Car,2) (River,1)` instead of `(Car,1) (Car,1) (River,1)` |
| **4. Shuffle and Sort** | All pairs with the **same key** are sent to the **same reducer** over the network, and sorted by key | `Bear:[1,1]  Car:[2,1]  Deer:[1,1]  River:[1,1]` |
| **5. Reducer Phase** | Each group is **combined** into one result | `Bear 2 · Car 3 · Deer 2 · River 2` |
| **6. Final Output** | Results are **written back to HDFS**, one file per reducer (`part-r-00000`, …) | `Bear 2`, `Car 3`, `Deer 2`, `River 2` |

> **Note:** The final output is in **alphabetical order** (Bear, Car, Deer, River). That's the **Sort** in Shuffle and Sort.

---

## 10. Business Example: Total Sales per City

**Question:** *What are the total sales per city?* The receipts file is split into 2 blocks on 2 machines.

| Block 1 (Machine 1) | Block 2 (Machine 2) |
|---|---|
| Cairo, 100 | Giza, 70 |
| Alex, 50 | Cairo, 20 |
| Cairo, 30 | Alex, 40 |
| | Giza, *NULL* |

**Map:** emit `(city, amount)` and drop rows with no amount.

| Machine 1 Output | Machine 2 Output |
|---|---|
| (Cairo, 100) | (Giza, 70) |
| (Alex, 50) | (Cairo, 20) |
| (Cairo, 30) | (Alex, 40) |
| | ~~(Giza, NULL)~~ → **dropped** |

**Shuffle:**

| Key | Values Gathered |
|---|---|
| Cairo | [100, 30, 20] |
| Alex | [50, 40] |
| Giza | [70] |

**Reduce:**

| Key | Calculation | Result |
|---|---|---|
| Cairo | 100 + 30 + 20 | **150** |
| Alex | 50 + 40 | **90** |
| Giza | 70 | **70** |

**What happened to the data:**

- **Rows:** 7 in → 6 after Map → 3 out. Rows shrink at each stage, like **bronze → silver → gold** in a medallion lakehouse.
- **NULLs:** MapReduce does **nothing automatic**. The rule *we wrote* dropped the NULL row. Skipping it, treating it as 0, or sending it to an error file is a **business decision**.
- **Duplicates:** if a receipt was loaded twice, it would be **counted twice**. MapReduce doesn't deduplicate.

---

## 11. MapReduce in SQL Terms

The whole job above is one SQL query:

```sql
SELECT city, SUM(amount)
FROM receipts
WHERE amount IS NOT NULL   -- Map: filter rows, choose key/value
GROUP BY city;             -- Shuffle: group by key
                           -- SUM(): Reduce
```

| MapReduce | SQL |
|---|---|
| Map | `SELECT` / `WHERE` |
| Shuffle | `GROUP BY` (also `JOIN`) |
| Reduce | `SUM`, `COUNT`, `AVG`, `MAX`… |

> **Note:** This is why **Hive** was created: people wanted to write **SQL** instead of hand-coding Map and Reduce in Java. Hive turns the SQL into MapReduce jobs behind the scenes.

---

## 12. Important Details Beyond the Diagram

### 1. Shuffle = Network + Sort

- The **only step** where data moves **between machines**.
- Keys are also **sorted** along the way.
- It is the **slowest and most expensive** step, in MapReduce **and** in Spark.

### 2. Combiner (Mini-Reduce)

- Runs on the **mapper's machine** to shrink data **before** the shuffle.
- It is **optional**: Hadoop may run it **0, 1, or many times**, so the result must be correct either way.
- That's why a reducer can receive **mixed values** like `("Goku", [1, 2, 1, 2])`: some machines combined, some didn't. The reducer just adds everything: **6**.

| Operation | Combiner Safe? | Why |
|---|---|---|
| **SUM / COUNT** | ✅ Yes | (1+1) + (1+2) = 1+1+1+2 |
| **MAX / MIN** | ✅ Yes | The max of the maxes is the overall max |
| **AVERAGE** | ❌ No | avg(avg(2,4), 6) = avg(3, 6) = **4.5**, but avg(2,4,6) = **4** |

> **Note:** To average correctly, send **`(sum, count)`** pairs and divide **only at the end**. Spark and SQL engines use this same trick.

### 3. Partitioner

- Decides **which reducer** gets each key. By default: `hash(key) mod number_of_reducers`.
- Guarantees that **all values of one key reach the same reducer**, so every total is complete and correct.

### 4. Data Skew

- If **one key has far more data** than the others (e.g. one huge city, or a `NULL` key), **one reducer does most of the work** while the others sit idle.
- The job is only as fast as its **slowest reducer**.
- This is the **#1 real-world cause of slow jobs**, and still a key topic in Spark.

### 5. Disk Between Steps

- Map output is written to **local disk** before the shuffle, and reduce output to **HDFS**.
- A multi-step job = **several MapReduce jobs chained**, each reading from and writing to disk again.
- This is the **main reason MapReduce is slow**.

### 6. Fault Tolerance

- If a machine dies, its task is **re-run on another machine** that holds one of the other copies of the block (HDFS **replication = 3**).
- The job still finishes. ➡️ See [03-hdfs-failures-and-ha.md](../03-hdfs-failures-and-ha.md)

---

## 13. How Hadoop Runs a MapReduce Job

| Component | Role in the Job |
|---|---|
| **HDFS** | Stores the input as **blocks** (128 MB, replication 3) and stores the **final output** |
| **YARN** | The **resource manager**: gives each map and reduce task a machine, CPU and memory ➡️ See [06-yarn.md](../06-yarn.md) |
| **Data Locality** | Map tasks are scheduled **on the node holding the block**, so the code moves, not the data |

```text
 HDFS blocks ──► YARN schedules map tasks on the nodes holding the blocks
             ──► Map + Combine (local disk)
             ──► Shuffle & Sort (network)
             ──► Reduce ──► Output written to HDFS
```

---

## 14. Benefits

- **Much less network traffic:** only the code and small results travel
- **Faster processing** through parallelism
- **Scales horizontally:** more data means adding more machines
- **Fault tolerant:** if one machine fails, only its part is re-run, not the whole job

---

## 15. Trade-offs to Keep in Mind

- **The shuffle still moves data** and is usually the most expensive part of a job, so a good design minimizes it.
- **Distributed systems add complexity:** coordination, failures, and uneven data (**data skew**).
- **For small data, a single machine** is often simpler and faster.
- **Slow:** disk I/O between every step.
- **Verbose:** many lines of Java for simple tasks like a word count.
- **Poor for iterative work** (machine learning loops) and **interactive queries**, because every pass re-reads from disk.
- **Batch only:** no real-time stream processing.

---

## 16. MapReduce vs Spark

| | MapReduce | Spark |
|---|---|---|
| Between steps | Writes to **disk** | Keeps data in **memory** |
| Multi-step jobs | Separate chained jobs | One optimized plan (**DAG**) |
| Speed | Slow | Often **10–100× faster** |
| APIs | Java Map/Reduce classes | SQL, Python (PySpark), Scala |
| Shuffle | Expensive | **Still expensive**: the same idea survives |
| Combiner | Written manually | Automatic in `reduceByKey` (preferred over `groupByKey`) |

> **Note:** In the **Spark UI**, **stages** are the work between shuffles. That's the Map → Shuffle → Reduce idea under new names.

---

## 17. Where You'll See This in Data Engineering

- **Hadoop:** HDFS splits files into blocks across machines, and MapReduce runs tasks on the nodes holding those blocks.
- **Spark:** the scheduler tries to run each task where its data partition lives.
- **Cloud platforms** (Databricks, S3, ADLS): storage and compute are often separate, so data locality is weaker. Fast networks, partitioning, file pruning, and caching make up for that.
- **Every SQL `GROUP BY` and `JOIN`** in a warehouse (BigQuery, Snowflake, Synapse) runs a distributed shuffle under the hood.

---

## 18. Is MapReduce Still Used? What to Know

- **Rarely used for new projects.** Spark and SQL engines replaced it.
- You may meet **legacy MapReduce or Hive jobs** at banks, telecom, or government companies running on-premise Hadoop, usually to **migrate them to Spark**.

| Know It ✅ | Skip It ❌ |
|---|---|
| Map → Shuffle → Reduce flow | Writing Mapper/Reducer classes in Java |
| Why the shuffle is expensive | MapReduce configuration and tuning |
| Combiner, partitioner, data skew | Cluster administration |
| How it maps to SQL `GROUP BY` | Hadoop Streaming internals |
| Why Spark replaced it | |

---

## 19. Interview Quick Answers

| Question | Answer |
|---|---|
| **Explain MapReduce** | Split the data → map each piece to key-value pairs in parallel → shuffle & sort by key → reduce each group → write the output |
| **Why is Spark faster?** | It keeps intermediate data **in memory** and builds **one optimized plan (DAG)** instead of chained, disk-based jobs |
| **What is a combiner?** | A **local mini-reduce** that cuts shuffle data. It's only valid for operations like SUM, COUNT, MAX, MIN |
| **What is data skew?** | One key holds most of the data, so **one reducer becomes the bottleneck** |
| **What is the most expensive step?** | The **shuffle**, because it moves data over the network and sorts it |

---

## 20. Self-Check Questions

1. If we add more machines, which step gets faster, and why?
2. To count **receipts per city** (not the total amount), what does Map emit, and what does Reduce do?
3. With a combiner, what does the split `Goku Goku Goku Vegeta` send to the shuffle?
4. Why can't a combiner compute an **average** directly?
5. If a dedup bug made every receipt load twice, what happens to the totals?

---

## 21. Quick Revision Summary

- **Traditional processing** moves data to the code: slow, a bottleneck, and a single point of failure.
- Code is **small (KBs)**, data is **huge (GBs/TBs)**, so **move the code to the data**.
- **Data locality:** compute where the data is stored. **Local processing:** each machine handles only its own part.
- Everything travels as **`(key, value)` pairs**. The key decides the group.
- **Split** → **Map** (process locally) → **Combine** (optional, local) → **Shuffle & Sort** (group by key, moves data) → **Reduce** (combine) → **Output** (HDFS).
- **Combiner** is safe for SUM/COUNT/MAX/MIN, **not** AVERAGE. The **partitioner** sends each key to one reducer. **Skew** = one overloaded reducer.
- **MapReduce = SQL `GROUP BY`** under the hood. Hive turns SQL into MapReduce.
- **Benefits:** less network traffic, parallel speed, horizontal scaling, fault tolerance.
- **Weakness:** disk between steps makes it slow, which is why **Spark** (in memory, DAG) replaced it. The **shuffle** idea lives on.

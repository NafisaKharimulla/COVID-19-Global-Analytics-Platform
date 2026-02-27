#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Task 12: Resource & Memory Planning 
"""
## 1️.Executor Memory
Executor memory is the RAM allocated to each Spark executor process.  
It is used for shuffle, joins, aggregation, caching, and execution tasks.  
If memory is too low, Spark may spill to disk or throw OOM errors.

---

## 2️. Executor Cores
Executor cores define how many tasks an executor can run in parallel.  
More cores increase parallelism but also increase memory pressure.  
Best practice is typically 3–5 cores per executor.

---

## 3️. Ideal Partition Size (128–256 MB Rule)
Each Spark partition should ideally be between 128MB and 256MB.  
Too small partitions cause scheduling overhead, while too large partitions cause memory pressure.  
Proper partition sizing improves performance and reduces spill.

---

## 4️.YARN Container Allocation
In YARN mode, each executor runs inside a container.  
Container memory = Executor Memory + Memory Overhead (default ~10%).  
If overhead is insufficient, the container may be killed.

---

## 5️. What Happens During OOM
Out Of Memory (OOM) occurs when executor or driver memory is insufficient.  
This may crash executors or fail the entire Spark application.  
It is commonly caused by large joins, shuffle, or improper partition sizing.

---

## 6️. Spill to Disk Behavior
When memory is insufficient during shuffle or aggregation, Spark spills intermediate data to disk.  
Spilling prevents job failure but slows down execution due to disk I/O.  
Proper memory planning and partition tuning reduce spill frequency.
"""


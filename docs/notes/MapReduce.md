# Introduction
Step
---
Input,Split,Map,Shuffle,Reduce,Finalize

Example
---
统计文档中单词的个数。(n个map任务和m个reduce任务??)\
Input:输入一个文档\
Split:按行分割成n个\
Map:统计每行的单词个数，得到n个list\
Shuffle:将n个list中单词打乱分到m个worker上\
Reduce:m个worker进行整合\
Finalize:整合m个worker的结果
---
# Overview
Fault Tolerance
---
* Worker Failure\
  Master会定期pingWorker，如果worker不响应就被判定为failed，于是Master会重置该worker为空闲状态，进行的map/reduce任务也被重置
* Master Failure\
  Master会定期保存checkpoints，如果GG了就根据最新的checkpoints搞一个副本
* Semantics in the Presence of Failures
  1. map任务完成会向master发送R个临时文件名，如果是已完成的任务就忽略，否则存入数据结构
  2. reduce任务完成时，worker会自动将临时输出文件重命名为finaloutput，依靠原子重命名操作保证最终文件系统状态仅包含一次执行reduce任务产生的数据。

Backup Tasks
--- 
如果部分机器磁盘损坏导致读取速度下降，这样会导致CPU、内存等资源的竞争。因此MapReduce会在任务接近完成时，将一些空闲机器去执行那些正在进行的任务。

---
# Refinement
Partitioning Function
---
根据hash函数分给R个reduce任务，或者是根据需求分区，比如每一个reduce任务都是合并链接等等。

Combiner Function
---
在本地就预先合并

Ski


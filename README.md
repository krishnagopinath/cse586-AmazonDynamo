# cse586-AmazonDynamo

This assignment is about implementing a simplified version of Dynamo. (And you might argue that it’s not Dynamo any more ;-) 

The three main pieces that were implemented: 
* Partitioning, 
* Replication, and 
* Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, your implementation should always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value. To accomplish this goal, this document gives you a guideline of the implementation. However, you have freedom to come up with your own design as long as you provide availability and linearizability at the same time (that is, to the extent that the tester can test). The exception is partitioning and replication, which should be done exactly the way Dynamo does.

Ref : https://docs.google.com/document/d/1iHtWvSE2pul7_OhcUMkZ9EMGn1IFhhPwwmz00TDLjP8/edit


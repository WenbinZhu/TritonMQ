## Design

### Producers
1. Produces can generate messages related to specific topics and send to brokers.
2. To provide load balance, producers will first contact Zookeeper, and find a leader in a group by hashing the topic name, and send messages to the leader. The message looks like: `<message, topic, uuid>`.
3. Each message produced by producers is associated with a unique id decided by producer (uuid), which is used to prevent duplication: producers can retry sending a same message if they do not hear a response for some while and the receivers remember each message received with the uuid, and if duplicate messages are received, simple acknowledge it without appending it to its log.
4. The primary will forward the message to backups. To provide high reliability in cases of failure, only when the all the backups acknowledge the message can the primary send response to the produce.
5. Each message forwarded by the primary is also associated with a unique incremental timestamp (ts) decided by primary, to keep the order of messages in case the primary fails and a backup becomes the new primary. The message stored on primary and backups looks like:  `<message, topic, uuid, ts>`.
6. The producer will get a watcher from ZooKeeper. If it is noticed that the leader broker fails, it will send to the new leader in the group.
7. We provide a consistency semantics that messages from a producer be kept in order, but messages from different producers can be in arbitrary (just like sequential consistency). However, the producer can specify a `max_in_flight` configuration for each topic, indicating how many messages can be sent asynchronously at a time to loose the order of the messages from a producer.

### Consumers

1. Consumers can subscribe to some topics and get the messages related to the topics. We plan to use the push model for consumers, namely, brokers push messages to consumers instead of consumers poll from brokers. The push model is better for our project because all the messages are in-memory, and the broker should deliver and purge the messages as soon as possible to save memory space.
2. We plan to first develop a one-to-all semantics for consumers: a message will be sent to all consumers (e.g. a chat room application), we can add a one-to-one semantics (a message is sent to any and only one consumer who subscribed to the topic) later if time allows.
3. On start up, the consumer will register itself to Zookeeper. Each consumer is also associated with an offset registry, indicating the last consumed offset in a topic.
4. A new consumer should only be able to receive new messages produced after it joins.
5. We provide a semantics that the a consumer receives the messages of a topic in a strictly identical order as in the brokers. So the broker will advance the offset only when a consumer acknowledges a message.

### Brokers

1. Brokers store  and manage messages from producer, replicate messages to other broker servers and push messages to consumers.
2. To makes things easier, we consider using in-memory storage only and fix the max number of brokers (just like lab 3, fix max number but each broker server can up and down).
3. Users can specify number of replications in a global config file, and Zookeeper is used for registering the brokers, monitoring, and electing leaders if some server in a replica group fails (group members are also fixed).
4. The primary brokers will record the offsets of each consumer w.r.t. to the topics and write to ZooKeeper periodically  in case of primary failure.
5. The primary brokers has a thread periodically purging the messages. A message can be disposed only if all registered consumers for its topic have acknowledged (i.e. all the offsets are larger than this message). The primary also send purge messages to backups instruction them to delete those messages.
6. Once the ZooKeeper detects the primary down, it select another backup as new primary and any other nodes in the same group will be notified.
7. In case of failure, migrations are needed to prevent data loss. We follow the same assumption as in lab3 that in a replica group, no other nodes will fail before migration completes.



## Implementation

ZooKeeper should store the following information

1. Number of groups and their nodes' addresses. 
2. For each topic, the clients connected to this topic and their offset (position).

ZooKeeper should have following structure

```
/groups/GROUP_ID
	/replica/NODE_ADDR (ephemeral)
	/primary => NODE_ADDR
	/topics/TOPIC_NAME
		/CLIENT_ADDR (ephemeral) => OFFSET
```


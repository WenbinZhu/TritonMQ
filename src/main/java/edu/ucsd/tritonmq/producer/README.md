- Curator 和 Armeria 都依赖`guava`, 但依赖不同版本，且两个版本不兼容。因此务必使用3.3.0版本的Curator以及更新机器上的zookeeper版本到3.5.2 
(卸载原来brew装的3.4.9，再下3.5.2源码)

- Zookeeper地址:
    - /primary/0 -> localhost:9001
    - /primary/1 -> localhost:9005
    - /replica/0 -> localhost:9001, localhost:9002, localhost:9003
    - /replica/1 -> localhost:9004, localhost:9005, localhost:9006
    - 依此类推

- Junit 无法测试异步方法，简单做法直接在`main`里面测试这类问题，或者在Junit的test case 中加上countdownlatch
    
- Broker assume每个group至少启动一个节点，因此producer启动前`/primary`和`/replica`下的所有grouId节点都要建立好

- Broker 最好在static块中创建`/primary/group_id` 和 `/replica/group_id`，否则会有concurrent的重复创建zookeeper节点的问题

- Broker 还要给consumer创建好`/consumer`节点

- Assume producer/broker/consumer与zookeeper的连接都不会断，除非producer/broker/consumer自己挂了

- 每个Broker监控自己是否成为primary, 参考`LeaderElectionExample.java`

- 使用Curator进行监听，最好使用listener， 比如`PathChildrenCacheListener`， 而不是watcher，因为watcher是one-time-trigger,
后续发生多次事件的话只会监听到第一次，除非重新绑定watcher

- Consumer 注册结构 /subscribe/topic1/consumer1

- 序列化：
```
ByteArrayOutputStream bao = new ByteArrayOutputStream();
ObjectOutputStream output = new ObjectOutputStream(bao);
output.writeObject(record);
byte[] bytes = bao.toByteArray();
send(ByteBuffer.wrap(bytes), future);
```
- Zookeeper地址:
    - /primary/0 -> 127.0.0.1:9001
    - /primary/1 -> 127.0.0.1:9005
    - /replica/0 -> 127.0.0.1:9001, 127.0.0.1:9002, 127.0.0.1:9003
    - /replica/1 -> 127.0.0.1:9004, 127.0.0.1:9005, 127.0.0.1:9006
    - 依此类推
    
- Broker assume每个group至少启动一个节点，因此producer启动前`/primary`和`/replica`下的所有grouId节点都要建立好

- 每个Broker监控自己是否成为primary, 参考`LeaderElectionExample.java`

- 序列化：
```
ByteArrayOutputStream bao = new ByteArrayOutputStream();
ObjectOutputStream output = new ObjectOutputStream(bao);
output.writeObject(record);
byte[] bytes = bao.toByteArray();
```
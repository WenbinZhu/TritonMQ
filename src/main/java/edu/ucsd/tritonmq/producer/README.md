- Zookeeper地址:
    - /primary/0
    - /primary/1
    - /replica/0
    - /replica/1
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
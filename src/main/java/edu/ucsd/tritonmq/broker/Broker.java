package edu.ucsd.tritonmq.broker;

import com.linecorp.armeria.common.SerializationFormat;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.thrift.THttpService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.ZKPaths;
import org.eclipse.jetty.util.ConcurrentHashSet;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static edu.ucsd.tritonmq.common.Utils.*;


/**
 * Created by dangyi on 5/28/17.
 */
public class Broker {
    protected int groupId;
    private int retry;
    private int timeout;
    private int port;
    private String host;
    private String address;
    private String zkAddr;
    private Server server;
    private LeaderLatch latch;
    private volatile boolean started;
    protected volatile long timestamp;
    protected CuratorFramework zkClient;
    protected volatile boolean isPrimary;
    protected Set<String> backups;
    protected Map<UUID, String> requests;
    protected Map<String, Map<String, Integer>> offsets;
    protected Map<String, Deque<BrokerRecord<?>>> records;


    /**
     * Create a new broker in a specific group.
     *
     * @param groupId which group this broker belongs to
     * @param configs broker configs including zk address etc
     */
    public Broker(int groupId, Properties configs) {
        int nr = (Integer) configs.get("retry");
        this.groupId = groupId;
        this.started = false;
        this.isPrimary = false;
        this.timestamp = (long) 0;
        this.host = configs.getProperty("host");
        this.port = (Integer) configs.get("port");
        this.address = host + ":" + port;
        this.zkAddr = configs.getProperty("zkAddr");
        this.timeout = (Integer) configs.get("timeout");
        this.retry = Integer.min(5, Integer.max(nr, 0));
        this.backups = new ConcurrentHashSet<>();
        this.records = new ConcurrentHashMap<>();
        this.offsets = new ConcurrentHashMap<>();
        this.requests = new ConcurrentHashMap<>();
        this.zkClient = initZkClient(Second, 1, this.zkAddr, 100, 100);

        assert zkClient != null;
        assert zkClient.getState() == CuratorFrameworkState.STARTED;
    }

    /**
     * Primary listens to replica come and leave events
     *
     */
    private void addListener(String path) {
        PathChildrenCacheListener plis = (client, event) -> {

            switch (event.getType()) {
                case CHILD_ADDED: {
                    String backup = new String(client.getData().forPath(event.getData().getPath()));
                    System.out.println(backup + " added to group " + groupId);

                    if (!backup.equals(address)) {
                        backups.add(backup);
                        new MigrateThread(backup).start();
                    }

                    break;
                }

                case CHILD_REMOVED: {
                    String backup = ZKPaths.getNodeFromPath(event.getData().getPath());
                    backups.remove(backup);
                    break;
                }
            }
        };

        try {
            PathChildrenCache cache = new PathChildrenCache(zkClient, path, false);
            cache.start();
            cache.getListenable().addListener(plis);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Register self to ZooKeeper and listen to the leader election
     *
     */
    private void register() {
        try {
            String path = new File(ReplicaPath, String.valueOf(groupId)).toString();

            if (zkClient.checkExists().forPath(path) == null)
                zkClient.create().creatingParentContainersIfNeeded().forPath(path);

            latch = new LeaderLatch(zkClient, path, address);
            latch.addListener(new LeaderLatchListener() {

                public void notLeader() {
                    System.out.println("group " + String.valueOf(groupId) + ", " + address + ": not leader");
                }

                public void isLeader() {
                    System.out.println("group " + String.valueOf(groupId) + ", " + address + ": is leader");
                    String primary = new File(PrimaryPath, String.valueOf(groupId)).toString();

                    try {
                        if (zkClient.checkExists().forPath(primary) == null)
                            zkClient.create().creatingParentContainersIfNeeded().forPath(primary);

                        zkClient.setData().forPath(primary, address.getBytes());

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }

                    addListener(path);
                    timestamp = largestTimeStamp();
                    isPrimary = true;
                }
            });

            latch.start();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Start the broker. It should be able to serve requests upon return.
     *
     */
    public synchronized void start() {
        if (started)
            return;

        register();

        InetSocketAddress addr = new InetSocketAddress(host, port);
        ServerBuilder sb = new ServerBuilder();
        sb.port(addr, SessionProtocol.HTTP).serviceAt("/",
                THttpService.of(new BrokerHandler(this), SerializationFormat.THRIFT_BINARY));
        server = sb.build();

        server.start();
        new DeliverThread(this).start();

        started = true;
    }

    /**
     *
     * @return the address this broker listens
     */
    public String getListenAddr() {
        throw new NotImplementedException();
    }

    protected synchronized long largestTimeStamp() {
        long ts = 0;

        synchronized (records) {
            for (Map.Entry<String, Deque<BrokerRecord<?>>> entry : records.entrySet()) {
                for (BrokerRecord<?> record : entry.getValue()) {
                    ts = Math.max(ts, record.timestamp());
                }
            }
        }

        return ts + 1;
    }

    protected synchronized long incrementTs() {
        return timestamp++;
    }
}

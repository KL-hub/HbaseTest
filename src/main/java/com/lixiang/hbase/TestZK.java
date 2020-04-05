package com.lixiang.hbase;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Description //TODO
 * @Author 李项
 * @Date 2020/4/5
 * @Version 1.0
 */
public class TestZK implements Watcher {

    private ZooKeeper zookeeper;
    /**
     *      * 超时时间
     *     
     */
    private static final int SESSION_TIME_OUT = 2000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            System.out.println("Watch received event");
            countDownLatch.countDown();
        }
    }


    /**
     * 连接zookeeper
     *     * @param host
     *     * @throws Exception
     *    
     */
    public void connectZookeeper(String host) throws Exception {
        zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
        countDownLatch.await();
        System.out.println("zookeeper connection success");
    }

    /**
     *     * 创建节点
     *     * @param path
     *     * @param data
     *     * @throws Exception
     *    
     */
    public String createNode(String path, String data) throws Exception {
        return this.zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     *     * 获取路径下所有子节点
     *     * @param path
     *     * @return
     *     * @throws KeeperException
     *     * @throws InterruptedException
     *    
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        List<String> children = zookeeper.getChildren(path, false);
        return children;
    }

    public static void main(String[] args) throws Exception {
        TestZK zookeeper = new TestZK();
        zookeeper.connectZookeeper("lixiang.ac.cn:2181");

        List<String> children = zookeeper.getChildren("/");
        System.out.println(children);

    }
}

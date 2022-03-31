package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public LeaderElection(ZooKeeper zooKeeper){
        this.zooKeeper = zooKeeper;
    }



    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Node Name :" + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reElectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZNodeName = "";
        
        while(predecessorStat == null){
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);
            if(smallestChild.equals(currentZnodeName)){
                System.out.println("Current Node is the Leader node!");
                return;
            } else {
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZNodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNodeName, this);
            }
        }
        System.out.println("Watching node " + predecessorZNodeName);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch(watchedEvent.getType()){
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
                break;
        }
    }
}

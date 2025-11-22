/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
import java.io.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For simplicity, so far all the code in a single class (including the callbacks).
//		You are free to break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback
{
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isManager=false;
    boolean initialized=false;

    String myWorkerZNode = null;
    AtomicInteger assignmentIndex = new AtomicInteger(0);

    DistProcess(String zkhost)
    {
        zkServer=zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
    {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
    }

    void initialize()
    {
        try
        {
            runForManager();	// See if you can become the manager (i.e, no other manager exists)
            isManager=true;

            if (zk.exists("/dist03/assignment", false) == null) // create assignment directory if non-existent yet
            {
                zk.create("/dist03/assignment", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            getTasks(); // Install monitoring on any new tasks that will be created.
            getWorkers(); // monitor new workers
                                    // TODO monitor for worker tasks?
        }
        catch(NodeExistsException nee)
        { 
            isManager=false; 
            InitializeWorker();
        } // TODO: What else will you need if this was a worker process?
        catch(UnknownHostException uhe)
        { 
            System.out.println(uhe); 
        }
        catch(KeeperException ke)
        { 
            System.out.println(ke); 
        }
        catch(InterruptedException ie)
        { 
            System.out.println(ie); 
        }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));

    }

    // Manager fetching task znodes...
    void getTasks()
    {
        zk.getChildren("/dist03/tasks", this, this, null);  
    }

    // Try to become the manager.
    void runForManager() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create("/dist03/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);

        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our intialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false) 
            {
                initialize();
                initialized = true;
            }
        }

        if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged)
        {
            if(e.getPath() != null && e.getPath().equals("/dist03/tasks")) // fetch new tasks
            {
                getTasks();
            }
            else if(e.getPath() != null && e.getPath().equals("/dist03/workers")) // fetch new workers
            {
                getWorkers();
            }
            else if (myWorkerZNode != null && e.getPath() != null && e.getPath().equals("/dist03/assign/" + myWorkerZNode)) // this process' assignment directory changed
            {
                // reinstall watch and let processResult handle work
                zk.getChildren(e.getPath(), this, this, null);
            }
            else if (e.getPath() != null && e.getPath().startsWith("/dist03/assign/"))
            {
                zk.getChildren(e.getPath(), this, this, null);
            }
        }
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children)
    {

        // !! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        // This logic is for manager !!
        //Every time a new task znode is created by the client, this will be invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        //		Also have a mechanism to assign these tasks to a "Worker" process.
        //		The worker must invoke the "compute" function of the Task send by the client.
        //What to do if you do not have a free worker process?

        System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);

        if ("/dist03/workers".equals(path)) // if workers changed
        {
            System.out.println("Current workers:");
            for (String child : children) 
            {
                System.out.println(" - " + child);
                try 
                {
                    byte[] data = zk.getData("/dist03/workers/" + child, false, null);
                    if (data != null) 
                    {
                        System.out.println("   data: " + new String(data));
                    }
                } 
                catch (KeeperException ke) 
                { 
                    System.out.println(ke); 
                }
                catch (InterruptedException ie) 
                { 
                    System.out.println(ie); 
                }
            }

            if (isManager)
            {
                watchAllAssigns(children);
                assignTasks();
            }
            return;
        }

        if (isManager && path != null && path.startsWith("/dist03/assignment")) // manager will assign tasks when child list changes
        {
            assignTasks();
            return;
        }

        // if process is worker and has assignments
        if (!isManager && myWorkerZNode != null && path != null && path.equals("/dist03/assign/" + myWorkerZNode))
        {
            if (children == null || children.size() == 0)
            {
                return;
            }
            String assignment = children.get(0);
            try
            {
                String assignmentNode = "/dist03/assign/" + myWorkerZNode + "/" + assignment;
                byte[] taskSerialized = zk.getData(assignmentNode, false, null);

                // deserialize
                ByteArrayInputStream inputStream = new ByteArrayInputStream(taskSerialized);
                ObjectInput input = new ObjectInputStream(inputStream);
                DistTask diskTask = (DistTask) input.readObject();

                diskTask.compute();

                // serialize result
                ByteArrayOutputStream bOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream oOutputStream = new ObjectOutputStream(bOutputStream);
                oOutputStream.writeObject(diskTask); 
                oOutputStream.flush();
                byte[] resultBytes = bOutputStream.toByteArray();

                // result written as child of task node
                if (zk.exists("/dist03/tasks/" + assignment + "/result", false) == null)
                {
                    zk.create("/dist03/tasks" + assignment + "/result", resultBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                //empty assignment node
                try
                {
                    zk.delete(assignmentNode, -1); 
                }
                catch (KeeperException.NoNodeException nne) {}
            }
            catch(NodeExistsException nee){System.out.println(nee);}
            catch(KeeperException ke){System.out.println(ke);}
            catch(InterruptedException ie){System.out.println(ie);}
            catch(IOException io){System.out.println(io);}
            catch(ClassNotFoundException cne){System.out.println(cne);}

            return;
        }

        // tasks have changed
        if ("/dist03/tasks".equals(path) && isManager)
        {
            // try to assign tasks to idle workers
            assignTasks();
            return;
        }

        // fallback
        for(String c: children)
        {
            System.out.println("Unhandled case with child:" + c);
        }

        // for(String c: children)
        // {
        //     System.out.println(c);
        //     try
        //     {
        //         //TODO There is quite a bit of worker specific activities here,
        //         // that should be moved done by a process function as the worker.

        //         //TODO!! This is not a good approach, you should get the data using an async version of the API.
        //         byte[] taskSerial = zk.getData("/dist03/tasks/"+c, false, null);

        //         // Re-construct our task object.
        //         ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
        //         ObjectInput in = new ObjectInputStream(bis);
        //         DistTask dt = (DistTask) in.readObject();

        //         //Execute the task.
        //         //TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
        //         dt.compute();
                
        //         // Serialize our Task object back to a byte array!
        //         ByteArrayOutputStream bos = new ByteArrayOutputStream();
        //         ObjectOutputStream oos = new ObjectOutputStream(bos);
        //         oos.writeObject(dt); oos.flush();
        //         taskSerial = bos.toByteArray();

        //         // Store it inside the result node.
        //         zk.create("/dist03/tasks/"+c+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //         //zk.create("/distXX/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //     }
        //     catch(NodeExistsException nee){System.out.println(nee);}
        //     catch(KeeperException ke){System.out.println(ke);}
        //     catch(InterruptedException ie){System.out.println(ie);}
        //     catch(IOException io){System.out.println(io);}
        //     catch(ClassNotFoundException cne){System.out.println(cne);}
        // }
    }

    public void InitializeWorker()
    {
        try 
        {
            String parent = "/dist03/workers";
            if (zk.exists(parent, false) == null)  // create persistent parent node if missing
            {
                zk.create(parent, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String myNode = zk.create(parent + "/worker-", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);  // create ephemeral sequential child node
            System.out.println("DISTAPP : Registered worker node: " + myNode);

            // sequential node num
            myWorkerZNode = myNode.substring(myNode.lastIndexOf('/') + 1);

            //  create my assign dir if it doesn't exist
            if (zk.exists("/dist03/assign", false) == null) 
            {
                zk.create("/dist03/assign", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String myAssignPath = "/dist03/assign/" + myWorkerZNode;
            // create the worker's assignment dir if it doesn't exist
            if (zk.exists(myAssignPath, false) == null) 
            {
                zk.create(myAssignPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            // watch my assign dir for new assignments
            zk.getChildren(myAssignPath, this, this, null);
        }
        catch(KeeperException ke)
        { 
            System.out.println("Zookeeper exception occured:" + ke); 
        }
        catch(InterruptedException ie)
        { 
            System.out.println(ie); 
        }
    }

    public void getWorkers()
    {
        zk.getChildren("/dist03/workers", this, this, null); 
    }

    // makes manager watch every worker's assignment directory
    public void watchAllAssigns(List<String> workers)
    {
        for (String worker : workers)
        {
            try
            {
                String p = "/dist03/assign/" + worker;
                if (zk.exists(p, false) == null) 
                {
                    // create worker-specific assign dir if missing
                    zk.create(p, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                // async watch: when children change, processResult will be called for path p
                zk.getChildren(p, this, this, null);
            }
            catch(KeeperException ke)
            { 
                System.out.println(ke); 
            }
            catch(InterruptedException ie)
            { 
                System.out.println(ie); 
            }
        }
    }

    public void assignTasks()
    {
        if (!isManager)
        {
            return;
        }

        List<String> workers;
        List<String> tasks;
        try 
        {
            // get current workers and tasks
            workers = zk.getChildren("/dist03/workers", false);
            tasks = zk.getChildren("/dist03/tasks", false);
        } 
        catch (Exception e) 
        {
            System.out.println(e);
            return;
        }

        if (tasks == null || tasks.size() == 0) 
        {
            return;
        }
        if (workers == null || workers.size() == 0) 
        {
            return;
        }

        Map<String, List<String>> assignMap = new HashMap<>(); // worker, list of assignments
        try 
        {
            for (String worker : workers) 
            {
                String assignmentPath = "/dist03/assign/" + worker;
                List<String> assignmentChildren = Collections.emptyList();
                if (zk.exists(assignmentPath, false) != null) 
                {
                    assignmentChildren = zk.getChildren(assignmentPath, false);
                }
                assignMap.put(worker, assignmentChildren);
            }
        } 
        catch (Exception e) 
        {
            System.out.println(e);
            return;
        }

        // build list of idle workers
        List<String> idle = new ArrayList<>();
        for (String worker : workers) {
            List<String> achildren = assignMap.get(worker);
            if (achildren == null || achildren.size() == 0) 
            {
                idle.add(worker);
            }
        
        }
        if (idle.isEmpty()) // no idle workers
        {
            return;
        }

        // assign tasks to idle workers (one per worker)
        for (String task : tasks)
        {
            try {
                if (zk.exists("/dist03/tasks/" + task + "/result", false) != null) // already completed tasks
                { 
                    continue;
                }

                // skip if already assigned to any worker
                boolean alreadyAssigned = false;
                for (String worker : workers) 
                {
                    String potentialAssignment = "/dist03/assign/" + worker + "/" + task;
                    if (zk.exists(potentialAssignment, false) != null) 
                    { 
                        alreadyAssigned = true; 
                        break; 
                    }
                }
                if (alreadyAssigned) continue;

                if (idle.isEmpty()) break; // no idle worker left

                // pick next idle worker (simple round-robin on idle list)
                int index = assignmentIndex.getAndIncrement() % idle.size();
                String chosen = idle.remove(index);

                String workerAssignPath = "/dist03/assign/" + chosen; // create worker assignment dir if non existent
                if (zk.exists(workerAssignPath, false) == null) 
                {
                    zk.create(workerAssignPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                // read task data
                byte[] taskSerial = zk.getData("/dist03/tasks/"+task, false, null);

                // create assignment node under worker's assign dir
                String assignPath = workerAssignPath + "/" + task;
                if (zk.exists(assignPath, false) == null) 
                {
                    zk.create(assignPath, taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                System.out.println("Assigned task " + task + " to worker " + chosen);

            } 
            catch (Exception e) 
            {
                System.out.println(e);
            }
        }
    }

    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        //Replace this with an approach that will make sure that the process is up and running forever.
        Thread.sleep(20000); 
    }
}

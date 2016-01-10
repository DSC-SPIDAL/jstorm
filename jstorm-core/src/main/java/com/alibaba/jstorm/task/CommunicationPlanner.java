package com.alibaba.jstorm.task;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.schedule.Assignment;
import com.alibaba.jstorm.schedule.default_assign.ResourceWorkerSlot;
import com.alibaba.jstorm.utils.Thrift;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This class is used as the API for collective communications.
 */
public class CommunicationPlanner {
    private Logger LOG = LoggerFactory.getLogger(CommunicationPlanner.class);

    /**
     * TaskId to Worker mapping
     */
    private ConcurrentHashMap<Integer, ResourceWorkerSlot> taskNodePort;

    /**
     * Topology configuration
     */
    private Map conf = null;

    /**
     * Create the planner from the WorkerData
     * @param workerData WorkerData of the worker where this task resides
     */
    public CommunicationPlanner(WorkerData workerData) {
        try {
            this.taskNodePort = getTaskNodes(workerData);

            StringBuilder sb = new StringBuilder("");
            for (Map.Entry<Integer, ResourceWorkerSlot> we : taskNodePort.entrySet()) {
                sb.append(we.getKey()).append(": ").append(we.getValue().getHostname()).append(", ").append(we.getValue().getPort()).append("  ");
            }
            LOG.info("TaskNodePort: " + sb.toString());
        } catch (Exception e) {
            String s = "Failed to get the task and node data";
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }

        this.conf = workerData.getStormConf();

        /*
         * Node id to task id mapping of this topology
         */
        Map<String, List<Integer>> nodeTasks = tasksPerNode(this.taskNodePort);
        StringBuilder sb = new StringBuilder("");
        for (Map.Entry<String, List<Integer>> we : nodeTasks.entrySet()) {
            sb.append(we.getKey()).append(": ");
            for (Integer t : we.getValue()) {
                sb.append(t).append(",");
            }
        }
        LOG.info("NodeTasks: " + sb.toString());

        sb = new StringBuilder("");
        for (Map.Entry<String, List<Integer>> entry : workerData.getComponentToSortedTasks().entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(" , ");
        }
        LOG.info("Component Tasks: " + sb.toString());
    }

    /**
     * Get the tasks to which we need to broadcast.
     * 1. We will send the data to 1 worker in each node. The task in that worker will
     * broadcast the message to other workers in the same machine
     *
     * @param context topology context
     * @param correspondingTaskId this task id
     * @return map of streams to tasks
     */
    public DownstreamTasks getDownStreamTasks(TopologyContext context, int correspondingTaskId) {
        String componentId = context.getComponentId(correspondingTaskId);
        DownstreamTasks downStreamTasks = new DownstreamTasks();
        Map<String, Map<String, Grouping>> targets = context.getTargets(componentId);
        int taskId = context.getThisTaskId();
        Boolean useFlatTree = (Boolean) conf.get(Config.COLLECTIVE_USE_FLAT_TREE);
        Boolean usePipeLine = (Boolean) conf.get(Config.COLLECTIVE_USE_PIPE_LINE);
        // assume we are in the top broadcasting, we build the tree from this node
        String correspondingTaskNode = getNodeId(correspondingTaskId);
        int correspondingTaskPort = getWorkerPort(correspondingTaskId);
        for (Map.Entry<String, Map<String, Grouping>> e : targets.entrySet()) {
            String stream = e.getKey();

            TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings = null;
            Map<String, Grouping> streamTargets = targets.get(stream);
            for (Map.Entry<String, Grouping> entry : streamTargets.entrySet()) {
                Set<Integer> allTasks = new TreeSet<Integer>();
                Set<Integer> shuffleRootTasks = new TreeSet<Integer>();
                Grouping g = entry.getValue();
                String id = entry.getKey();
                // first lets process ALL operation
                // we need to get all the tasks which are bounded by ALL operation to this task.
                // then we will add those tasks to a common pool and build a tree
                // we will use this tree for calculating the downstream tasks
                if (Grouping._Fields.ALL.equals(Thrift.groupingType(g))) {
                    List<Integer> bCastTasks = context.getComponentTasks(id);
                    allTasks.addAll(bCastTasks);

                    List<Integer> ts = context.getComponentTasks(componentId);
                    TreeSet<Integer> rootTasks = new TreeSet<Integer>(ts);
                    // go through the component tasks list to figure out the correct locations
                    mappings = exctractWorkerMappings(allTasks, taskNodePort);
                    LOG.info("Task ID passed: {}", correspondingTaskId);
                    if (usePipeLine != null && usePipeLine) {
                        CommunicationPipeLine pipeLine = new CommunicationPipeLine(conf, correspondingTaskId, correspondingTaskNode, correspondingTaskPort, mappings);
                        downStreamTasks.addPipeLine(new GlobalTaskId(correspondingTaskId, stream), pipeLine);
                    } else {
                        CommunicationTree allTree;
                        if (useFlatTree != null && useFlatTree) {
                            allTree = new CommunicationTree(conf, correspondingTaskId, correspondingTaskNode, correspondingTaskPort, mappings, true, true);
                        } else {
                            allTree = new CommunicationTree(conf, correspondingTaskId, correspondingTaskNode, correspondingTaskPort, mappings, true, false);
                        }

                        GlobalTaskId streamId = new GlobalTaskId(correspondingTaskId, stream);
                        LOG.info("TaskId: {}, StreamId: {}, Tree: {}", taskId, streamId, allTree.printTree());
                        // query the tree to get the broad cast tasks
                        downStreamTasks.addCollectiveTree(streamId, allTree);
                    }
                } /*else if (Grouping._Fields.SHUFFLE.equals(Thrift.groupingType(g))) {
                    // lets process the shuffle operation
                    List<Integer> bCastTasks = context.getComponentTasks(id);
                    shuffleRootTasks.addAll(bCastTasks);

                    List<Integer> ts = context.getComponentTasks(componentId);
                    Set<Integer> shuffleTasks = new TreeSet<Integer>();
                    shuffleTasks.addAll(ts);
                    // go through the component tasks list to figure out the correct locations
                    mappings = exctractWorkerMappings(shuffleTasks, taskNodePort);
                    LOG.info("SHUFFLE Collective tree");
                    CommunicationTree shuffleTree = new CommunicationTree(conf, shuffleRootTasks, mappings, false);
//                    Set<Integer> shuffleChildTasks = shuffleTree.getChildTasks(taskId);
                    downStreamTasks.addCollectiveTree(new GlobalStreamId(componentId, stream), shuffleTree);
                }*/
            }
        }


        // assume we are in the other end of the tree
        // we start from the source node and try to build the tree
        Map<GlobalStreamId, Grouping> sources = context.getThisSources();
        for (Map.Entry<GlobalStreamId, Grouping> e : sources.entrySet()) {
            GlobalStreamId globalStreamId = e.getKey();
            Grouping g = e.getValue();
            String stream = globalStreamId.get_streamId();
            String sourceComponentId = globalStreamId.get_componentId();
            TreeSet<Integer> sourceTasks = new TreeSet<Integer>(context.getComponentTasks(sourceComponentId));
            TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings = null;
            GlobalStreamId sourceGlobalStreamId = new GlobalStreamId(sourceComponentId, stream);

            // first lets calculate for ALL operation
            if (Grouping._Fields.ALL.equals(Thrift.groupingType(g))) {
                TreeSet<Integer> allTasks = new TreeSet<Integer>();
                List<Integer> ts = context.getComponentTasks(componentId);
                allTasks.addAll(ts);
                mappings = exctractWorkerMappings(allTasks, taskNodePort);

                for (int sourceTask : sourceTasks) {
                    int rootTaskPort = getWorkerPort(sourceTask);
                    String rootTaskNode = getNodeId(sourceTask);
                    LOG.info("Task ID passed: {}", rootTaskPort);
                    if (usePipeLine != null && usePipeLine) {
                        CommunicationPipeLine pipeLine = new CommunicationPipeLine(conf, sourceTask, rootTaskNode, rootTaskPort, mappings);
                        downStreamTasks.addPipeLine(new GlobalTaskId(sourceTask, stream), pipeLine);
                    } else {
                        CommunicationTree tree;
                        if (useFlatTree != null && useFlatTree) {
                            tree = new CommunicationTree(conf, sourceTask, rootTaskNode, rootTaskPort, mappings, true, true);
                        } else {
                            tree = new CommunicationTree(conf, sourceTask, rootTaskNode, rootTaskPort, mappings, true, false);
                        }
                        LOG.info("TaskId: {}, StreamID: {}, Tree: {}", taskId, sourceGlobalStreamId, tree.printTree());
                        // query the tree to get the broadcast tasks
                        downStreamTasks.addCollectiveTree(new GlobalTaskId(sourceTask, stream), tree);
                    }
                }
            } /*else if (Grouping._Fields.SHUFFLE.equals(Thrift.groupingType(g))) {
                // calculate for SHUFFLE operation
                Set<Integer> componentTasks = new HashSet<Integer>(context.getComponentTasks(componentId));
                Set<Integer> shuffleTasks = new TreeSet<Integer>();
                List<Integer> ts = context.getComponentTasks(sourceComponentId);
                shuffleTasks.addAll(ts);
                mappings = exctractWorkerMappings(shuffleTasks, taskNodePort);
                LOG.info("SHUFFLE Collective tree");
                CommunicationTree tree = new CommunicationTree(conf, componentTasks, mappings, false);
                // query the tree to get the broadcast tasks
//                Set<Integer> childTasks = tree.getChildTasks(taskId);
                downStreamTasks.addCollectiveTree(sourceGlobalStreamId, tree);
            }*/
        }

        StringBuilder sb = new StringBuilder("");
        for (Map.Entry<GlobalTaskId, Set<Integer>> e : downStreamTasks.allDownStreamTasks(taskId).entrySet()) {
            sb.append(e.getKey()).append(":");
            for (Integer t : e.getValue()) {
                sb.append(" ").append(t);
            }
            sb.append("\n");
        }
        LOG.info("Downstream tasks ({}, {}) {}", componentId, context.getThisTaskId(), sb.toString());

        return downStreamTasks;
    }

    /**
     * Get the list of task Ids for each node sorted in ascending order
     * @return map with nodeid, list of tasks in ascending order
     */
    private static Map<String, List<Integer>> tasksPerNode(ConcurrentHashMap<Integer, ResourceWorkerSlot> taskNodePort) {
        Map<String, List<Integer>> nodeTasks = new HashMap<String, List<Integer>>();

        for (Map.Entry<Integer, ResourceWorkerSlot> taskEntries : taskNodePort.entrySet()) {
            String nodeId = taskEntries.getValue().getNodeId();
            int taskId = taskEntries.getKey();

            List<Integer> taskList;
            if (nodeTasks.containsKey(nodeId)) {
                taskList = nodeTasks.get(nodeId);
            } else {
                taskList = new ArrayList<Integer>();
                nodeTasks.put(nodeId, taskList);
            }
            taskList.add(taskId);
        }

        for (Map.Entry<String, List<Integer>> nodeTasksEntries : nodeTasks.entrySet()) {
            List<Integer> taskList = nodeTasksEntries.getValue();
            Collections.sort(taskList);
        }

        return nodeTasks;
    }

    private ConcurrentHashMap<Integer, ResourceWorkerSlot> getTaskNodes(WorkerData workerData) throws Exception {
        ConcurrentHashMap<Integer, ResourceWorkerSlot> taskNodePort = new ConcurrentHashMap<Integer, ResourceWorkerSlot>();

        StormClusterState zkCluster =  workerData.getZkCluster();
        String topologyId = workerData.getTopologyId();
        Assignment assignment = zkCluster.assignment_info(topologyId, null);
        if (assignment == null) {
            String errMsg = "Failed to get Assignment of " + topologyId;
            LOG.error(errMsg);
            // throw new RuntimeException(errMsg);
            return null;
        }

        Set<ResourceWorkerSlot> workers = assignment.getWorkers();
        if (workers == null) {
            String errMsg = "Failed to get taskToResource of "
                    + topologyId;
            LOG.error(errMsg);
            return null;
        }
        workerData.getWorkerToResource().addAll(workers);

        Map<Integer, ResourceWorkerSlot> my_assignment = new HashMap<Integer, ResourceWorkerSlot>();
        for (ResourceWorkerSlot worker : workers) {
            for (Integer id : worker.getTasks()) {
                my_assignment.put(id, worker);
            }
        }
        taskNodePort.putAll(my_assignment);
        return taskNodePort;
    }

    /**
     * Get tasks for nodes
     * @return Mapping from node to worker to task ids
     */
    private TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> getMappings(TopologyContext context, String rootComponent, Grouping._Fields operation, String stream) {
        TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings = new TreeMap<String, TreeMap<Integer, TreeSet<Integer>>>();
        if (Grouping._Fields.ALL.equals(operation)) {
            Map<String, Map<String, Grouping>> targets = context.getTargets(rootComponent);

            Map<String, Grouping> streamTargets = targets.get(stream);
            for (Map.Entry<String, Grouping> e : streamTargets.entrySet()) {
                Set<Integer> tasks = new TreeSet<Integer>();
                Grouping g = e.getValue();
                String id = e.getKey();
                if (operation.equals(Thrift.groupingType(g))) {
                    List<Integer> bCastTasks = context.getComponentTasks(id);
                    tasks.addAll(bCastTasks);
                }

                // go through the component tasks list to figure out the correct locations
                mappings = exctractWorkerMappings(tasks, taskNodePort);
            }
        } else if (Grouping._Fields.SHUFFLE.equals(operation)) {
            // because we are building a inverted tree, we will get the tasks of the source node
            List<Integer> ts = context.getComponentTasks(rootComponent);
            Set<Integer> tasks = new TreeSet<Integer>();
            tasks.addAll(ts);

            // go through the component tasks list to figure out the correct locations
            mappings = exctractWorkerMappings(tasks, taskNodePort);
        }
        return mappings;
    }

    public static String printMappings(TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, TreeMap<Integer, TreeSet<Integer>>> e : mappings.entrySet()) {
            sb.append("Node: ").append(e.getKey()).append(": {");
            TreeMap<Integer, TreeSet<Integer>> nodeDetails = e.getValue();
            for (Map.Entry<Integer, TreeSet<Integer>> e2 : nodeDetails.entrySet()) {
                sb.append(e2.getKey()).append(": (");
                TreeSet<Integer> tasks = e2.getValue();
                for (Integer t : tasks) {
                    sb.append(t).append(" ");
                }
                sb.append(") ");
            }
            sb.append(" } ");
        }
        return sb.toString();
    }

    /**
     * Get the mappings of supervisor, worker to task
     * @param tasks
     * @param taskNodePort
     * @return
     */
    private TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> exctractWorkerMappings( Set<Integer> tasks, ConcurrentHashMap<Integer, ResourceWorkerSlot> taskNodePort) {
        TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> broadCastTasks = new TreeMap<String, TreeMap<Integer, TreeSet<Integer>>>();
        for (Integer t : tasks) {
            ResourceWorkerSlot rw = this.taskNodePort.get(t);
            String n = rw.getNodeId();
            TreeMap<Integer, TreeSet<Integer>> broadCastTasksForNode = broadCastTasks.get(n);
            if (broadCastTasksForNode == null) {
                broadCastTasksForNode = new TreeMap<Integer, TreeSet<Integer>>();
                broadCastTasks.put(n, broadCastTasksForNode);
            }
            Integer w = rw.getPort();
            TreeSet<Integer> broadCastTasksForNodeWorker = broadCastTasksForNode.get(w);
            if (broadCastTasksForNodeWorker == null) {
                broadCastTasksForNodeWorker = new TreeSet<Integer>();
                broadCastTasksForNode.put(w, broadCastTasksForNodeWorker);
            }
            broadCastTasksForNodeWorker.add(t);
        }
        return broadCastTasks;
    }

    private int getWorkerPort(int taskId) {
        ResourceWorkerSlot slot = taskNodePort.get(taskId);
        return slot.getPort();
    }

    private String getNodeId(int taskId) {
        ResourceWorkerSlot slot = taskNodePort.get(taskId);
        return slot.getNodeId();
    }
}
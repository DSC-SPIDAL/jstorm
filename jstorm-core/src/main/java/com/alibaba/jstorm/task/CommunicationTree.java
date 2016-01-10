package com.alibaba.jstorm.task;

import backtype.storm.Config;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a collective communications tree. The parameters of the tree defines how the collective communications happen.
 */
public class CommunicationTree {
    private static Logger LOG = LoggerFactory.getLogger(CommunicationTree.class);
    /**
     * Weather we have a expanding tree or a gathering tree. These operations are from n parallel tasks to m parallel tasks.
     * Expanding trees are when n < m and gathering trees are when n > m
     */
    private boolean expandingTree;

    /**
     * How many branches we permit at the worker level
     */
    private int workerLevelBranchingFactor = 2;

    /**
     * How many branches we permit at the node level
     */
    private int nodeLevelBranchingFactor = 2;

    /**
     * A node in the collective tree
     */
    private static class TreeNode {
        List<TreeNode> children = new ArrayList<TreeNode>();
        TreeNode parent;
        TreeSet<Integer> taskIds = new TreeSet<Integer>();
        TreeNode left() {
            if (children.size() > 0) {
                return children.get(0);
            }
            return null;
        }
        TreeNode right() {
            if (children.size() > 1) {
                return children.get(1);
            }
            return null;
        }
    }

    /** Root of this tree */
    private TreeNode root;

    public boolean isExpandingTree() {
        return expandingTree;
    }

    public TreeSet<Integer> rootTasks() {
        return root.taskIds;
    }

    /**
     * Create a collective tree
     * @param conf topology configuration
     */
    public CommunicationTree(Map conf, int rootTaskId, String rootSupervisorId, int rootWorkerPort,
                             TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings,
                             boolean expandingTree, boolean useFlatTree) {
        StringBuilder sb = new StringBuilder("Root Tasks: ");
        sb.append(rootTaskId).append(" ");
        sb.append("\n");
        String s = CommunicationPlanner.printMappings(mappings);
        sb.append("Mappings: ").append(s);

        LOG.info("Collective tree: " + sb.toString());
        Object workerLevelBranching = conf.get(Config.COLLECTIVE_WORKER_BRANCHING_FACTOR);
        if (workerLevelBranching != null) {
            if (workerLevelBranching instanceof Integer) {
                this.workerLevelBranchingFactor = (int) workerLevelBranching;
            }
            if (workerLevelBranching instanceof Long) {
                long w = (long) workerLevelBranching;
                this.workerLevelBranchingFactor = (int) w;
            }
        }

        Integer nodeLevelBranching = (Integer) conf.get(Config.COLLECTIVE_NODE_BRANCHING_FACTOR);
        // if we are going to use a flat tree no level branching factor should be the size of supervisors
        if (useFlatTree) {
            this.nodeLevelBranchingFactor = mappings.size();
        } else {
            if (nodeLevelBranching != null) {
                this.nodeLevelBranchingFactor = nodeLevelBranching;
            }
        }

        LOG.info("NodeLevel branching: {}, WorkerLevel Branching: {}", this.nodeLevelBranchingFactor, this.workerLevelBranchingFactor);
        this.expandingTree = expandingTree;

        root = new TreeNode();
        root.taskIds.add(rootTaskId);

        List<SupervisorWorker> supervisorWorkers = buildList(mappings, rootSupervisorId, rootWorkerPort);
        buildTree(root, supervisorWorkers);
        // LOG.info("Tree: {}", BTreePrinter.print(root));
    }

    public String printTree() {
        return BTreePrinter.print(root);
    }

    public TreeSet<Integer> taskIdsOfNode(int taskId) {
        TreeNode treeNode = searchTree(root, taskId);
        if (treeNode != null) {
            return treeNode.taskIds;
        } else {
            return null;
        }
    }

    /**
     * Search for tree
     * @param node node of the tree
     * @param item task id
     * @return the node, null if not found
     */
    private TreeNode searchTree(TreeNode node, int item) {
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        treeNodes.offer(node);
        while (!treeNodes.isEmpty()) {
            TreeNode n = treeNodes.poll();

            // if this is the node return it
            if (n.taskIds.contains(item)) {
                return n;
            }

            // expand the children
            for (TreeNode tn : n.children) {
                treeNodes.offer(tn);
            }
        }
        return null;
    }

    private List<SupervisorWorker> buildList(TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings, String rootSupervisorId, int rootWorkerPort) {
        List<SupervisorWorker> supervisorWorkers = new ArrayList<SupervisorWorker>();
        for (Map.Entry<String, TreeMap<Integer, TreeSet<Integer>>> e : mappings.entrySet()) {
            String supervisor = e.getKey();

            TreeMap<Integer, TreeSet<Integer>> workers = e.getValue();
            List<WorkerTask> workerTasks = new ArrayList<WorkerTask>();
            for (Map.Entry<Integer, TreeSet<Integer>> w : workers.entrySet()) {
                List<Integer> tasks = new ArrayList<Integer>(w.getValue());
                Collections.sort(tasks);
                WorkerTask workerTask = new WorkerTask(w.getKey(), tasks);
                if (rootSupervisorId.equals(supervisor) && rootWorkerPort == w.getKey()) {
                    workerTask.setPriority(10);
                }
                workerTasks.add(workerTask);
            }
            Collections.sort(workerTasks);
            SupervisorWorker e1 = new SupervisorWorker(supervisor, workerTasks);
            if (rootSupervisorId.equals(supervisor)) {
                e1.setPriority(10);
            }
            supervisorWorkers.add(e1);
        }
        Collections.sort(supervisorWorkers);

        return supervisorWorkers;
    }

    private void buildBinaryTree(TreeNode parent, List<SupervisorWorker> supervisorWorkers) {
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();

    }

    /**
     * Build the whole collective tree
     * @param parent the starting node
     * @param supervisorWorkers worker to task mapping in nodes, all these tasks are connected using collective operations
     */
    private void buildTree(TreeNode parent, List<SupervisorWorker> supervisorWorkers) {
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        treeNodes.offer(parent);

        TreeNode p = treeNodes.poll();
        int nodeCount = 0;

        for (SupervisorWorker worker : supervisorWorkers) {
            List<WorkerTask> workers = worker.getWorkerTasksList();
            TreeNode n = buildTreeOfNode(workers);
            p.children.add(n);
            treeNodes.offer(n);
            nodeCount++;

            if (nodeCount == nodeLevelBranchingFactor) {
                nodeCount = 0;
                p = treeNodes.poll();
            }
        }
    }

    /**
     * Build the part of the tree specific to the node
     * @param tasks worker to task mapping in this node, all these tasks are connected using collective operations
     * @return the first node of this sub tree
     */
    private TreeNode buildTreeOfNode(List<WorkerTask> tasks) {
        TreeNode nodeRoot = new TreeNode();

        int count = workerLevelBranchingFactor;
        boolean root = true;
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        treeNodes.add(nodeRoot);
        TreeNode parent = null;

        for (WorkerTask workerTask : tasks) {
            List<Integer> t = workerTask.getTasks();

            // if worker level branching factor is < 0, then we will create a flat tree
            if (count == workerLevelBranchingFactor) {
                parent = treeNodes.poll();
                count = 0;
            }

            // we need to do some special processing for root
            if (root) {
                parent.taskIds.addAll(t);
                root = false;
            } else {
                TreeNode node = new TreeNode();
                node.taskIds.addAll(t);
                // add the node to parent
                node.parent = parent;
                parent.children.add(node);
                treeNodes.offer(node);
                count++;
            }
        }
        return nodeRoot;
    }

    /**
     * After building the tree, we can use this method to query the tasks to which we need to communicate.
     * 1. We will send the data to 1 worker in each node. The task in that worker will
     * broadcast the message to other workers in the same machine
     *
     * @return task ids,
     */
    public TreeSet<Integer> getChildTasks(int taskId) {
        TreeNode t = searchTree(root, taskId);
        if (t != null) {
            if (expandingTree) {
                TreeSet<Integer> treeSet = new TreeSet<Integer>();
                // check weather taskId is the first of the node, if not first we don't have downstream tasks
                int first = t.taskIds.first();
                // we return empty set
                if (taskId != first) {
                    return treeSet;
                } else {
                    for (Integer task : t.taskIds) {
                        treeSet.add(task);
                    }
                }

                // if we have child nodes, add the first of each child node as well
                for (TreeNode tn : t.children) {
                    // add the first of the childrens task ids
                    treeSet.add(tn.taskIds.first());
                }

                return treeSet;
            } else {
                // in case of non-expanding tree we only have one child
                TreeSet<Integer> treeSet = new TreeSet<Integer>();
                int first = t.taskIds.first();
                // if we are the first task, then we only have child nodes from the parent
                if (taskId == first) {
                    TreeNode parent = t.parent;
                    if (parent != null) {
                        treeSet.add(parent.taskIds.first());
                        return treeSet;
                    }
                    return treeSet;
                } else {
                    for (Integer task : t.taskIds) {
                        // add the first tasks as children
                        if (task == taskId) {
                            treeSet.add(first);
                        }
                    }
                }
                return treeSet;
            }
        }
        return new TreeSet<Integer>();
    }

    static class BTreePrinter {
        public static String print(TreeNode root) {
            StringBuilder sb = new StringBuilder("(");
            int state = 0; // 0 = saw node, 1 = saw 1, 2 = saw 2
            Queue<Object> queue = new LinkedList<Object>();
            queue.offer(root);
            queue.offer(1);
            while(!queue.isEmpty()) {
                Object t = queue.poll();
                if (t instanceof Integer) {
                    sb.append("\n");
                    if (!queue.isEmpty()) {
                        queue.add(1);
                    }
                }

                if (t instanceof TreeNode) {
                    printNodeInternal((TreeNode) t, sb);
                    queue.addAll(((TreeNode) t).children);
                }
            }
            return sb.toString();
        }

        private static void printNodeInternal(TreeNode node, StringBuilder sb) {
            sb.append("[");
            for (Integer t : node.taskIds) {
                sb.append(t).append(" ");
            }
            sb.append("]");
        }
    }
}


/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.task.backpressure.BackpressureController;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

/**
 * Sending entrance
 * <p/>
 * Task sending all tuples through this Object
 * <p/>
 * Serialize the Tuple and put the serialized data to the sending queue
 * 
 * @author yannian
 */
public class TaskTransfer {

    private static Logger LOG = LoggerFactory.getLogger(TaskTransfer.class);

    protected Map storm_conf;
    protected DisruptorQueue transferQueue;
    protected KryoTupleSerializer serializer;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;
    protected DisruptorQueue serializeQueue;
    protected final AsyncLoopThread serializeThread;
    protected volatile TaskStatus taskStatus;
    protected String taskName;
    protected AsmHistogram serializeTimer;
    protected Task task;
    protected String topolgyId;
    protected String componentId;
    protected int taskId;

    protected ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    protected ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    protected BackpressureController backpressureController;

    protected ConcurrentHashMap<Integer, IConnection> intraNodeConnections;

    protected boolean intraNodeMessagingEnabled = false;

    // broadcast tasks for each stream
    private DownstreamTasks downStreamTasks;

    public TaskTransfer(Task task, String taskName, KryoTupleSerializer serializer, TaskStatus taskStatus, WorkerData workerData) {
        this.task = task;
        this.taskName = taskName;
        this.serializer = serializer;
        this.taskStatus = taskStatus;
        this.storm_conf = workerData.getStormConf();
        this.transferQueue = workerData.getTransferQueue();
        this.innerTaskTransfer = workerData.getInnerTaskTransfer();

        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();

        this.topolgyId = workerData.getTopologyId();
        this.componentId = this.task.getComponentId();
        this.taskId = this.task.getTaskId();

        this.intraNodeConnections = workerData.getIntraNodeConnections();
        this.intraNodeMessagingEnabled = workerData.isIntraNodeMessagingEnabled();

        int queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
        WaitStrategy waitStrategy = (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(storm_conf);
        this.serializeQueue = DisruptorQueue.mkInstance(taskName, ProducerType.MULTI, queue_size, waitStrategy);
        this.serializeQueue.consumerStarted();

        String taskId = taskName.substring(taskName.indexOf(":") + 1);
        QueueGauge serializeQueueGauge = new QueueGauge(serializeQueue, taskName, MetricDef.SERIALIZE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topolgyId, componentId, this.taskId, MetricDef.SERIALIZE_QUEUE, MetricType.GAUGE),
                new AsmGauge(serializeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(Integer.valueOf(taskId), MetricDef.SERIALIZE_QUEUE, serializeQueueGauge);
        serializeTimer =
                (AsmHistogram) JStormMetrics.registerTaskMetric(
                        MetricUtils.taskMetricName(topolgyId, componentId, this.taskId, MetricDef.SERIALIZE_TIME, MetricType.HISTOGRAM), new AsmHistogram());

        serializeThread = setupSerializeThread();

        backpressureController = new BackpressureController(storm_conf, task.getTaskId(), serializeQueue, queue_size);
        LOG.info("Successfully start TaskTransfer thread");

    }

    public void setDownStreamTasks(DownstreamTasks downStreamTasks) {
        this.downStreamTasks = downStreamTasks;
    }

    public void transfer(TupleExt tuple) {
        int targetTaskId = tuple.getTargetTaskId();
        // this is equal to taskId
        int sourceTaskId = tuple.getSourceTask();
        GlobalTaskId globalStreamId = new GlobalTaskId(sourceTaskId, tuple.getSourceStreamId());

        // first check weather we need to skip
        if (downStreamTasks.isSkip(globalStreamId, sourceTaskId, targetTaskId)) {
            return;
        }

        // we will get the target is no mapping
        int mapping = downStreamTasks.getMapping(globalStreamId, sourceTaskId, targetTaskId);
        // LOG.info("Got a mapping of task transfer {} --> {}", targetTaskId, mapping);
        StringBuilder innerTaskTextMsg = new StringBuilder();
        StringBuilder outerTaskTextMsg = new StringBuilder();
        DisruptorQueue exeQueue = innerTaskTransfer.get(mapping);
        if (exeQueue != null) {
            // in this case we are not going to hit TaskReceiver, so we need to do what we did there
            // lets determine weather we need to send this message to other tasks as well acting as an intermediary
            Map<GlobalTaskId, Set<Integer>> downsTasks = downStreamTasks.allDownStreamTasks(mapping);
            if (downsTasks != null && downsTasks.containsKey(globalStreamId) && !downsTasks.get(globalStreamId).isEmpty()) {
                Set<Integer> tasks = downsTasks.get(globalStreamId);
                byte[] tupleMessage = null;

                for (Integer task : tasks) {
                    if (task != mapping) {
                        // these tasks can be in the same worker or in a different worker
                        DisruptorQueue exeQueueNext = innerTaskTransfer.get(task);
                        if (exeQueueNext != null) {
                            innerTaskTextMsg.append(task).append(" ");
                            exeQueueNext.publish(tuple);
                        } else {
                            outerTaskTextMsg.append(task).append(" ");
                            if (tupleMessage == null) {
                                tupleMessage = serializer.serialize(tuple);
                            }
                            TaskMessage taskMessage = new TaskMessage(task, tupleMessage, tuple.getSourceTask(), tuple.getSourceStreamId());
                            IConnection conn = getConnection(task);
                            if (conn != null) {
                                conn.send(taskMessage);
                            }
                        }
                    } else {
                        innerTaskTextMsg.append(task).append(" ");
                        exeQueue.publish(tuple);
                    }
                }
            } else {
                exeQueue.publish(tuple);
            }
        } else {
            int taskid = tuple.getTargetTaskId();
            byte[] tupleMessage;
            tupleMessage = serializer.serialize(tuple);
            TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage,
                    tuple.getSourceTask(), tuple.getSourceStreamId());
            IConnection conn = getConnection(taskid);
            if (conn != null) {
                conn.send(taskMessage);
            }
        }
    }

    public void transfer(byte []tuple, int task, int sourceTask, String streamId) {
        TaskMessage taskMessage = new TaskMessage(task, tuple, sourceTask, streamId);
        IConnection conn = getConnection(task);
        if (conn != null) {
            conn.send(taskMessage);
        } else {
            String s = "No connection to task: " + task;
            LOG.error(s);
            throw new RuntimeException(s);
        }
    }

    public void transfer(byte []tuple, Tuple tupleExt, int task) {
        DisruptorQueue exeQueue = innerTaskTransfer.get(task);
        if (exeQueue == null) {
            TaskMessage taskMessage = new TaskMessage(task, tuple);
            IConnection conn = getConnection(task);
            if (conn != null) {
                conn.send(taskMessage);
            } else {
                String s = "No connection to task: " + task;
                LOG.error(s);
                throw new RuntimeException(s);
            }
        } else {
            exeQueue.publish(tupleExt);
        }
    }
    
    public void push(int taskId, TupleExt tuple) {
    	serializeQueue.publish(tuple);
    }

    protected AsyncLoopThread setupSerializeThread() {
        return new AsyncLoopThread(new TransferRunnable());
    }

    public AsyncLoopThread getSerializeThread() {
        return serializeThread;
    }

    public BackpressureController getBackpressureController() {
        return backpressureController;
    }

    protected class TransferRunnable extends RunnableCallback implements EventHandler {

        private AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();

        @Override
        public String getThreadName() {
            return taskName + "-" + TransferRunnable.class.getSimpleName();
        }
		

        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();
        }

        @Override
        public void run() {
            while (shutdown.get() == false) {
                serializeQueue.consumeBatchWhenAvailable(this);
            }
        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }
        
        public byte[] serialize(ITupleExt tuple) {
        	return serializer.serialize((TupleExt)tuple);
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {

            if (event == null) {
                return;
            }

            long start = System.nanoTime();

            try {
			
			    ITupleExt tuple = (ITupleExt) event;
                int taskid = tuple.getTargetTaskId();
                IConnection conn = getConnection(taskid);
                if (conn != null) {
                	byte[] tupleMessage = serialize(tuple);
                    TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
                    conn.send(taskMessage);
                }
            } finally {
                long end = System.nanoTime();
                serializeTimer.update((end - start)/TimeUtils.NS_PER_US);
            }

        }


        protected void pullTuples(Object event) {
            TupleExt tuple = (TupleExt) event;
            int taskid = tuple.getTargetTaskId();
            IConnection conn = getConnection(taskid);
            if (conn != null) {
                while (conn.available() == false) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {

                    }
                }
                byte[] tupleMessage = serializer.serialize(tuple);
                TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
                conn.send(taskMessage);
            }
        }

    }

    protected IConnection getConnection(int taskId) {
        IConnection conn = null;
        WorkerSlot thisNodePort = taskNodeport.get(TaskTransfer.this.taskId);
        WorkerSlot nodePort = taskNodeport.get(taskId);
        if (nodePort == null) {
            String errormsg = "can`t not found IConnection to " + taskId;
            LOG.warn("Intra transfer warn", new Exception(errormsg));
        } else {
            // LOG.info("***********" + thisNodePort.getNodeId() + ":" + nodePort.getNodeId());
            if (intraNodeMessagingEnabled && thisNodePort.getNodeId().equals(nodePort.getNodeId())) {
                conn = intraNodeConnections.get(nodePort.getPort());
            }
            if (conn == null) {
                conn = nodeportSocket.get(nodePort);
                if (conn == null) {
                    String errormsg = "can`t not found nodePort " + nodePort;
                    LOG.warn("Intra transfer warn", new Exception(errormsg));
                }
            }
        }
        return conn;
    }


}

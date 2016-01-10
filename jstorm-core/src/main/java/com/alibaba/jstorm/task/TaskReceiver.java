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

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeUtils;
import com.esotericsoftware.kryo.KryoException;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TaskReceiver {
    private static Logger LOG = LoggerFactory.getLogger(TaskReceiver.class);

    protected Task task;
    protected final int taskId;
    protected final String idStr;

    protected TopologyContext topologyContext;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;

    protected final boolean isDebugRecv;

    protected DisruptorQueue deserializeQueue;
    protected KryoTupleDeserializer deserializer;
    protected AsyncLoopThread deserializeThread;
    protected AsmHistogram deserializeTimer;

    protected TaskStatus taskStatus;

    // if this task works as a broadcast intermediary, what are the tasks we need to relay this message
    // the tasks are grouped with stream id
    protected DownstreamTasks downStreamTasks;

    protected TaskTransfer taskTransfer;

    public TaskReceiver(Task task, int taskId, Map stormConf, TopologyContext topologyContext, Map<Integer, DisruptorQueue> innerTaskTransfer,
            TaskStatus taskStatus, String taskName,  DownstreamTasks downStreamTasks, TaskTransfer taskTransfer) {
        this.task = task;
        this.taskId = taskId;
        this.idStr = taskName;

        this.topologyContext = topologyContext;
        this.innerTaskTransfer = innerTaskTransfer;

        this.taskTransfer = taskTransfer;
        this.downStreamTasks = downStreamTasks;

        this.taskStatus = taskStatus;

        this.isDebugRecv = ConfigExtension.isTopologyDebugRecvTuple(stormConf);

        int queueSize = JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 256);

        WaitStrategy waitStrategy = (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(stormConf);
        this.deserializeQueue = DisruptorQueue.mkInstance("TaskDeserialize", ProducerType.MULTI, queueSize, waitStrategy);
        setDeserializeThread();
        this.deserializer = new KryoTupleDeserializer(stormConf, topologyContext);

        String topologyId = topologyContext.getTopologyId();
        String component = topologyContext.getThisComponentId();

        deserializeTimer =
                (AsmHistogram) JStormMetrics.registerTaskMetric(
                        MetricUtils.taskMetricName(topologyId, component, taskId, MetricDef.DESERIALIZE_TIME, MetricType.HISTOGRAM), new AsmHistogram());

        QueueGauge deserializeQueueGauge = new QueueGauge(deserializeQueue, idStr, MetricDef.DESERIALIZE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topologyId, component, taskId, MetricDef.DESERIALIZE_QUEUE, MetricType.GAUGE),
                new AsmGauge(deserializeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(taskId, MetricDef.DESERIALIZE_QUEUE, deserializeQueueGauge);
    }

    public AsyncLoopThread getDeserializeThread() {
        return deserializeThread;
    }

    protected void setDeserializeThread() {
        this.deserializeThread = new AsyncLoopThread(new DeserializeRunnable(deserializeQueue, innerTaskTransfer.get(taskId)));
    }

    public DisruptorQueue getDeserializeQueue() {
        return deserializeQueue;
    }

    class DeserializeRunnable extends RunnableCallback implements EventHandler {

        DisruptorQueue deserializeQueue;
        DisruptorQueue exeQueue;

        DeserializeRunnable(DisruptorQueue deserializeQueue, DisruptorQueue exeQueue) {
            this.deserializeQueue = deserializeQueue;
            this.exeQueue = exeQueue;
        }

        @Override
        public String getThreadName() {
            return idStr + "-deserializer";
        }

        protected Object deserialize(byte[] ser_msg) {
            long start = System.nanoTime();
            try {
                if (ser_msg == null) {
                    return null;
                }

                if (ser_msg.length == 0) {
                    return null;
                } else if (ser_msg.length == 1) {
                    byte newStatus = ser_msg[0];
                    LOG.info("Change task status as " + newStatus);
                    taskStatus.setStatus(newStatus);

                    return null;
                }

                // ser_msg.length > 1
                Tuple tuple = deserializer.deserialize(ser_msg);

                if (isDebugRecv) {
                    LOG.info(idStr + " receive " + tuple.toString());
                }

                return tuple;
            } catch (KryoException e) {
                throw new RuntimeException(e);
            } catch (Throwable e) {
                if (taskStatus.isShutdown() == false) {
                    LOG.error(idStr + " recv thread error " + JStormUtils.toPrintableString(ser_msg) + "\n", e);
                }
            } finally {
                long end = System.nanoTime();
                deserializeTimer.update((end - start) / TimeUtils.NS_PER_US);
            }

            return null;
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
            if (event instanceof TaskMessage) {
                processTaskMessage((TaskMessage) event);
            } else {
                Object tuple = deserialize((byte[]) event);
                if (tuple instanceof Tuple) {
                    processTupleEvent((Tuple) tuple, event);
                } else if (tuple instanceof BatchTuple) {
                    for (Tuple t : ((BatchTuple) tuple).getTuples()) {
                        processTupleEvent(t, event);
                    }
                } else if (tuple instanceof TimerTrigger.TimerEvent) {
                    exeQueue.publish(event);
                } else {
                    LOG.warn("Received unknown message");
                }
            }
        }

        public void processTaskMessage(TaskMessage event)
                throws Exception {
            byte[] msg = event.message();
            // LOG.info("Task message stream: " + taskMessage.stream() + " component: " + taskMessage.componentId());
            Set<Integer> transferTasks = new HashSet<Integer>();
            String streamId = event.stream();
            int sourceTask = event.sourceTask();
            String sourceComponent = topologyContext.getComponentId(sourceTask);
            GlobalTaskId globalStreamId = new GlobalTaskId(sourceTask, streamId);
            StringBuilder innerTaskTextMsg = new StringBuilder();
            StringBuilder outerTaskTextMsg = new StringBuilder();
            // lets determine weather we need to send this message to other tasks as well acting as an intermediary
            Map<GlobalTaskId, Set<Integer>> downsTasks = downStreamTasks.allDownStreamTasks(taskId);
            if (downsTasks != null && downsTasks.containsKey(globalStreamId) && !downsTasks.get(globalStreamId).isEmpty()) {
                // for now lets use the deserialized task and send it back... ideally we should send the byte message
                Set<Integer> tasks = downsTasks.get(globalStreamId);
                for (Integer task : tasks) {
                    if (task != taskId) {
                        // these tasks can be in the same worker or in a different worker
                        DisruptorQueue exeQueueNext = innerTaskTransfer.get(task);
                        if (exeQueueNext != null) {
                            innerTaskTextMsg.append(task).append(" ");
                            transferTasks.add(task);
                        } else {
                            outerTaskTextMsg.append(task).append(" ");
                            taskTransfer.transfer(msg, task, sourceTask, streamId);
                        }
                    } else {
                        innerTaskTextMsg.append(task).append(" ");
                        transferTasks.add(task);
                    }
                }
            } else {
                innerTaskTextMsg.append(taskId).append(" ");
                transferTasks.add(taskId);
            }
            Object tuple = deserialize(msg);
            if (tuple != null) {
                for (Integer t : transferTasks) {
                    if (t == taskId) {
                        exeQueue.publish(tuple);
                    } else {
                        DisruptorQueue exeQueueNext = innerTaskTransfer.get(t);
                        exeQueueNext.publish(tuple);
                    }
                }
            }
        }

        private void processTupleEvent(Tuple tuple, Object event) {
            if (tuple != null) {
                String streamId = tuple.getSourceStreamId();
                int sourceTask = tuple.getSourceTask();
                GlobalTaskId globalStreamId = new GlobalTaskId(sourceTask, streamId);
                // lets determine weather we need to send this message to other tasks as well acting as an intermediary
                Map<GlobalTaskId, Set<Integer>> downsTasks = downStreamTasks.allDownStreamTasks(taskId);
                if (downsTasks != null && downsTasks.containsKey(globalStreamId) && !downsTasks.get(globalStreamId).isEmpty()) {
                    // for now lets use the deserialized task and send it back... ideally we should send the byte message
                    Set<Integer> tasks = downsTasks.get(globalStreamId);
                    StringBuilder innerTaskTextMsg = new StringBuilder();
                    StringBuilder outerTaskTextMsg = new StringBuilder();
                    for (Integer task : tasks) {
                        if (task != taskId) {
                            // these tasks can be in the same worker or in a different worker
                            DisruptorQueue exeQueueNext = innerTaskTransfer.get(task);
                            if (exeQueueNext != null) {
                                innerTaskTextMsg.append(task).append(" ");
                                exeQueueNext.publish(tuple);
                            } else {
                                outerTaskTextMsg.append(task).append(" ");
                                taskTransfer.transfer((byte[]) event, tuple, task);
                            }
                        } else {
                            innerTaskTextMsg.append(task).append(" ");
                            exeQueue.publish(tuple);
                        }
                    }

                } else {
                    exeQueue.publish(tuple);
                }
            }
        }


        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();
        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }

        @Override
        public void run() {
            deserializeQueue.consumerStarted();
            LOG.info("Successfully start recvThread of " + idStr);

            while (taskStatus.isShutdown() == false) {
                try {

                    deserializeQueue.consumeBatchWhenAvailable(this);
                } catch (Throwable e) {
                    if (taskStatus.isShutdown() == false) {
                        LOG.error("Unknow exception ", e);
                    }
                }

            }

            task.unregisterDeserializeQueue();

            LOG.info("Successfully shutdown recvThread of " + idStr);
        }

        public Object getResult() {
            LOG.info("Begin to shutdown recvThread of " + idStr);
            return -1;
        }

    }
}
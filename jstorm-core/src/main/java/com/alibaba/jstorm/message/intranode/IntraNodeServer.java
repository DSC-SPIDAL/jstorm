package com.alibaba.jstorm.message.intranode;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import io.mappedbus.MappedBusReader;
import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinitySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IntraNodeServer implements IConnection {
    private static Logger LOG = LoggerFactory.getLogger(IntraNodeServer.class);
    public static final int LONG_BYTES = 8;
    public static final int INTEGER_BYTES = 4;
    public static final long DEFAULT_FILE_SIZE = 20000000L;
    public static final int PACKET_SIZE = 1024;

    // 2 Longs for uuid, 1 int for total number of packets, and 1 int for packet number
    private static int metaDataExtent = 2 * LONG_BYTES + 2 * INTEGER_BYTES;
    private HashMap<UUID, ArrayList<ByteBuffer>> msgs = new HashMap<UUID, ArrayList<ByteBuffer>>();
    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;

    private MappedBusReader reader;
    private int packetSize = PACKET_SIZE;
    private long fileSize = DEFAULT_FILE_SIZE;

    private boolean run = true;

    private Thread serverThread;

    private int count = 0;

    private int sourceTask;

    public IntraNodeServer(String baseFile, String supervisorId, int sourceTask, int targetTask, ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues, Map conf) {
        this.deserializeQueues = deserializeQueues;
        String sharedFile = baseFile + "/" + sourceTask;;
        int cpu = -1;
        this.sourceTask = sourceTask;
        if (conf != null) {
            Integer fileSizeConf = (Integer) conf.get(Config.STORM_MESSAGING_INTRANODE_FILE_SIZE);
            if (fileSizeConf != null) {
                fileSize = fileSizeConf;
            }

            Integer packetSizeConf = (Integer) conf.get(Config.STORM_MESSAGING_INTRANODE_PACKET_SIZE);
            if (packetSizeConf != null) {
                packetSize = packetSizeConf;
            }

            Object cpuBindsConfig = conf.get(Config.SUPERVISOR_SLOTS_PORTS_CPU_BINDS);
            if (cpuBindsConfig != null && cpuBindsConfig instanceof Map) {
                Object cpuConfig = ((Map) cpuBindsConfig).get(sourceTask);
                if (cpuConfig != null && cpuConfig instanceof  Integer) {
                    cpu = (int) cpuConfig;
                    LOG.info("CPU: {}", cpu);
                } else {
                    LOG.info("Not a integer");
                }
            } else {
                LOG.info("Not a map");
            }
        } else {
            LOG.info("Conf null in intraserver");
        }

        this.reader = new MappedBusReader(sharedFile, fileSize, packetSize, true);
        try {
            reader.open();
            LOG.info("Starting intranode server: " + sharedFile);
            serverThread = new Thread(new ServerWorker(cpu));
            serverThread.start();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class ServerWorker implements Runnable {
        int cpu;

        public ServerWorker(int cpu) {
            LOG.info("Creating server worker....");
            this.cpu = cpu;
        }

        public void run() {
            try {

                LOG.info("Starting server worker....");
                if (cpu > 0) {
                    //LOG.info("Setting affinity of process {} thread {} to {}", sourceTask, Affinity.getThreadId(), cpu);
                    BitSet bitSet = new BitSet(24);
                    bitSet.set(cpu);
                    Affinity.setAffinity(bitSet);
                    //LOG.info("CPU of task {} thread {} is {}", sourceTask, Affinity.getThreadId(), Affinity.getCpu());
                } else {
                    LOG.info("Not Setting affinity of process {}", cpu);
                }
                LOG.info("started intranode server worker....");
                byte[] bytes = new byte[packetSize];
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int length, totalPackets;
                UUID uuid;
                ArrayList<ByteBuffer> packets;
                boolean isFresh;
                while (run) {
                    if (reader.next()) {
                        // LOG.info("Received memory message");
                        length = reader.readBuffer(bytes, 0);
                        assert length == packetSize;
                        uuid = new UUID(buffer.getLong(0),
                                buffer.getLong(LONG_BYTES));
                        totalPackets = buffer.getInt(2 * LONG_BYTES);
                        packets = msgs.get(uuid);
                        if ((isFresh = packets == null)){
                            packets = new ArrayList<ByteBuffer>();
                        }
                        packets.add(ByteBuffer.wrap(Arrays.copyOf(bytes,
                                    bytes.length)));

                        if (packets.size() == totalPackets){
                            createMsg(isFresh ? packets : msgs.remove(uuid));
                            continue;
                        }
                        if (isFresh){
                            msgs.put(uuid, packets);
                        }
                    }
//                    if (count > 900) {
//                         System.out.println("Size of in complete messages: " + msgs.size() + " count2: " + count2);
//                        Thread.sleep(100);
//                    }
                }
                LOG.info("Intranode server shutdown....");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createMsg(ArrayList<ByteBuffer> packets) {
        Collections.sort(packets, new Comparator<ByteBuffer>() {
            @Override
            public int compare(ByteBuffer p1, ByteBuffer p2) {
                int offset = 2*LONG_BYTES;
                final int packetNum1 = p1.getInt(offset);
                final int packetNum2 = p2.getInt(offset);
                return packetNum1 < packetNum2 ? -1 : (packetNum1 == packetNum2 ? 0 : 1);
            }
        });
        int packetNumber = 0;
        ByteBuffer packet = packets.get(packetNumber);
        int packetSize = packet.capacity();
        int packetDataSize = packet.capacity() - metaDataExtent;
        int offset = 2*LONG_BYTES+2*INTEGER_BYTES;
        int task = packet.getInt(offset);
        offset += INTEGER_BYTES;
        int contentLength = packet.getInt(offset);
        offset += INTEGER_BYTES;
        int compIdLength = packet.getInt(offset);
        offset += INTEGER_BYTES;
        int streamLength = packet.getInt(offset);
        offset += INTEGER_BYTES;

        byte[] content = new byte[contentLength];
        byte[] compId = new byte[compIdLength];
        byte[] stream = new byte[streamLength];

        int count = 0;
        while (count < contentLength) {
            int remainingToRead = contentLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+2*INTEGER_BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(content, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        count = 0;
        while (count < compIdLength) {
            int remainingToRead = compIdLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+2*INTEGER_BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(compId, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        count = 0;
        while (count < streamLength) {
            int remainingToRead = streamLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+2*INTEGER_BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(stream, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        TaskMessage msg = new TaskMessage(task, content, Integer.parseInt(new String(compId)), new String(stream));
        // LOG.info("Recvd message: " + msg.task() + " " + msg.componentId() + ":" + msg.stream() + ": count: " + ++this.count);
        enqueue(msg);
    }

    @Override
    public Object recv(Integer taskId, int flags) {
        return null;
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {
        deserializeQueues.put(taskId, recvQueu);
    }

    @Override
    public void enqueue(TaskMessage message) {
        int task = message.task();

        DisruptorQueue queue = deserializeQueues.get(task);
        if (queue == null) {
            LOG.debug("Received invalid message directed at port " + task
                    + ". Dropping...");
            return;
        }

        queue.publish(message);
    }

    @Override
    public void send(List<TaskMessage> messages) {
        throw new UnsupportedOperationException(
                "Server connection should not send any messages");
    }

    @Override
    public void send(TaskMessage message) {
        throw new UnsupportedOperationException(
                "Server connection should not send any messages");
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public void close() {
        try {
            run = false;
            if (serverThread != null) {
                serverThread.join();
            }
            reader.close();
        } catch (IOException e) {
            LOG.warn("Failed to close reader", e);
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    public static void main(String[] args) {
        String baseFile = "/dev/shm";
//        String baseFile = "/home/supun/dev/projects/jstorm-modified";
        String nodeFile = "nodeFile";
        IntraNodeServer server = new IntraNodeServer(baseFile, nodeFile, 1, 1, new ConcurrentHashMap<Integer, DisruptorQueue>(), null);
    }
}


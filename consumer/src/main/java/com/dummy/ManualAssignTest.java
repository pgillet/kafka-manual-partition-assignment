package com.dummy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.WriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ManualAssignTest {

    private static final Logger logger = LoggerFactory.getLogger(ManualAssignTest.class);

    private final UUID GUID = UUID.randomUUID();

    // Set an explicit client ID for logging
    public final String clientId = "consumer-" + GUID;

    // ZooKeeper client
    private ZooKeeper zk;
    private WriteLock partitionLock;

    private List<PartitionInfo> partitionInfos;

    // Last visited partition
    private int lastVisited = 0;

    private KafkaConsumer<String, String> consumer;

    private final String topic = "my-topic";


    public ManualAssignTest() {

        Properties props = new Properties();
        // 10.0.2.2: IP address back to localhost from VirtualBox/Minikube
        // When connecting to the broker, the listener that will be returned to the client will be the listener to which
        // you connected (based on the port), here the MINIKUBE listener that is configured with port 29092.
        props.setProperty("bootstrap.servers", "10.0.2.2:29092");

        // Manual partition assignment does not use group coordination, so consumer failures will not cause assigned
        // partitions to be rebalanced. Each consumer acts independently even if it shares a groupId with another
        // consumer. To avoid offset commit conflicts (this cannot occur here as a partition can only be assigned to only
        // one consumer at a time), you should usually ensure that the groupId is unique for each consumer instance.
        props.setProperty("group.id", "my-group-id");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("max.poll.records", "20");

        // consumer.poll() will only return transactional messages which have been committed.
        props.setProperty("isolation.level", "read_committed");

        // props.setProperty("auto.offset.reset", "earliest");

        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("client.id", clientId);

        logger.info("Creating new consumer service");

        consumer = new KafkaConsumer<>(props);
    }


    public void run() {

        // Get all partitions' metadata for my-topic
        partitionInfos = consumer.partitionsFor(topic);

        logger.info("Listing all partitions for topic {}", topic);
        partitionInfos.forEach(partitionInfo -> logger.info(partitionInfo.toString()));
        logger.info("Listed all partitions for topic {}", topic);

        try {

            final String connectString = "10.0.2.2:2181";
            // Create a ZooKeeper client. Once a connection to a server is established, a session ID is assigned to the
            // client. The client will send heart beats to the server periodically to keep the session valid.
            zk = new ZooKeeper(connectString, 5000, new ConsumerWatcher());

            final int minBatchSize = 20;
            List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

            while (true) {

                manualAssign();

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }

                //if (buffer.size() >= minBatchSize) { // WARNING: last sent records may not appear if the buffer is not fully filled
                if (!buffer.isEmpty()) {

                    doStuff();

                    for (ConsumerRecord<String, String> record : buffer) {
                        logger.info("Record consumed and processed = [partition = {}, offset = {}, key = {}, value = {}]", record.partition(), record.offset(), record.key(), record.value());
                    }

                    logger.info("Committing offsets");
                    consumer.commitSync();
                    logger.info("Committed offsets");

                    buffer.clear();
                } else {
                    logger.info("No records available");
                    Thread.sleep(5000);
                }

                logger.info("Releasing lock on partition {}", partitionLock.getDir());
                partitionLock.unlock();
                logger.info("Released lock on partition {}", partitionLock.getDir());

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        } finally {

            // This finally block can be commented so as to simulate a crash of the VM (the machine in its entirety,
            // not the JVM) in which case we would not even go through the finally block.
            // This helps to ensure that Zookeeper releases the lock on its own once the ZK session is expired.

            logger.info("Closing Kafka consumer");
            consumer.close();

            // Removes the lock on the partition associated to this consumer.
            if (partitionLock != null) {
                logger.info("Releasing lock on partition {}", partitionLock.getDir());
                partitionLock.unlock();
                logger.info("Released lock on partition {}", partitionLock.getDir());
                // The lock is held by an ephemeral node in ZooKeeper.
                // We don't need to retry this operation in the case of failure
                // as ZK will remove ephemeral files and we don't wanna hang
                // this process when closing if we cannot reconnect to ZK.
            }
            // Close the ZooKeeper client
            if (zk != null) {
                logger.info("Closing ZooKeeper client");
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        }
    }


    public static void main(String[] args) {
        ManualAssignTest test = new ManualAssignTest();
        test.run();
    }

    private void manualAssign() throws KeeperException, InterruptedException {

        // Round Robin on partitions
        while (true) {

            lastVisited = (lastVisited + 1) % partitionInfos.size();

            PartitionInfo partitionInfo = partitionInfos.get(lastVisited);

            final String dir = String.format("/%s-%d", topic, partitionInfo.partition());

            partitionLock = new WriteLock(zk, dir, null);
            partitionLock.setLockListener(new LockCallback(dir));

            if (partitionLock.tryLock()) {
                // Partition is available
                logger.info("Assigning partition {} to consumer", dir);
                TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
                consumer.assign(Arrays.asList(partition));
                logger.info("Assigned partition {} to consumer", dir);

                break;
            } else {
                // partitionLock = null;
                logger.info("Partition {} is already assigned", dir);
                Thread.sleep(100);
            }
        }

        // At this point, if the consumer is not assigned to any topic or any partitions because no partition is
        // available, a call to KafkaConsumer#poll will throw an exception and the service will simply exit.
        // This may be a valid and desired behavior.
        // In practise this cannot occur here, as we cannot exit the loop as long as the consumer is not assigned a
        // partition.

    }

    /**
     * Simulates a long processing. Every once in a while, throw an exception to simulate a crash.
     *
     * @throws InterruptedException
     */
    private void doStuff() throws InterruptedException {

        Random rand = new Random();

        // 1 chance out of 10 to crash
        boolean oops = rand.nextDouble() > 0.9;

        int bound = 30; // 3 mn max duration
        int seconds = rand.nextInt(bound) + 1; // At least 1 sec
        int crashBound = rand.nextInt(seconds);

        logger.info("Running heavy and long computation...");
        for (int i = 0; i < seconds; i++) {
            Thread.sleep(1000);
            if (i >= crashBound && oops) {
                IllegalStateException ex = new IllegalStateException("Oops, something really bad happened ;)");
                logger.error(ex.getMessage(), ex);
                throw ex;
            }
        }
        logger.info("Ended heavy and long computation successfully after {} seconds", seconds);
    }

}

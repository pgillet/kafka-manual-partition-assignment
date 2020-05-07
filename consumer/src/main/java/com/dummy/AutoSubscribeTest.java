package com.dummy;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class AutoSubscribeTest {

    private static final Logger logger = LoggerFactory.getLogger(AutoSubscribeTest.class);

    // Set an explicit client ID for logging
    public static final String clientId = "consumer-" + UUID.randomUUID();

    private final String topic = "my-topic";

    private KafkaConsumer<String, String> consumer;

    public AutoSubscribeTest() {

        Properties props = new Properties();
        // 10.0.2.2: IP address back to localhost from VirtualBox/Minikube
        // When connecting to the broker, the listener that will be returned to the client will be the listener to which
        // you connected (based on the port), here the MINIKUBE listener that is configured with port 29092.
        props.setProperty("bootstrap.servers", "10.0.2.2:29092");
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

    /*public static void main(String[] args) throws InterruptedException {
        AutoSubscribeTest test = new AutoSubscribeTest();
        test.run();
    }*/

    public void run() {
        try {
            autoSubscribe();

            final int minBatchSize = 20;
            List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }

                //if (buffer.size() >= minBatchSize) { // WARNING: last sent records may not appear if the buffer is not fully filled
                if (!buffer.isEmpty()) {

                    doStuff();

                    for (ConsumerRecord<String, String> record : buffer) {
                        logger.info("Record consumed and processed = [offset = {}, key = {}, value = {}]", record.offset(), record.key(), record.value());
                    }
                    logger.info("Committing offsets");
                    consumer.commitSync();
                    logger.info("Committed offsets");

                    buffer.clear();
                } else {
                    logger.info("No records available");
                    Thread.sleep(5000);
                }

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        } finally {
            logger.info("Closing Kafka consumer");
            consumer.close();
        }
    }

    private void autoSubscribe() {
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

            long start;

            /**
             *  This method will be called before a rebalance operation starts and after the consumer stops fetching data.
             *  It is recommended that offsets should be committed in this callback to either Kafka or a custom offset
             *  store to prevent duplicate data, but in our case offsets should be only committed after records have been
             *  successfully processed, which can take a long time.
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info("--- REBALANCE OPERATION IS COMING ---");
                start = System.currentTimeMillis();
            }


            /**
             * This method will be called after the partition re-assignment completes and before the consumer starts
             * fetching data, and only as the result of a poll(long) call.
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info("--- PARTITIONS ASSIGNED ---");
                long stop = System.currentTimeMillis();
                logger.info("Partition rebalance operation took {}s", (stop - start) / 1000);
                for (TopicPartition partition : partitions) {
                    logger.debug("Partition assigned = {}", partition);
                }
            }
        };

        consumer.subscribe(Arrays.asList(topic), listener);
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

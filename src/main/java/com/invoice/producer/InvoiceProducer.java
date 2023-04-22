package com.invoice.producer;

import static com.invoice.common.CommonSettings.BOOTSTRAP_SERVERS;
import static com.invoice.common.CommonSettings.TOPIC_NAME;

import com.invoice.common.Invoice;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;

@RequiredArgsConstructor
public class InvoiceProducer {

    private final static int NUM_THREADS = 3;
    private final static int NUM_INVOICES_PER_THREAD = 10;
    private final static CountDownLatch LATCH =
            new CountDownLatch(NUM_THREADS * NUM_INVOICES_PER_THREAD);
    private final static boolean SEPARATE_CUSTOMERS_PARTITIONS = false;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        assert checkKafkaConnection(props);

        // create threads to generate invoices
        for (int i = 0; i < NUM_THREADS; i++) {
            new Thread(() -> {
                for (int j = 0; j < NUM_INVOICES_PER_THREAD; j++) {
                    // generate invoice
                    Invoice invoice = Invoice.generateRandomInvoice();
                    int recordPartition = 0;

                    // Writing each customer in a separate partition is a feature flag
                    if (SEPARATE_CUSTOMERS_PARTITIONS) {
                        recordPartition = (int) invoice.getCustomerId();

                        // Increase the partitions in case we have a new customer
                        if (getCurrentTopicPartitions(TOPIC_NAME, props) - 1 < recordPartition) {
                            increaseTopicPartitions(TOPIC_NAME, recordPartition, props);
                        }
                    }

                    // send invoice to Kafka
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC_NAME, recordPartition,
                                    invoice.getCustomerName(), invoice.toString());
                    producer.send(record, new InvoiceCallback());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        // wait for all invoices to be sent
        LATCH.await();

        producer.close();
    }

    // Makes sure we are successfully connected to the Kafka server
    private static boolean checkKafkaConnection(Properties props)
            throws ExecutionException, InterruptedException {
        AdminClient client = AdminClient.create(props);

        Collection<Node> nodes = client.describeCluster()
                .nodes()
                .get();

        var res = nodes != null && nodes.size() > 0;
        System.out.println("Connection established successfully: " + res);
        return res;
    }

    // Make sure we have enough partitions to fit each customer in a separate partition
    private static void increaseTopicPartitions(String topicName, int partitions, Properties props) {
        try {
            AdminClient adminClient = AdminClient.create(props);

            int currentPartitions = getCurrentTopicPartitions(topicName, props);

            // Construct the request to increase the number of partitions
            Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
            newPartitionsMap.put(topicName, NewPartitions.increaseTo(partitions));

            CreatePartitionsResult result = adminClient.createPartitions(newPartitionsMap);

            // Send the request and wait for the response
            CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitionsMap);
            createPartitionsResult.all().get();

            System.out.println("Partitions increased successfully for topic " + topicName);
        } catch (ExecutionException | InterruptedException | UnknownTopicOrPartitionException | InvalidPartitionsException e) {
            System.err.println("Error increasing partitions for topic " + topicName + ": " + e.getMessage());
        }
    }

    // Get the current partition count in the given topic
    private static int getCurrentTopicPartitions(String topic, Properties props) {
        try {
            AdminClient adminClient = AdminClient.create(props);

            // Get the current description of the topic
            TopicDescription
                    topicDescription = adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get();
            // Get the number of partitions for the topic
            return topicDescription.partitions().size();
        } catch (Exception e) {
            return Integer.MIN_VALUE;
        }
    }

    private static class InvoiceCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            //Check if errors exist and print proper message
            if (exception == null) {
                System.out
                        .printf("Invoice sent successfully to topic %s partition %d at offset %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                System.err.println("Failed to send invoice: " + exception.getMessage());
            }

            //Wait until a set of operations being performed in other threads completes.
            LATCH.countDown();
        }
    }

}


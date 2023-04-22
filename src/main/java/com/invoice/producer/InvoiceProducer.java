package com.invoice.producer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class InvoiceProducer {

    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String TOPIC_NAME = "invoices";
    private final static int NUM_THREADS = 3;
    private final static int NUM_INVOICES_PER_THREAD = 10;
    private final static CountDownLatch LATCH = new CountDownLatch(NUM_THREADS * NUM_INVOICES_PER_THREAD);

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        // create threads to generate invoices
        for (int i = 0; i < NUM_THREADS; i++) {
            new Thread(() -> {
                for (int j = 0; j < NUM_INVOICES_PER_THREAD; j++) {
                    // generate invoice
                    Invoice invoice = generateInvoice();

                    // send invoice to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, invoice.getCustomerId(), invoice.toString());
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

    private static class Invoice {
        private String customerId;
        private double amount;

        public Invoice(String customerId, double amount) {
            this.customerId = customerId;
            this.amount = amount;
        }

        public String getCustomerId() {
            return customerId;
        }

        @Override
        public String toString() {
            return "Invoice{" +
                    "customerId='" + customerId + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }

    private static class InvoiceCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                System.out.printf("Invoice sent successfully to topic %s partition %d at offset %d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                System.err.println("Failed to send invoice: " + exception.getMessage());
            }

            LATCH.countDown();
        }
    }

    private static Invoice generateInvoice() {
        // simulate generating an invoice
        String customerId = "customer-" + (int) (Math.random() * 10);
        double amount = Math.random() * 100;
        return new Invoice(customerId, amount);
    }
}


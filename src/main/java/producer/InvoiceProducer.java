package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InvoiceProducer {

    public static void main(String[] args) {
        // Set up Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms", "2000");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Generate invoices in multiple threads
        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                Random rand = new Random();
                while (true) {
                    // Generate random invoice data
                    String customer = "Customer " + rand.nextInt(10);
                    BigDecimal amount = new BigDecimal(rand.nextDouble() * 100).setScale(2, BigDecimal.ROUND_HALF_UP);

                    // Send invoice data to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>("invoices", customer, amount.toString());
                    producer.send(record);

                    // Wait for a random amount of time before generating the next invoice
                    try {
                        Thread.sleep(rand.nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        // Shut down the producer and executor when done
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            executor.shutdown();
        }));
    }

}

package com.invoice.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class InvoiceConsumer {

    public static void main(String[] args) {
        // Set up Kafka com.invoice.common.consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "invoice-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the "invoices" topic
        consumer.subscribe(Collections.singletonList("invoices"));

        // Poll for new messages and print them to the console
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String customer = record.key();
                BigDecimal amount = new BigDecimal(record.value());
                System.out.printf("Invoice received: Customer %s, Amount: $%.2f%n", customer, amount);
            }
        }
    }

}

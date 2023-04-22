package com.invoice.consumer;

import static com.invoice.common.CommonSettings.BOOTSTRAP_SERVERS;
import static com.invoice.common.CommonSettings.TOPIC_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.invoice.common.Invoice;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class InvoiceConsumer {

    private static final Map<String, Double> customerDebt = new HashMap<>();

    public static void main(String[] args) throws JsonProcessingException {
        // Set up Kafka com.invoice.common.consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "invoice-consumer-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the "invoices" topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        System.out.println("Successfully subscribed to the topic");

        // TODO: keep separate summary for each different customer in memory and once in a while
        //  print short summary for each customer.

        // TODO: extra effort - use avro for binarizing the records

        // Poll for new messages and print them to the console
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

            System.out.println(records.count());
            for (ConsumerRecord<String, String> record : records) {
                // parse JSON->Invoice
                ObjectMapper mapper = new ObjectMapper();
                Invoice invoice = mapper.readValue(record.value(), Invoice.class);

                if (!customerDebt.containsKey(invoice.getCustomerName())) {
                    customerDebt.put(invoice.getCustomerName(), 0d);
                }

                customerDebt.put(invoice.getCustomerName(),
                        customerDebt.get(invoice.getCustomerName()) + invoice.getAmount());
            }

            customerDebt.forEach((key, value) -> System.out
                    .println("Customer " + key + " has a cost of " + value));
        }
    }

}

package com.invoice.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

public class ConsumerThread extends Thread {

    private int id;
    private KafkaConsumer<String,String> consumer;
    private boolean readOn = true;

    public ConsumerThread(int id, KafkaConsumer<String,String> consumer) {
        this.id = id;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (readOn) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1l));

            for (ConsumerRecord<String,String> r : records) {
                String message = r.value();
                long offset = r.offset();

                System.out.println("Consumer #"+id+" got message: {partition:'"+r.partition()+"',offset:'"+offset+"',key:'"+r.key()+"',message:'"+message+"'}");
            }
        }
    }

    public void stopReading() {
        readOn = false;
    }

    public int getConsumerId() {
        return id;
    }

}
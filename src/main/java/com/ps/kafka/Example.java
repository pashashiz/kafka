package com.ps.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Example {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newCachedThreadPool();
        SimpleProducer producer = new SimpleProducer();
        SimpleConsumer consumer = new SimpleConsumer();
        executor.submit(() -> {
            consumer.subscribe("test");
        });
        Thread.sleep(2000);
        executor.submit(() -> {
            producer.send("test", 10);
        });
        Thread.sleep(3000);
        consumer.stop();
        producer.stop();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public static class SimpleProducer {

        private KafkaProducer<String, String> producer;

        public SimpleProducer() {
            // create instance for properties to access producer configs
            Properties props = new Properties();
            // assign localhost id
            props.put("bootstrap.servers", "localhost:9092");
            // set acknowledgements for producer requests.
            props.put("acks", "all");
            // if the request fails, the producer can automatically retry,
            props.put("retries", 0);
            // specify buffer size in config
            props.put("batch.size", 16384);
            // reduce the no of requests less than 0
            props.put("linger.ms", 1);
            // the buffer.memory controls the total amount of memory available to the producer for buffering.
            props.put("buffer.memory", 33554432);
            // serializers
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }

        public void send(String topic, int messages) {
            System.out.println("Sending to a topic " + topic);
            IntStream.range(0, messages).forEach(i -> {
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), "value" + i));
                System.out.println("Message " + i + " is sent");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        public void stop() {
            producer.close();
        }

    }

    public static class SimpleConsumer {

        private KafkaConsumer<String, String> consumer;
        private volatile boolean closed = false;

        public SimpleConsumer() {
            // kafka consumer configuration settings
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
        }

        public void subscribe(String topic) {
            consumer.subscribe(Arrays.asList(topic));
            System.out.println("Subscribed to a topic " + topic);
            while (!closed) {
                System.out.println("Try to poll...");
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("Got a record: offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
            }
            consumer.close();
        }

        public void stop() {
            closed = true;
        }
    }

}

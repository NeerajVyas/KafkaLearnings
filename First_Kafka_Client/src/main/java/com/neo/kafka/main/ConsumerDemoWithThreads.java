package com.neo.kafka.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }
    private ConsumerDemoWithThreads(){

    }

    private void run()  {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String bootstrapServers="127.0.0.1:9092";
        String groupId ="my-first-java-group";
        String topic ="loop_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //creating the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers,groupId,topic,latch);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error(""+e);
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got Interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        // to deal with concurrency
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        Logger logger;

        public ConsumerRunnable(String bootstrapServers, String groupId,String topic,CountDownLatch latch){
            this.latch=latch;
            this.logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
            logger.info("Inside ConsumerRunnable Constructor");

            //1. Consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            //deserializer --> kafka send these bytes right back to the consumer. Consumer has to take these bytes and create a string from it.
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            //earliest or latest or none

            logger.info("Kafka Consumer Created");
            //create a consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton(topic));
            //by doing Collections.singleton we subscribing to a single topic
            // consumer.subscribe(Arrays.asList("first_topic","loop_topic"));
            logger.info("Subscription Done");
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    //  consumer.poll(100);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key : " + record.key() + ",  Value : " + record.value());
                        logger.info("Partition : " + record.partition() + ", Offset :" + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received Shutdown Signal");
            } finally {
                consumer.close();
                //tell our main code we're done with the consumer.
                latch.countDown();
            }

        }

        public void shutdown(){
            // the wakeup method is a special method to interrupt consumer.poll
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}

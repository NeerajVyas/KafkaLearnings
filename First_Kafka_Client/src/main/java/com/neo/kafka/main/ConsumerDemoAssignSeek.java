package com.neo.kafka.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String bootstrapServers="127.0.0.1:9092";
        String topic ="loop_topic";

        //1. Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //deserializer --> kafka send these bytes right back to the consumer. Consumer has to take these bytes and create a string from it.
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //earliest or latest or none

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numberOfMessageReadSoFar=0;

        //poll for new data
        while (keepOnReading){
          //  consumer.poll(100);
            ConsumerRecords <String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                numberOfMessageReadSoFar++;
                logger.info("key : "+record.key() + ",  Value : "+record.value());
                logger.info("Partition : "+ record.partition() + ", Offset :" +record.offset());
                if (numberOfMessageReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");

    }
}

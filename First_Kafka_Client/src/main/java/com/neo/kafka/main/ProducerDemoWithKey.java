package com.neo.kafka.main;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKey {
    public static void main (String [] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

        //create the producer properties
        Properties properties = new Properties();
        //https://docs.confluent.io/current/installation/configuration/producer-configs.html
        String bootstrapServers="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //NOTE : String serializer because by default Kafka Client will convert whatever we send to kafka into bytes 0s and 1s.
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        // <key,value>
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i< 10 ; i ++) {

            String topic = "loop_topic";
            String value = "Java Generate message - " + Integer.toString(i);
            String key = "id_"+Integer.toString(i);

            //create a producer record.
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key : "+ key);
            // id_0 is going to partitions 1
            // id_1 is going to partitions 0
            // id_2 is going to partitions 2
            // id_3 is going to partitions 0
            // id_4 is going to partitions 2
            // id_5 is going to partitions 2
            // id_6 is going to partitions 0
            // id_7 is going to partitions 2
            // id_8 is going to partitions 1
            // id_9 is going to partitions 2

            //send Data ~ synchronous (Bad Practice) --> .get() and add throw exceptions
             producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();   //block the .send() to make it synchronous - don't do this in production
        }
        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}

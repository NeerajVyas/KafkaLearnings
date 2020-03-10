package com.neo.kafka.main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main (String [] args){

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

        //create a producer record.
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","Generated Message from Java");

        //send Data ~ asynchronous  --> So flush and close is mandatory otherwise data won't be sent.
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}

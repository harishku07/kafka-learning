package com.github.harishku.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        /*creating consumer properties*/

        String bootstrapServer = "localhost:9092";
        String topic = "second_topic";
        String groupId = "my_first_application";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        /*creating the consumer*/
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        /*Subscribing to the topic*/
        consumer.subscribe(Collections.singleton(topic));

        /*poll for the data*/
        while (true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> consumerRecord:consumerRecords) {
                logger.info("Key: "+consumerRecord.key()+", value: "+consumerRecord.value());
                logger.info("Partition: "+consumerRecord.partition()+", offset: "+consumerRecord.offset());
            }
        }
    }
}

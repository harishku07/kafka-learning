package com.github.harishku.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        /*creating producer properties*/
        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        /*creating the producer*/
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 20; i++) {

            String topic = "second_topic";
            String value = "hello iam from java: "+ i;
            String key = "id_"+i;

            logger.info("key : "+i);
            /*creating the producer record*/
            ProducerRecord<String,String>  record = new ProducerRecord<String, String>(topic,key,value);

            /*sending the message  -- asynchronous*/
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    /*everytime when a message is sent to topic or an error thrown this method will execute*/
                    if(e==null){
                        logger.info("Received new metadata: \n"
                                +"Topic: "+recordMetadata.topic() +"\n"
                                +"Partition: "+recordMetadata.partition()+"\n"
                                +"Offset: "+recordMetadata.offset()+"\n"
                                +"TimeStamp: "+recordMetadata.timestamp());
                    }else {
                        logger.error("Error while producing the data",e);
                    }
                }
            }).get(); /* blocks the .send to make it synchronous*/
        }


        /*flush data*/
        producer.flush();

        /*flush and close producer*/
        producer.close();
    }
}

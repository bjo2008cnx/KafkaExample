package com.jasongj.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoCallback {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("partitioner.class", HashPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord record = new ProducerRecord<String, String>("topic1", Integer.toString(i), Integer.toString(i));
            producer.send(record);
            //send(producer, record);
            sendJava8(producer, record);
        }
        producer.close();
    }

    private static void sendJava8(Producer<String, String> producer, ProducerRecord record) {
        producer.send(record, (metadata, exception) -> {
            if (metadata != null) {
                System.out.printf("Send record partition:%d, offset:%d, keysize:%d, valuesize:%d %n", metadata.partition(), metadata.offset(), metadata
                        .serializedKeySize(), metadata.serializedValueSize());
            }
            if (exception != null) {
                exception.printStackTrace();
            }
        });
    }

    private static void send(Producer<String, String> producer, ProducerRecord record) {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.printf("Send record partition:%d, offset:%d, keysize:%d, valuesize:%d %n", metadata.partition(), metadata.offset(), metadata
                        .serializedKeySize(), metadata.serializedValueSize());
            }
        });
    }

}

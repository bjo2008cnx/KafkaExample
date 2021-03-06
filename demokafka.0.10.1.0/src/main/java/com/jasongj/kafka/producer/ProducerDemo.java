package com.jasongj.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

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
		props.put("interceptor.classes", EvenProducerInterceptor.class.getName());

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		for (int i = 10000; i < 20000; i++) {
			producer.send(new ProducerRecord<>("test_20180201", Integer.toString(i), Integer.toString(i)));
			Thread.sleep(10);
		}
		producer.close();
	}

}

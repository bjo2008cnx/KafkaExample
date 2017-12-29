package com.jasongj.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * 如果一个consumer同时消费多个分区，默认情况下，这多个分区的优先级是一样的，同时消费。Kafka提供机制，可以让暂停某些分区的消费，先获取其他分区的内容。场景举栗：
 1. 流式计算，consumer同时消费两个Topic，然后对两个Topic的数据做Join操作。但是这两个Topic里面的数据产生速率差距较大。Consumer就需要控制下获取逻辑，先获取慢的Topic，慢的读到数据后再去读快的。
 2. 同样多个Topic同时消费，但是Consumer启动是，本地已经存有了大量某些Topic数据。此时就可以优先去消费下其他的Topic。

 调控的手段：让某个分区消费先暂停，时机到了再恢复，然后接着poll。接口：pause(TopicPartition…)，resume(TopicPartition…)
 */
public class DemoConsumerFlowControl {

	public static void main(String[] args) {
		args = new String[] { "kafka0:9092", "topic1", "group239", "consumer2" };
		if (args == null || args.length != 4) {
			System.err.println(
					"Usage:\n\tjava -jar kafka_consumer.jar ${bootstrap_server} ${topic_name} ${group_name} ${client_id}");
			System.exit(1);
		}
		String bootstrap = args[0];
		String topic = args[1];
		String groupid = args[2];
		String clientid = args[3];

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap);
		props.put("group.id", groupid);
		props.put("client.id", clientid);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener(){

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				partitions.forEach(topicPartition -> {
					System.out.printf("Revoked partition for client %s : %s-%s %n", clientid, topicPartition.topic(), topicPartition.partition());
				});
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				partitions.forEach(topicPartition -> {
					System.out.printf("Assigned partition for client %s : %s-%s %n", clientid, topicPartition.topic(), topicPartition.partition());
				});
			}});
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100000000);
			consumer.pause(Arrays.asList(new TopicPartition(topic, 0)));
			consumer.pause(Arrays.asList(new TopicPartition(topic, 1)));
			records.forEach(record -> {
				System.out.printf("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s%n", clientid, record.topic(),
						record.partition(), record.offset(), record.key(), record.value());
			});
		}
	}

}

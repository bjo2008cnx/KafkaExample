package com.jasongj.kafka.consumer.commit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 如果consumer在获得数据后需要加入处理，数据完毕后才确认offset，需要程序来控制offset的确认。举个栗子：
 * consumer获得数据后，需要将数据持久化到DB中。自动确认offset的情况下，如果数据从kafka集群读出，就确认，但是持久化过程失败，就会导致数据丢失。
 * 我们就需要控制offset的确认
 */
public class ManualCommitDemo {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        /* 关闭自动确认选项 */
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            /* 数据达到批量要求，就写入DB，同步确认offset */
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

}

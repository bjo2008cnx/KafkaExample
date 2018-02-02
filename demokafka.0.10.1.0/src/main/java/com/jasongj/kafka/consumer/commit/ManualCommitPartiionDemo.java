package com.jasongj.kafka.consumer.commit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 精细的控制对具体分区具体offset数据的确认
 *
 * @author Michael.Wang
 * @date 2017/12/22
 */
public class ManualCommitPartiionDemo {
    private static boolean running = true;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        /* 关闭自动确认选项 */
        props.put("enable.auto.commit", "false");
        props.put("max.poll.interval.ms", "10");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "60000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /* 自动确认offset的时间间隔  */
        props.put("auto.commit.interval.ms", "1000");

        props.put("session.timeout.ms", "30000");

        //消息发送的最长等待时间.需大于session.timeout.ms这个时间
        props.put("request.timeout.ms", "40000");

        //一次从kafka中poll出来的数据条数
        //max.poll.records条数据需要在在session.timeout.ms这个时间内处理完
        props.put("max.poll.records", "100");

        //server发送到消费端的最小数据，若是不满足这个数值则会等待直到满足指定大小。默认为1表示立即接收。
        props.put("fetch.min.bytes", "1");
        //若是不满足fetch.min.bytes时，等待消费端请求的最长等待时间
        props.put("fetch.wait.max.ms", "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test_20180201"));

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (TopicPartition partition : records.partitions()) {
                    //按分区消费
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    System.out.println("partitionRecords::" + partitionRecords.size());
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                   /* 同步确认某个分区的特定offset */
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    System.out.println("lastOffset:: " + lastOffset);
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
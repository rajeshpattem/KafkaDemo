package com.example.kafkademo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaListen {


    @org.springframework.kafka.annotation.KafkaListener(topics = "test", groupId = "group_id", containerFactory = "userKafkaListenerFactory")
    public void listen(ConsumerRecord<String, String> record, Consumer consumer, Acknowledgment acknowledgment) {
        try {
            long currentOffset = consumer.position(new TopicPartition("test", 0));
            System.out.println(currentOffset);
            process(record.value());
            acknowledgment.acknowledge();
            System.out.println(currentOffset);
        } catch (Exception e) {
            if (process2(record.value())) {
                acknowledgment.acknowledge();
            }
        }
    }

    public String process(String s) {
        if (s.equals("Ramesh") || s.equals("Rajesh")) {
            throw new RuntimeException();
        }
        return s;
    }

    public boolean process2(String s) {
        if (s.equals("Rajesh"))
            return true;
        return false;
    }


}

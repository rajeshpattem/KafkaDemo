package com.example.kafkademo.Controller;

import com.example.kafkademo.DTO.User;
import com.example.kafkademo.KafkaListen;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "v1")
public class ProducerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "test";

    @GetMapping("/publish/{name}")
    public String postMessage(@PathVariable("name") final String name) {

        kafkaTemplate.send(TOPIC, name);
        return "Message Published Successfully";
    }

}

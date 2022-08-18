package com.example.rabbitmq;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class RabbitmqApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(RabbitmqApplication.class, args);
        AmqpTemplate template = run.getBean(AmqpTemplate.class);

    }

}

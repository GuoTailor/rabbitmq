package com.example.rabbitmq;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;

/**
 * Created by gyh on 2022/7/22
 */
@Slf4j
//@Configuration
public class RabbitmqQueue {
    @Bean
    public ApplicationRunner runner(AmqpTemplate template) {
        return args -> {
            System.out.println("准备开始");
            Thread.sleep(5_000);
            template.convertAndSend("test", "all", "foo");
//            template.convertAndSend("test", "all2", "foo2");
        };
    }

    @Bean
    public RabbitTemplate rabbitTemplate(CachingConnectionFactory connectionFactory) {
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        connectionFactory.setPublisherReturns(true);
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> log.info("消息发送成功:correlationData({}),ack({}),cause({})", correlationData, ack, cause));
        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> log.info("消息丢失:exchange({}),route({}),replyCode({}),replyText({}),message:{}", exchange, routingKey, replyCode, replyText, message));

        RetryTemplate retryTemplate = new RetryTemplate();

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(100);
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(10_000);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(30);
        retryTemplate.setRetryPolicy(retryPolicy);

        rabbitTemplate.setRetryTemplate(retryTemplate);
        return rabbitTemplate;
    }

    @Bean
    public DirectExchange myExchange() {
        return new DirectExchange("test");
    }

    @Bean
    public Queue myQueue() {
        return new Queue("myqueue");
    }

    @Bean
    public Queue myQueue2() {
        return new Queue("myqueue2");
    }

    @Bean
    public Binding myBinding(DirectExchange myExchange) {
        return BindingBuilder.bind(myQueue()).to(myExchange).with("all");
    }

    @Bean
    public Binding myBinding2(DirectExchange myExchange) {
        return BindingBuilder.bind(myQueue2()).to(myExchange).with("all2");
    }

    @RabbitListener(queues = "myqueue")
    public void listen(String in, Message message, Channel channel) throws IOException {
        System.out.println("myqueue " + in);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    @RabbitListener(queues = "myqueue2")
    public void listen2(String in, Message message, Channel channel) throws IOException {
        System.out.println("myqueue2 " + in);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), true);
    }

}

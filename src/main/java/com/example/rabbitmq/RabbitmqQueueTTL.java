package com.example.rabbitmq;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
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
public class RabbitmqQueueTTL {
    @Bean
    public ApplicationRunner runner(AmqpTemplate template) {
        return args -> {
            template.convertAndSend("test_ttl", "ttl1", "foo");
            template.convertAndSend("test_ttl", "ttl2", "foo2");
            template.convertAndSend("test_ttl", "ttl3", "foo3");
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
        retryTemplate.registerListener(new RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(RetryContext retryContext, RetryCallback<T, E> retryCallback) {
                // 执行之前调用 （返回false时会终止执行）
                System.out.println("----open----" + retryContext.getRetryCount());
                return true;
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext retryContext, RetryCallback<T, E> retryCallback, Throwable throwable) {
                // 重试结束的时候调用 （最后一次重试 ）
                System.out.println("---------------最后一次调用" + retryContext.getRetryCount());
                if (throwable != null) {
                    throwable.printStackTrace();
                }
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext retryContext, RetryCallback<T, E> retryCallback, Throwable throwable) {
                //  异常 都会调用
                System.err.println("-----第{}次调用" + retryContext.getRetryCount());
            }
        });

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(10.0);
        backOffPolicy.setMaxInterval(10000);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retryTemplate.setRetryPolicy(retryPolicy);

        rabbitTemplate.setRetryTemplate(retryTemplate);
        return rabbitTemplate;
    }

    @Bean
    public DirectExchange myExchange() {
        return new DirectExchange("test_ttl");
    }

    @Bean
    Queue delayQueuePerMessageTTL() {
        return QueueBuilder.durable("messageTTL")
                .withArgument("x-dead-letter-exchange", "test_ttl") // DLX，dead letter发送到的exchange
                .withArgument("x-dead-letter-routing-key", "ttl1") // dead letter携带的routing key
                .build();
    }

    @Bean
    Queue delayQueuePerQueueTTL() {
        return QueueBuilder.durable("queueTTL")
                .withArgument("x-dead-letter-exchange", "test_ttl") // DLX
                .withArgument("x-dead-letter-routing-key", "ttl2") // dead letter携带的routing key
                .withArgument("x-message-ttl", "3000") // 设置队列的过期时间
                .build();
    }

    @Bean
    Queue delayProcessQueue() {
        return QueueBuilder.durable("queue")
                .build();
    }

    @Bean
    public Binding myBinding(DirectExchange myExchange) {
        return BindingBuilder.bind(delayQueuePerMessageTTL()).to(myExchange).with("ttl1");
    }

    @Bean
    public Binding myBinding2(DirectExchange myExchange) {
        return BindingBuilder.bind(delayQueuePerQueueTTL()).to(myExchange).with("ttl2");
    }

    @Bean
    public Binding myBinding3(DirectExchange myExchange) {
        return BindingBuilder.bind(delayProcessQueue()).to(myExchange).with("ttl3");
    }

    @RabbitListener(queues = "messageTTL")
    public void listen(String in, Message message, Channel channel) throws IOException {
        System.out.println("messageTTL " + in);
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

    @RabbitListener(queues = "queueTTL")
    public void listen2(String in, Message message, Channel channel) throws IOException {
        System.out.println("queueTTL " + in);
        channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
    }

    @RabbitListener(queues = "queue")
    public void listen3(String in, Message message, Channel channel) throws IOException {
        System.out.println("queue " + in);
        channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
    }

}

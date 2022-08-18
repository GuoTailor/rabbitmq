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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gyh on 2022/7/22
 */
@Slf4j
@Configuration
public class RabbitmqQueueTTL3 {
    @Bean
    public ApplicationRunner runner(AmqpTemplate template) {
        return args -> {
            template.convertAndSend("test_ttl", "delay_key", "foo1", message -> {
                // 延迟时间单位是毫秒
                message.getMessageProperties().setExpiration("3000");
                System.out.println("消息发送时间:" + LocalDateTime.now()
                        .format(DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm:ss")) +
                        "消息内容: foo3");
                return message;
            });

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

    /**
     * 死信交换机
     */
    @Bean
    public DirectExchange userOrderDelayExchange() {
        return new DirectExchange("test_ttl");
    }

    /**
     * 死信队列
     */
    @Bean
    public Queue userOrderDelayQueue() {
        Map<String, Object> map = new HashMap<>(16);
        map.put("x-dead-letter-exchange", "test_ttl2");
        map.put("x-dead-letter-routing-key", "receive_key");
        return new Queue("delay_queue", true, false, false, map);
    }

    /**
     * 给死信队列绑定交换机
     */
    @Bean
    public Binding userOrderDelayBinding() {
        return BindingBuilder.bind(userOrderDelayQueue()).to(userOrderDelayExchange()).with("delay_key");
    }

    /**
     * 死信接收交换机
     */
    @Bean
    public DirectExchange userOrderReceiveExchange() {
        return new DirectExchange("test_ttl2");
    }

    /**
     * 死信接收队列
     */
    @Bean
    public Queue userOrderReceiveQueue() {
        return new Queue("receive_queue");
    }

    /**
     * 死信交换机绑定消费队列
     */
    @Bean
    public Binding userOrderReceiveBinding() {
        return BindingBuilder.bind(userOrderReceiveQueue()).to(userOrderReceiveExchange()).with("receive_key");
    }

    @RabbitListener(queues = "receive_queue")
    public void listen2(String in, Message message, Channel channel) throws IOException {
        System.out.println("receive_queue " + in);
        byte[] body = message.getBody();
        System.out.println(LocalDateTime.now() + new String(body));
        channel.basicReject(message.getMessageProperties().getDeliveryTag(), true);
//        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }

}

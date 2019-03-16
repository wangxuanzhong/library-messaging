package com.syswin.library.messaging.redis.spring;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
@ConditionalOnProperty(value = "library.messaging.type", havingValue = "redis")
class RedisConfig {

  @Bean(destroyMethod = "destroy")
  RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory, List<RedisMqConsumer> mqConsumer) {
    RedisMessageListenerContainer container = new RedisMessageListenerContainer();
    container.setConnectionFactory(connectionFactory);
    mqConsumer.forEach(consumer -> container.addMessageListener(consumer, new ChannelTopic(consumer.topic())));
    return container;
  }
}

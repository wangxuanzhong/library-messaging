package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqImplementation.REDIS;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.redis.spring.RedisMqConsumer;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@ConditionalOnProperty(value = "library.messaging.redis.enabled", havingValue = "true")
@Configuration
class DefaultRedisMqConsumerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private List<RedisMqConsumer> redisMqConsumers(
      List<MqConsumerConfig> mqConsumerConfigs
  ) {
    return mqConsumerConfigs.stream()
        .filter(config -> REDIS == config.implementation())
        .map(config -> {
          log.info("Started Redis MQ consumer on topic {}", config.topic());
          return new RedisMqConsumer(
              config.topic(),
              config.listener());
        })
        .collect(Collectors.toList());
  }

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean(destroyMethod = "destroy")
  RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
      List<MqConsumerConfig> mqConsumerConfigs,
      List<MqConsumer> mqConsumers) {
    List<RedisMqConsumer> redisMqConsumers = redisMqConsumers(mqConsumerConfigs);
    mqConsumers.addAll(redisMqConsumers);

    RedisMessageListenerContainer container = new RedisMessageListenerContainer();
    container.setConnectionFactory(connectionFactory);
    redisMqConsumers.forEach(consumer -> container.addMessageListener(consumer, new ChannelTopic(consumer.topic())));
    return container;
  }
}
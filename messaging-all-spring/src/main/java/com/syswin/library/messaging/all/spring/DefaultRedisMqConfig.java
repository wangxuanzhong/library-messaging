package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.redis.spring.RedisMqConsumer;
import com.syswin.library.messaging.redis.spring.RedisMqProducer;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

@ConditionalOnProperty(value = "library.messaging.type", havingValue = "redis")
@Configuration
class DefaultRedisMqConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Bean
  Map<String, MqProducer> rocketMqProducers(
      RedisTemplate<String, Object> redisTemplate,
      List<MqProducerConfig> mqProducerConfigs
  ) {
    Map<String, MqProducer> mqProducers = new HashMap<>();
    mqProducerConfigs.forEach(config -> {
      mqProducers.put(config.group(), new RedisMqProducer(redisTemplate));
      log.info("Started Redis MQ producer of group {}", config.group());
    });

    return mqProducers;
  }

  @Bean
  List<RedisMqConsumer> rocketMqConsumers(
      List<MqConsumerConfig> mqConsumerConfigs
  ) {
    return mqConsumerConfigs.stream()
        .map(config -> {
          log.info("Started Redis MQ consumer on topic {}", config.topic());
          return new RedisMqConsumer(
              config.topic(),
              config.listener());
        })
        .collect(Collectors.toList());
  }
}

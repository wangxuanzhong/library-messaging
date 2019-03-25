package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqImplementation.REDIS;

import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.redis.spring.MessageRedisTemplate;
import com.syswin.library.messaging.redis.spring.RedisMqProducer;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@ConditionalOnProperty(value = "library.messaging.redis.enabled", havingValue = "true")
@Configuration
class DefaultRedisMqProducerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  MessageRedisTemplate libraryRedisTemplate(RedisConnectionFactory connectionFactory) {
    MessageRedisTemplate redisTemplate = new MessageRedisTemplate();
    redisTemplate.setConnectionFactory(connectionFactory);
    redisTemplate.setKeySerializer(new StringRedisSerializer());
    redisTemplate.setValueSerializer(new StringRedisSerializer());
    return redisTemplate;
  }

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, RedisMqProducer> libraryRedisMqProducers(
      MessageRedisTemplate redisTemplate,
      Map<String, MqProducer> mqProducers,
      List<MqProducerConfig> mqProducerConfigs
  ) {
    Map<String, RedisMqProducer> redisMqProducers = new HashMap<>();
    mqProducerConfigs.stream()
        .filter(config -> REDIS == config.implementation())
        .forEach(config -> {
          redisMqProducers.put(config.group(), new RedisMqProducer(redisTemplate));
          log.info("Started Redis MQ producer of group {}", config.group());
        });
    mqProducers.putAll(redisMqProducers);
    return redisMqProducers;
  }
}

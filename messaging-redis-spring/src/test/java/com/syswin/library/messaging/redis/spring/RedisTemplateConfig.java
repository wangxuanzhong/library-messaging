package com.syswin.library.messaging.redis.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
class RedisTemplateConfig {
  @Bean
  MessageRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory) {
    MessageRedisTemplate redisTemplate = new MessageRedisTemplate();
    redisTemplate.setConnectionFactory(connectionFactory);
    redisTemplate.setKeySerializer(new StringRedisSerializer());
    redisTemplate.setValueSerializer(new StringRedisSerializer());
    return redisTemplate;
  }
}

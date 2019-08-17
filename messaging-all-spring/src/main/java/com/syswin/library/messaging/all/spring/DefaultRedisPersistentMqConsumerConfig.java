/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqImplementation.REDIS_PERSISTENCE;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.redis.spring.MessageRedisTemplate;
import com.syswin.library.messaging.redis.spring.RedisPersistentMqConsumer;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.messaging.redis-persistence.enabled", havingValue = "true")
@Configuration
class DefaultRedisPersistentMqConsumerConfig {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final List<RedisPersistentMqConsumer> redisMqConsumers = new ArrayList<>();

  private List<RedisPersistentMqConsumer> redisMqConsumers(
      MessageRedisTemplate redisTemplate,
      List<MqConsumerConfig> mqConsumerConfigs,
      int pollingInterval
  ) {
    mqConsumerConfigs.stream()
        .filter(config -> REDIS_PERSISTENCE == config.implementation())
        .forEach(config -> {
          RedisPersistentMqConsumer consumer = new RedisPersistentMqConsumer(
              config.topic(),
              config.group(),
              config.listener(),
              redisTemplate,
              pollingInterval);

          consumer.start();
          log.info("Started Redis Persistent MQ consumer on topic {}", config.topic());
          redisMqConsumers.add(consumer);
        });
    return redisMqConsumers;
  }

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean
  List<RedisPersistentMqConsumer> redisPersistentConsumers(MessageRedisTemplate redisTemplate,
      List<MqConsumerConfig> mqConsumerConfigs,
      List<MqConsumer> mqConsumers,
      @Value("${library.messaging.redis-persistence.polling-interval:100}") int pollingInterval) {

    List<RedisPersistentMqConsumer> consumers = redisMqConsumers(redisTemplate, mqConsumerConfigs, pollingInterval);
    mqConsumers.addAll(consumers);

    return consumers;
  }

  @PreDestroy
  void shutdown() {
    redisMqConsumers.forEach(MqConsumer::shutdown);
  }
}

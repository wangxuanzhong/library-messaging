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

import static com.syswin.library.messaging.all.spring.MqConsumerType.BROADCAST;
import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;
import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ_ONS;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.rocketmqons.AbstractRocketMqOnsConsumer;
import com.syswin.library.messaging.rocketmqons.OrderedRocketMqOnsConsumer;
import com.syswin.library.messaging.rocketmqons.RocketMqOnsConfig;
import com.syswin.library.messaging.rocketmqons.RocketMqOnsConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.messaging.rocketmqons.enabled", havingValue = "true")
@Configuration
class DefaultRocketMqOnsConsumerConfig {

  private final Map<MqConsumerType, String> consumerTypeMapping = new HashMap<>();
  private final List<AbstractRocketMqOnsConsumer> rocketMqOnsConsumers = new ArrayList<>();

  DefaultRocketMqOnsConsumerConfig() {
    consumerTypeMapping.put(BROADCAST, "BROADCASTING");
    consumerTypeMapping.put(CLUSTER, "CLUSTERING");
  }

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean
  List<AbstractRocketMqOnsConsumer> libraryRocketMqOnsConsumers(
      @Value("${spring.rocketmqons.host}") String namesrvAddr,
      @Value("${spring.rocketmqons.accessKey}") String accessKey,
      @Value("${spring.rocketmqons.secretKey}") String secretKey,
      List<MqConsumer> mqConsumers,
      List<MqConsumerConfig> mqConsumerConfigs
  ) throws MessagingException {

    for (MqConsumerConfig config : mqConsumerConfigs) {
      if (config.implementation() == ROCKET_MQ_ONS) {
        AbstractRocketMqOnsConsumer mqConsumer = mqConsumer(namesrvAddr, accessKey, secretKey, config);
        mqConsumer.start();
        rocketMqOnsConsumers.add(mqConsumer);
      }
    }

    mqConsumers.addAll(rocketMqOnsConsumers);
    return rocketMqOnsConsumers;
  }

  @PreDestroy
  void shutdown() {
    rocketMqOnsConsumers.forEach(MqConsumer::shutdown);
  }

  private AbstractRocketMqOnsConsumer mqConsumer(String namesrvAddr, String accessKey, String secretKey,
      MqConsumerConfig config) {
    RocketMqOnsConfig mqOnsConfig = new RocketMqOnsConfig(namesrvAddr, accessKey, secretKey, config.group());

    if (config.isConcurrent()) {
      return new RocketMqOnsConsumer(
          mqOnsConfig,
          config.topic(),
          config.tag(),
          consumerTypeMapping.getOrDefault(config.type(), "CLUSTERING"),
          config.listener());
    }

    return new OrderedRocketMqOnsConsumer(
        mqOnsConfig,
        config.topic(),
        config.tag(),
        consumerTypeMapping.getOrDefault(config.type(), "CLUSTERING"),
        config.listener());
  }
}

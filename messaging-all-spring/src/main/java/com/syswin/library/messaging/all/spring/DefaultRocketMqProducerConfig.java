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
import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.rocketmq.RocketMqProducer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PreDestroy;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.messaging.rocketmq.enabled", havingValue = "true")
@Configuration
class DefaultRocketMqProducerConfig {

  private final Map<MqConsumerType, MessageModel> consumerTypeMapping = new HashMap<>();
  private final Map<String, RocketMqProducer> rocketMqProducers = new HashMap<>();

  DefaultRocketMqProducerConfig() {
    consumerTypeMapping.put(BROADCAST, BROADCASTING);
    consumerTypeMapping.put(CLUSTER, CLUSTERING);
  }

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, RocketMqProducer> libraryRocketMqProducers(
      @Value("${spring.rocketmq.host}") String brokerAddress,
      Map<String, MqProducer> mqProducers,
      List<MqProducerConfig> mqProducerConfigs
  ) throws MessagingException {

    for (MqProducerConfig config : mqProducerConfigs) {
      if (config.implementation() == ROCKET_MQ) {
        RocketMqProducer producer = new RocketMqProducer(brokerAddress, config.group());
        producer.start();
        rocketMqProducers.put(config.group(), producer);
      }
    }

    mqProducers.putAll(rocketMqProducers);
    return rocketMqProducers;
  }

  @PreDestroy
  void shutdown() {
    rocketMqProducers.values().forEach(MqProducer::shutdown);
  }

}

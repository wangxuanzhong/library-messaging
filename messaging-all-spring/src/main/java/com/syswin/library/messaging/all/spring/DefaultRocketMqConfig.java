package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ;
import static com.syswin.library.messaging.all.spring.MqConsumerType.BROADCAST;
import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.rocketmq.AbstractRocketMqConsumer;
import com.syswin.library.messaging.rocketmq.ConcurrentRocketMqConsumer;
import com.syswin.library.messaging.rocketmq.OrderedRocketMqConsumer;
import com.syswin.library.messaging.rocketmq.RocketMqProducer;
import java.util.ArrayList;
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
class DefaultRocketMqConfig {

  private final Map<MqConsumerType, MessageModel> consumerTypeMapping = new HashMap<>();
  private final Map<String, RocketMqProducer> rocketMqProducers = new HashMap<>();
  private final List<AbstractRocketMqConsumer> rocketMqConsumers = new ArrayList<>();

  DefaultRocketMqConfig() {
    consumerTypeMapping.put(BROADCAST, BROADCASTING);
    consumerTypeMapping.put(CLUSTER, CLUSTERING);
  }

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, RocketMqProducer> rocketMqProducers(
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

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean
  List<AbstractRocketMqConsumer> rocketMqConsumers(
      @Value("${spring.rocketmq.host}") String brokerAddress,
      List<MqConsumer> mqConsumers,
      List<MqConsumerConfig> mqConsumerConfigs
  ) throws MessagingException {

    for (MqConsumerConfig config : mqConsumerConfigs) {
      if (config.implementation() == ROCKET_MQ) {
        AbstractRocketMqConsumer mqConsumer = mqConsumer(brokerAddress, config);
        mqConsumer.start();
        rocketMqConsumers.add(mqConsumer);
      }
    }

    mqConsumers.addAll(rocketMqConsumers);
    return rocketMqConsumers;
  }

  @PreDestroy
  void shutdown() {
    rocketMqProducers.values().forEach(MqProducer::shutdown);
    rocketMqConsumers.forEach(MqConsumer::shutdown);
  }

  private AbstractRocketMqConsumer mqConsumer(String brokerAddress, MqConsumerConfig config) {
    if (config.isConcurrent()) {
      return new ConcurrentRocketMqConsumer(
          brokerAddress,
          config.group(),
          config.topic(),
          config.tag(),
          consumerTypeMapping.getOrDefault(config.type(), CLUSTERING),
          config.listener());
    }

    return new OrderedRocketMqConsumer(
        brokerAddress,
        config.group(),
        config.topic(),
        config.tag(),
        consumerTypeMapping.getOrDefault(config.type(), CLUSTERING),
        config.listener());
  }
}

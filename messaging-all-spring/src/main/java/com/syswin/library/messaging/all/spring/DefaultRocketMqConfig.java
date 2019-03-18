package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqConsumerType.BROADCAST;
import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
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

@ConditionalOnProperty(value = "library.messaging.type", havingValue = "rocketmq", matchIfMissing = true)
@Configuration
class DefaultRocketMqConfig {

  private final Map<MqConsumerType, MessageModel> consumerTypeMapping = new HashMap<>();
  private final Map<String, MqProducer> mqProducers = new HashMap<>();
  private final List<MqConsumer> mqConsumers = new ArrayList<>();

  DefaultRocketMqConfig() {
    consumerTypeMapping.put(BROADCAST, BROADCASTING);
    consumerTypeMapping.put(CLUSTER, CLUSTERING);
  }

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, MqProducer> rocketMqProducers(
      @Value("${spring.rocketmq.host}") String brokerAddress,
      List<MqProducerConfig> mqProducerConfigs
  ) throws MessagingException {

    for (MqProducerConfig config : mqProducerConfigs) {
      RocketMqProducer producer = new RocketMqProducer(brokerAddress, config.group());
      producer.start();
      mqProducers.put(config.group(), producer);
    }

    return mqProducers;
  }

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean
  List<MqConsumer> rocketMqConsumers(
      @Value("${spring.rocketmq.host}") String brokerAddress,
      List<MqConsumerConfig> mqConsumerConfigs
  ) throws MessagingException {

    for (MqConsumerConfig config : mqConsumerConfigs) {
      MqConsumer mqConsumer = mqConsumer(brokerAddress, config);
      mqConsumer.start();
      mqConsumers.add(mqConsumer);
    }

    return mqConsumers;
  }

  @PreDestroy
  void shutdown() {
    mqProducers.values().forEach(MqProducer::shutdown);
    mqConsumers.forEach(MqConsumer::shutdown);
  }

  private MqConsumer mqConsumer(String brokerAddress, MqConsumerConfig config) {
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

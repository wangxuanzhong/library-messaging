package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.embedded.EmbeddedMessageQueue;
import com.syswin.library.messaging.embedded.EmbeddedMqConsumer;
import com.syswin.library.messaging.embedded.EmbeddedMqProducer;
import com.syswin.library.messaging.embedded.MessageQueue;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.messaging.type", havingValue = "embedded")
@Configuration
class DefaultEmbeddedMqConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Bean(initMethod = "start", destroyMethod = "shutdown")
  MessageQueue messageQueue(@Value("${library.messaging.embedded.poll.interval:50}") long pollInterval) {
    return new EmbeddedMessageQueue(pollInterval);
  }

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, MqProducer> rocketMqProducers(
      MessageQueue messageQueue,
      List<MqProducerConfig> mqProducerConfigs
  ) {
    Map<String, MqProducer> mqProducers = new HashMap<>();
    mqProducerConfigs.forEach(config -> {
      mqProducers.put(config.group(), new EmbeddedMqProducer(messageQueue));
      log.info("Started embedded MQ producer of group {}", config.group());
    });

    return mqProducers;
  }

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean
  List<MqConsumer> rocketMqConsumers(
      MessageQueue messageQueue,
      List<MqConsumerConfig> mqConsumerConfigs
  ) {
    return mqConsumerConfigs.stream()
        .map(config -> createMqConsumer(messageQueue, config))
        .collect(Collectors.toList());
  }

  private EmbeddedMqConsumer createMqConsumer(MessageQueue messageQueue, MqConsumerConfig config) {
    EmbeddedMqConsumer mqConsumer = new EmbeddedMqConsumer(
        messageQueue,
        config.topic(),
        config.listener());

    mqConsumer.start();
    log.info("Started embedded MQ consumer on topic {}", config.topic());
    return mqConsumer;
  }
}

package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqImplementation.EMBEDDED;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.embedded.EmbeddedMqConsumer;
import com.syswin.library.messaging.embedded.MessageQueue;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.messaging.embedded.enabled", havingValue = "true")
@Configuration
class DefaultEmbeddedMqConsumerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ConditionalOnBean(MqConsumerConfig.class)
  @Bean
  List<EmbeddedMqConsumer> libraryEmbeddedMqConsumers(
      MessageQueue messageQueue,
      List<MqConsumer> mqConsumers,
      List<MqConsumerConfig> mqConsumerConfigs
  ) {
    List<EmbeddedMqConsumer> embeddedMqConsumers = mqConsumerConfigs.stream()
        .filter(config -> EMBEDDED == config.implementation())
        .map(config -> createMqConsumer(messageQueue, config))
        .collect(Collectors.toList());

    mqConsumers.addAll(embeddedMqConsumers);
    return embeddedMqConsumers;
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

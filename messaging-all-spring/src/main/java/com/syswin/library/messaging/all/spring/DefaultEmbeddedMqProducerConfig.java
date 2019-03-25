package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqImplementation.EMBEDDED;

import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.embedded.EmbeddedMqProducer;
import com.syswin.library.messaging.embedded.MessageQueue;
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

@ConditionalOnProperty(value = "library.messaging.embedded.enabled", havingValue = "true")
@Configuration
class DefaultEmbeddedMqProducerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, EmbeddedMqProducer> libraryEmbeddedMqProducers(
      MessageQueue messageQueue,
      Map<String, MqProducer> mqProducers,
      List<MqProducerConfig> mqProducerConfigs
  ) {
    Map<String, EmbeddedMqProducer> embeddedMqProducers = new HashMap<>();
    mqProducerConfigs.stream()
        .filter(config -> EMBEDDED == config.implementation())
        .forEach(config -> {
          embeddedMqProducers.put(config.group(), new EmbeddedMqProducer(messageQueue));
          log.info("Started embedded MQ producer of group {}", config.group());
        });
    mqProducers.putAll(embeddedMqProducers);
    return embeddedMqProducers;
  }

}

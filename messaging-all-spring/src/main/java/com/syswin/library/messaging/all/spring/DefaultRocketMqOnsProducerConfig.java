package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqConsumerType.BROADCAST;
import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;
import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ_ONS;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.rocketmqons.RocketMqOnsConfig;
import com.syswin.library.messaging.rocketmqons.RocketMqOnsProducer;
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
class DefaultRocketMqOnsProducerConfig {

  private final Map<MqConsumerType, String> consumerTypeMapping = new HashMap<>();
  private final Map<String, RocketMqOnsProducer> rocketMqOnsProducers = new HashMap<>();

  DefaultRocketMqOnsProducerConfig() {
    consumerTypeMapping.put(BROADCAST, "BROADCASTING");
    consumerTypeMapping.put(CLUSTER, "CLUSTERING");
  }

  @ConditionalOnBean(MqProducerConfig.class)
  @Bean
  Map<String, RocketMqOnsProducer> libraryRocketMqOnsProducers(
      @Value("${spring.rocketmqons.host}") String namesrvAddr,
      @Value("${spring.rocketmqons.accessKey}") String accessKey,
      @Value("${spring.rocketmqons.secretKey}") String secretKey,
      Map<String, MqProducer> mqProducers,
      List<MqProducerConfig> mqProducerConfigs
  ) throws MessagingException {

    for (MqProducerConfig config : mqProducerConfigs) {
      if (config.implementation() == ROCKET_MQ_ONS) {
        RocketMqOnsConfig mqOnsConfig = new RocketMqOnsConfig(namesrvAddr, accessKey, secretKey, config.group());
        RocketMqOnsProducer producer = new RocketMqOnsProducer(mqOnsConfig);
        producer.start();
        rocketMqOnsProducers.put(config.group(), producer);
      }
    }

    mqProducers.putAll(rocketMqOnsProducers);
    return rocketMqOnsProducers;
  }

  @PreDestroy
  void shutdown() {
    rocketMqOnsProducers.values().forEach(MqProducer::shutdown);
  }

}

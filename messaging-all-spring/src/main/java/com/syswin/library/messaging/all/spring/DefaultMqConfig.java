package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class DefaultMqConfig {

  @Bean
  Map<String, MqProducer> libraryMqProducers() {
    return new HashMap<>();
  }

  @Bean
  List<MqConsumer> libraryMqConsumers() {
    return new ArrayList<>();
  }
}

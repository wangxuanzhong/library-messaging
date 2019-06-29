package com.syswin.library.messaging.test.mixed;

import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;
import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ_ONS;
import static org.springframework.context.annotation.FilterType.ASSIGNABLE_TYPE;

import com.syswin.library.messaging.all.spring.MixedMqConfigTest;
import com.syswin.library.messaging.all.spring.MqConsumerConfig;
import com.syswin.library.messaging.all.spring.MqProducerConfig;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;

@ComponentScan(excludeFilters = {
    @Filter(type = ASSIGNABLE_TYPE, classes = {MixedMqConfigTestApp.class, MixedMqConfigTest.class})
})
@SpringBootApplication
public class RocketMqOnsConfigTestConfiguration {

  private static final String GROUP_ID = "GID-temail-test";

  @Bean
  Queue<String> messages() {
    return new ConcurrentLinkedQueue<>();
  }

  @Bean
  Consumer<String> messageListener(Queue<String> messages) {
    return messages::add;
  }

  @Bean
  MqProducerConfig rocketmqOnsProducerConfig() {
    return new MqProducerConfig(GROUP_ID, ROCKET_MQ_ONS);
  }

  @Bean
  MqConsumerConfig rocketmqOnsConsumerConfig(Consumer<String> listener) {
    return MqConsumerConfig.create()
        .group(GROUP_ID)
        .topic("PartitionOrderTopic")
        .type(CLUSTER)
        .implementation(ROCKET_MQ_ONS)
        .listener(listener)
        .concurrent()
        .build();
  }
}

package com.syswin.library.messaging.test.mixed;

import static com.syswin.library.messaging.all.spring.MqImplementation.EMBEDDED;
import static com.syswin.library.messaging.all.spring.MqImplementation.REDIS;
import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ;
import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;

import com.syswin.library.messaging.all.spring.MqConsumerConfig;
import com.syswin.library.messaging.all.spring.MqProducerConfig;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MixedMqConfigTestApp {

  public static void main(String[] args) {
    SpringApplication.run(MixedMqConfigTestApp.class, args);
  }

  @Bean
  Queue<String> messages() {
    return new ConcurrentLinkedQueue<>();
  }

  @Bean
  Consumer<String> messageListener(Queue<String> messages) {
    return messages::add;
  }

  @Bean
  MqProducerConfig redisProducerConfig() {
    return new MqProducerConfig("redis", REDIS);
  }

  @Bean
  MqProducerConfig embeddedProducerConfig() {
    return new MqProducerConfig("embedded", EMBEDDED);
  }

  @Bean
  MqProducerConfig rocketmqProducerConfig() {
    return new MqProducerConfig("rocketmq", ROCKET_MQ);
  }

  @Bean
  MqConsumerConfig redisConsumerConfig(Consumer<String> listener) {
    return MqConsumerConfig.create()
        .group("redis")
        .topic("brave-new-world")
        .type(CLUSTER)
        .implementation(REDIS)
        .listener(listener)
        .concurrent()
        .build();
  }

  @Bean
  MqConsumerConfig embeddedConsumerConfig(Consumer<String> listener) {
    return MqConsumerConfig.create()
        .group("embedded")
        .topic("brave-new-world")
        .type(CLUSTER)
        .implementation(EMBEDDED)
        .listener(listener)
        .concurrent()
        .build();
  }

  @Bean
  MqConsumerConfig rocketmqConsumerConfig(Consumer<String> listener) {
    return MqConsumerConfig.create()
        .group("rocketmq")
        .topic("brave-new-world")
        .type(CLUSTER)
        .implementation(ROCKET_MQ)
        .listener(listener)
        .concurrent()
        .build();
  }
}

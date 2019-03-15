package com.syswin.library.messaging.test.spring;

import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;

import com.syswin.library.messaging.all.spring.MqConsumerConfig;
import com.syswin.library.messaging.all.spring.MqProducerConfig;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MqConfigTestApp {

  public static void main(String[] args) {
    SpringApplication.run(MqConfigTestApp.class, args);
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
  MqProducerConfig producerConfig(@Value("${app.producer.group}") String group) {
    return new MqProducerConfig(group);
  }

  @Bean
  MqConsumerConfig consumerConfig(
      @Value("${app.consumer.group}") String group,
      @Value("${app.consumer.topic}") String topic,
      @Value("${app.consumer.tag}") String tag,
      Consumer<String> listener
  ) {
    return new MqConsumerConfig(group,
        topic,
        tag,
        CLUSTER,
        listener,
        true);
  }
}

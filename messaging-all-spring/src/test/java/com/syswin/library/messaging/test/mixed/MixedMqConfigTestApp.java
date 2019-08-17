/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.messaging.test.mixed;

import static com.syswin.library.messaging.all.spring.MqImplementation.EMBEDDED;
import static com.syswin.library.messaging.all.spring.MqImplementation.REDIS;
import static com.syswin.library.messaging.all.spring.MqImplementation.REDIS_PERSISTENCE;
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
  MqProducerConfig redisPersistentProducerConfig() {
    return new MqProducerConfig("redis-persistence", REDIS_PERSISTENCE);
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
  MqConsumerConfig redisPersistentConsumerConfig(Consumer<String> listener) {
    return MqConsumerConfig.create()
        .group("redis-persistence")
        .topic("brave-new-world")
        .type(CLUSTER)
        .implementation(REDIS_PERSISTENCE)
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

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

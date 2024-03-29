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

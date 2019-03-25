package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.embedded.EmbeddedMessageQueue;
import com.syswin.library.messaging.embedded.MessageQueue;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "library.messaging.embedded.enabled", havingValue = "true")
@Configuration
class DefaultEmbeddedMqMsgQueueConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Bean(initMethod = "start", destroyMethod = "shutdown")
  MessageQueue messageQueue(@Value("${library.messaging.embedded.poll.interval:50}") long pollInterval) {
    return new EmbeddedMessageQueue(pollInterval);
  }

}

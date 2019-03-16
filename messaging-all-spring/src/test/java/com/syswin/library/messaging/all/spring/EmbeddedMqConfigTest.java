package com.syswin.library.messaging.all.spring;

import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.MqProducer;
import com.syswin.library.messaging.embedded.EmbeddedMqConsumer;
import com.syswin.library.messaging.embedded.EmbeddedMqProducer;
import com.syswin.library.messaging.test.spring.MqConfigTestApp;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    "library.messaging.type=embedded",
    "app.producer.group=producer",
    "app.consumer.group=consumer",
    "app.consumer.topic=" + MqConfigTestBase.TOPIC,
    "app.consumer.tag=*"
}, classes = MqConfigTestApp.class)
public class EmbeddedMqConfigTest extends MqConfigTestBase {

  @Override
  Class<? extends MqConsumer> consumerType() {
    return EmbeddedMqConsumer.class;
  }

  @Override
  Class<? extends MqProducer> producerType() {
    return EmbeddedMqProducer.class;
  }
}

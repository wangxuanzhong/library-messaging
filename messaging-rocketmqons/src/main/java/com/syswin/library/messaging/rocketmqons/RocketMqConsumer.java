package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import java.util.Properties;

public class RocketMqConsumer extends AbstractRocketMqConsumer {

  public RocketMqConsumer(
      RocketMqConfig mqConfig,
      String topic,
      String tag,
      String messageModel,
      java.util.function.Consumer<String> messageConsumer) {
    super(mqConfig, topic, tag, messageModel, messageConsumer);
  }

  @Override
  protected Admin createConsumer(Properties properties) {
    return ONSFactory.createConsumer(properties);
  }

  @Override
  protected void subscribe(String topic, String tag) {
    ((Consumer) rmqConsumer).subscribe(topic, tag,
        (message, context) -> {
          try {
            RocketMqConsumer.this.consume(message);
          } catch (Exception e) {
            log.error("Failed to consume message from Rocket MQ: {}", message, e);
            return Action.ReconsumeLater;
          }
          return Action.CommitMessage;
        });
  }

}

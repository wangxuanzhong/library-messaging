package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import java.util.Properties;

public class RocketMqOnsConsumer extends AbstractRocketMqOnsConsumer<Consumer> {

  public RocketMqOnsConsumer(
      RocketMqOnsConfig mqConfig,
      String topic,
      String tag,
      String messageModel,
      java.util.function.Consumer<String> messageConsumer) {
    super(mqConfig, topic, tag, messageModel, messageConsumer);
  }

  @Override
  protected Consumer createConsumer(Properties properties) {
    return ONSFactory.createConsumer(properties);
  }

  @Override
  protected void subscribe(String topic, String tag) {
    rmqConsumer.subscribe(topic, tag,
        (message, context) -> {
          try {
            RocketMqOnsConsumer.this.consume(message);
          } catch (Exception e) {
            log.error("Failed to consume message from Rocket MQ: {}", message, e);
            return Action.ReconsumeLater;
          }
          return Action.CommitMessage;
        });
  }

}

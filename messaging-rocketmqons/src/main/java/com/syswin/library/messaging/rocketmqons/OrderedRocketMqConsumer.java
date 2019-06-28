package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import java.util.Properties;
import java.util.function.Consumer;

public class OrderedRocketMqConsumer extends AbstractRocketMqConsumer {

  public OrderedRocketMqConsumer(
      RocketMqConfig mqConfig,
      String topic,
      String tag,
      String messageModel,
      Consumer<String> messageConsumer) {
    super(mqConfig, topic, tag, messageModel, messageConsumer);
  }

  @Override
  protected Admin createConsumer(Properties properties) {
    return ONSFactory.createOrderedConsumer(consumerProperties);
  }

  @Override
  protected void subscribe(String topic, String tag) {
    ((OrderConsumer) rmqConsumer).subscribe(topic, tag,
        (message, context) -> {
          try {
            OrderedRocketMqConsumer.this.consume(message);
          } catch (Exception e) {
            log.error("Failed to consume message from Rocket MQ: {}", message, e);
            return OrderAction.Suspend;
          }
          return OrderAction.Success;
        });
  }
}

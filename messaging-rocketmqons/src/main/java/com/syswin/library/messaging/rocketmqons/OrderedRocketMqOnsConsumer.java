package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import java.util.Properties;
import java.util.function.Consumer;

public class OrderedRocketMqOnsConsumer extends AbstractRocketMqOnsConsumer<OrderConsumer> {

  public OrderedRocketMqOnsConsumer(
      RocketMqOnsConfig mqConfig,
      String topic,
      String tag,
      String messageModel,
      Consumer<String> messageConsumer) {
    super(mqConfig, topic, tag, messageModel, messageConsumer);
  }

  @Override
  protected OrderConsumer createConsumer(Properties properties) {
    return ONSFactory.createOrderedConsumer(consumerProperties);
  }

  @Override
  protected void subscribe(String topic, String tag) {
    rmqConsumer.subscribe(topic, tag,
        (message, context) -> {
          try {
            OrderedRocketMqOnsConsumer.this.consume(message);
          } catch (Exception e) {
            log.error("Failed to consume message from Rocket MQ: {}", message, e);
            return OrderAction.Suspend;
          }
          return OrderAction.Success;
        });
  }
}

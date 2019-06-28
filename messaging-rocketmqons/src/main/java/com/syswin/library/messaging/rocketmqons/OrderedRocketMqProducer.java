package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.impl.rocketmq.OrderProducerImpl;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.syswin.library.messaging.MessagingException;

public class OrderedRocketMqProducer extends AbstractRocketMqProducer {

  public OrderedRocketMqProducer(RocketMqConfig mqConfig) {
    super(mqConfig, null);
  }

  @Override
  protected Admin createProducer(RocketMqConfig mqConfig, LocalTransactionChecker checker) {
    return ONSFactory.createOrderProducer(mqConfig);
  }

  @Override
  public final void sendOrderly(String message, String topic)
      throws MessagingException {
    doSend(message, topic, "", "", this::loadBalancedSend);
  }

  private SendResult loadBalancedSend(Message message) throws MessagingException {
    String topic = message.getTopic();
    try {
      return ((OrderProducerImpl) rmqProducer).send(message, topic);
    } catch (Exception e) {
      throw new MessagingException(String
          .format("Failed to send message with topic: %s to Rocket MQ %s", topic,
              producerProperties.get(PropertyKeyConst.NAMESRV_ADDR)), e);
    }
  }
}

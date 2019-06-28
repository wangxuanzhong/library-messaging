package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.Message.SystemPropKey;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.impl.rocketmq.ONSUtil;
import com.aliyun.openservices.ons.api.impl.rocketmq.ProducerImpl;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.syswin.library.messaging.MessagingException;

public class RocketMqProducer extends AbstractRocketMqProducer {

  public RocketMqProducer(RocketMqConfig mqConfig) {
    super(mqConfig, null);
  }

  @Override
  protected Admin createProducer(RocketMqConfig mqConfig, LocalTransactionChecker checker) {
    return ONSFactory.createProducer(mqConfig);
  }

  @Override
  public final void send(String message, String topic, String tags, String keys)
      throws MessagingException {
    doSend(message, topic, tags, keys, this::loadBalancedSend);
  }

  @Override
  public final void sendRandomly(String message, String topic)
      throws MessagingException {
    doSend(message, topic, "", "", ((ProducerImpl) rmqProducer)::send);
  }

  private SendResult loadBalancedSend(Message message) throws MessagingException {
    String msgTags = message.getUserProperties(SystemPropKey.TAG);
    String topic = message.getTopic();
    String keys = message.getKey();
    String shardKey = msgTags == null ? topic : msgTags;
    try {
      com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult sendResultRMQ = ((ProducerImpl) rmqProducer)
          .getDefaultMQProducer().send(ONSUtil.msgConvert(message), (queues, msg, arg) -> {
            int index = Math.abs(arg.hashCode() % queues.size());
            return queues.get(index);
          }, shardKey);
      message.setMsgID(sendResultRMQ.getMsgId());
      SendResult sendResult = new SendResult();
      sendResult.setTopic(sendResultRMQ.getMessageQueue().getTopic());
      sendResult.setMessageId(sendResultRMQ.getMsgId());
      return sendResult;
    } catch (Exception e) {
      throw new MessagingException(String
          .format("Failed to send message with topic: %s, tag: %s, keys: %s to Rocket MQ %s", topic, msgTags, keys,
              producerProperties.get(PropertyKeyConst.NAMESRV_ADDR)), e);
    }
  }
}

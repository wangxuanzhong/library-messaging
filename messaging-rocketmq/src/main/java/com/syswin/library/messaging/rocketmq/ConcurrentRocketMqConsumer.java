package com.syswin.library.messaging.rocketmq;

import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentRocketMqConsumer extends AbstractRocketMqConsumer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final MessageListenerConcurrently messageListener;

  public ConcurrentRocketMqConsumer(String groupName,
      String topic,
      String tag,
      String brokerAddress,
      Consumer<String> messageConsumer,
      MessageModel messageModel) {

    super(groupName, topic, tag, brokerAddress, messageModel, messageConsumer);

    messageListener = (messages, context) -> {
      try {
        consume(messages);
      } catch (Exception e) {
        log.error("Failed to consume messages from Rocket MQ: {}", messages, e);
        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
      }
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    };
  }

  @Override
  void registerMessageListener() {
    consumer.registerMessageListener(messageListener);
    log.info("Registered concurrent message listener");
  }
}

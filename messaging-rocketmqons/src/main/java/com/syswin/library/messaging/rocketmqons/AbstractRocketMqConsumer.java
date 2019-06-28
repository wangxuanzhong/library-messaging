package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRocketMqConsumer implements MqConsumer {

  protected static final Logger log = LoggerFactory.getLogger(AbstractRocketMqConsumer.class);

  private final String topic;
  private final String tag;
  private final Consumer<String> messageConsumer;
  protected Properties consumerProperties;
  protected final Admin rmqConsumer;

  /**
   * @param topic
   * @param tag
   * @param messageConsumer
   */
  AbstractRocketMqConsumer(RocketMqConfig mqConfig, String topic, String tag, String messageModel,
      Consumer<String> messageConsumer) {
    this.topic = topic;
    this.tag = tag;
    this.messageConsumer = messageConsumer;

    consumerProperties = new Properties();
    consumerProperties.putAll(mqConfig);
    // messageModel的取值见：PropertyValueConst
    consumerProperties.put(PropertyKeyConst.MessageModel, messageModel);
    consumerProperties.put(PropertyKeyConst.InstanceName, UUID.randomUUID().toString());
    rmqConsumer = createConsumer(consumerProperties);
  }

  @Override
  public final void start() throws MessagingException {
    try {
      subscribe(topic, tag);
      rmqConsumer.start();
      log.info("Rocket MQ consumer {} in group {} is listening on topic {} tag {} with namesrv {}",
          getInstanceName(),
          getGroupId(),
          topic,
          tag,
          getNamesrvAddr());
    } catch (RuntimeException e) {
      throw new MessagingException("Failed to start Rocket MQ consumer with namesrv " +
          getNamesrvAddr(), e);
    }
  }

  @Override
  public final void shutdown() {
    rmqConsumer.shutdown();
    log.info("Rocket MQ consumer {} in group {} shut down successfully",
        getInstanceName(),
        getGroupId());
  }

  @Override
  public String topic() {
    return topic;
  }

  protected final void consume(Message message) {
    log.info("Rocket MQ consumer received message {} on topic {} tag {}", message, topic, tag);
    messageConsumer.accept(new String(message.getBody()));
  }

  protected abstract Admin createConsumer(Properties properties);

  protected abstract void subscribe(String topic, String tag);

  private String getNamesrvAddr() {
    return consumerProperties.getProperty(PropertyKeyConst.NAMESRV_ADDR);
  }

  private String getGroupId() {
    return consumerProperties.getProperty(PropertyKeyConst.GROUP_ID);
  }

  private String getInstanceName() {
    return consumerProperties.getProperty(PropertyKeyConst.InstanceName);
  }

}

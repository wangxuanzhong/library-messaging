package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;

import java.util.function.Consumer;

public class MqConsumerConfig {

  private final String group;
  private final String topic;
  private final String tag;
  private final MqConsumerType type;
  private final Consumer<String> listener;
  private final boolean concurrent;

  public MqConsumerConfig(String group, String topic, Consumer<String> listener) {
    this(group, topic, "", listener);
  }

  public MqConsumerConfig(String group, String topic, String tag, Consumer<String> listener) {
    this(group, topic, tag, CLUSTER, listener, true);
  }

  public MqConsumerConfig(String group, String topic, String tag, MqConsumerType type, Consumer<String> listener, boolean concurrent) {
    this.topic = topic;
    this.tag = tag;
    this.type = type;
    this.listener = listener;
    this.concurrent = concurrent;
    this.group = group;
  }

  public String group() {
    return group;
  }

  public String topic() {
    return topic;
  }

  public String tag() {
    return tag;
  }

  public MqConsumerType type() {
    return type;
  }

  public Consumer<String> listener() {
    return listener;
  }

  public boolean isConcurrent() {
    return concurrent;
  }
}

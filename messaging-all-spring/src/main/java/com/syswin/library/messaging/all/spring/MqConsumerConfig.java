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

  public static Builder create() {
    return new Builder();
  }

  private MqConsumerConfig(String group, String topic, String tag, MqConsumerType type, Consumer<String> listener, boolean concurrent) {
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

  public static class Builder {

    private String group;
    private String topic;
    private Consumer<String> listener;
    private String tag = "";
    private MqConsumerType type = CLUSTER;
    private boolean concurrent = true;

    private Builder() {
    }

    public Builder group(String group) {
      this.group = group;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder listener(Consumer<String> listener) {
      this.listener = listener;
      return this;
    }

    public Builder tag(String tag) {
      this.tag = tag;
      return this;
    }

    public Builder type(MqConsumerType type) {
      this.type = type;
      return this;
    }

    public Builder concurrent() {
      this.concurrent = true;
      return this;
    }

    public Builder sequential() {
      this.concurrent = false;
      return this;
    }

    public MqConsumerConfig build() {
      return new MqConsumerConfig(group, topic, tag, type, listener, concurrent);
    }
  }
}

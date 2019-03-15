package com.syswin.library.messaging.all.spring;

public class MqProducerConfig {

  private final String group;

  public MqProducerConfig(String group) {
    this.group = group;
  }

  public String group() {
    return group;
  }
}

package com.syswin.library.messaging.all.spring;

public class MqProducerConfig {

  private final String group;
  private final MqImplementation mqImplementation;

  public MqProducerConfig(String group, MqImplementation mqImplementation) {
    this.group = group;
    this.mqImplementation = mqImplementation;
  }

  public String group() {
    return group;
  }

  public MqImplementation implementation() {
    return mqImplementation;
  }
}

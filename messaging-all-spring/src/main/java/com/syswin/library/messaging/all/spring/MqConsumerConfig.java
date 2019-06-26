/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.messaging.all.spring;

import static com.syswin.library.messaging.all.spring.MqConsumerType.CLUSTER;

import java.util.function.Consumer;

public class MqConsumerConfig {

  private final String group;
  private final String topic;
  private final String tag;
  private final MqConsumerType type;
  private final MqImplementation mqImplementation;
  private final Consumer<String> listener;
  private final boolean concurrent;

  public static Builder create() {
    return new Builder();
  }

  private MqConsumerConfig(String group,
      String topic,
      String tag,
      MqConsumerType type,
      MqImplementation mqImplementation,
      Consumer<String> listener,
      boolean concurrent) {
    this.topic = topic;
    this.tag = tag;
    this.type = type;
    this.mqImplementation = mqImplementation;
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

  public MqImplementation implementation() {
    return mqImplementation;
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
    private MqImplementation implementation;
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

    public Builder implementation(MqImplementation implementation) {
      this.implementation = implementation;
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
      return new MqConsumerConfig(group, topic, tag, type, implementation, listener, concurrent);
    }
  }
}

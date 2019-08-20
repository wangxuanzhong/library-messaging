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

package com.syswin.library.messaging.redis.spring;

class RedisTopic {

  private static final String TOPIC_PREFIX = "topic:";
  private static final String EXPIRY_PREFIX = "expiry:";
  private static final String PAYLOAD_PREFIX = "payload:";

  private final String topic;
  private final String queue;
  private final String seqNo;
  private final String offset;

  RedisTopic(String topic) {
    this.topic = topic;
    this.queue = TOPIC_PREFIX + topic + ":queue";
    this.seqNo = TOPIC_PREFIX + topic + ":seqNo";
    this.offset = TOPIC_PREFIX + topic + ":offset:";
  }

  String queue() {
    return queue;
  }

  String offset(String consumerName) {
    return offset + consumerName;
  }

  String seqNo() {
    return seqNo;
  }

  public String topic() {
    return topic;
  }

  String toPayload(long currentSeqNo, String message, int expiryTimeSeconds) {
    // added seq to avoid duplicate message
    return "seq:" + currentSeqNo + "," + EXPIRY_PREFIX + (System.currentTimeMillis() + 1000 * expiryTimeSeconds) + "," + PAYLOAD_PREFIX + message;
  }

  String toMessage(String payload) {
    return payload.substring(payload.indexOf(PAYLOAD_PREFIX) + PAYLOAD_PREFIX.length());
  }

  boolean expired(String payload) {
    String expiryTime = payload.substring(payload.indexOf(EXPIRY_PREFIX) + EXPIRY_PREFIX.length(), payload.indexOf("," + PAYLOAD_PREFIX));
    return System.currentTimeMillis() > Long.valueOf(expiryTime);
  }
}
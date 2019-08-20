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

import com.syswin.library.messaging.MessageDeliverException;
import com.syswin.library.messaging.MqProducer;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

public class RedisPersistentMqProducer implements MqProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final MessageRedisTemplate redisTemplate;
  private final int expiryTimeSeconds;
  private final Function<RedisTopic, Long> sequenceSupplier;

  public RedisPersistentMqProducer(MessageRedisTemplate redisTemplate, int expiryTimeSeconds) {
    this(redisTemplate, new RedisSequenceSupplier(redisTemplate), expiryTimeSeconds);
  }

  RedisPersistentMqProducer(MessageRedisTemplate redisTemplate, Function<RedisTopic, Long> sequenceSupplier, int expiryTimeSeconds) {
    this.redisTemplate = redisTemplate;
    this.expiryTimeSeconds = expiryTimeSeconds;
    this.sequenceSupplier = sequenceSupplier;
  }

  @Override
  public void start() {

  }

  @Override
  public void send(String message, String topic, String tags, String keys) throws MessageDeliverException {
    doSend(message, topic);
  }

  @Override
  public void sendOrderly(String message, String topic) throws MessageDeliverException {
    doSend(message, topic);
  }

  @Override
  public void sendRandomly(String message, String topic) throws MessageDeliverException {
    doSend(message, topic);
  }

  @Override
  public void shutdown() {

  }

  private void doSend(String message, String topic) throws MessageDeliverException {
    try {
      RedisTopic redisTopic = new RedisTopic(topic);
      expireHead(redisTopic);
      double currentSeqNo = sequenceSupplier.apply(redisTopic);
      redisTemplate.opsForZSet().add(redisTopic.queue(), redisTopic.toPayload((long) currentSeqNo, message, expiryTimeSeconds), currentSeqNo);
      log.debug("Sent message {} to topic {} with seqNo {}", message, redisTopic.topic(), currentSeqNo);
    } catch (Exception e) {
      throw new MessageDeliverException(
          String.format("Failed to send message with topic: %s to Redis", topic),
          e);
    }
  }

  private void expireHead(RedisTopic redisTopic) {
    Set<TypedTuple<String>> messages = redisTemplate.opsForZSet().rangeWithScores(redisTopic.queue(), 0, 0);
    for (TypedTuple<String> tuple : messages) {
      String msg = tuple.getValue();
      if (redisTopic.expired(msg)) {
        redisTemplate.opsForZSet().removeRangeByScore(redisTopic.queue(), tuple.getScore(), tuple.getScore());
        log.debug("Message {} expired on topic {}", msg, redisTopic.topic());
      }
    }
  }

  private static class RedisSequenceSupplier implements Function<RedisTopic, Long> {

    private final MessageRedisTemplate redisTemplate;

    RedisSequenceSupplier(MessageRedisTemplate redisTemplate) {
      this.redisTemplate = redisTemplate;
    }

    @Override
    public Long apply(RedisTopic redisTopic) {
      return redisTemplate.opsForValue().increment(redisTopic.seqNo(), 1L);
    }
  }
}

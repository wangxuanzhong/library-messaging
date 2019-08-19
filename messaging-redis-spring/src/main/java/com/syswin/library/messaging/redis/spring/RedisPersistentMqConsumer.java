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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.syswin.library.messaging.MqConsumer;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

public class RedisPersistentMqConsumer implements MqConsumer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String consumerName;
  private final Consumer<String> messageConsumer;
  private final MessageRedisTemplate redisTemplate;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final String offset;
  private final RedisTopic redisTopic;
  private final int pollingInterval;

  public RedisPersistentMqConsumer(String topic,
      String consumerName,
      Consumer<String> messageConsumer,
      MessageRedisTemplate redisTemplate,
      int pollingInterval) {

    this.consumerName = consumerName;
    this.messageConsumer = messageConsumer;
    this.redisTemplate = redisTemplate;
    this.redisTopic = new RedisTopic(topic);
    this.offset = redisTopic.offset(consumerName);
    this.pollingInterval = pollingInterval;
  }

  @Override
  public void start() {
    scheduledExecutor.scheduleWithFixedDelay(() -> {
      Set<TypedTuple<String>> tuples = redisTemplate.opsForZSet().rangeWithScores(redisTopic.queue(), 0, 0);
      if (tuples.isEmpty()) {
        return;
      }

      long currentOffset = currentOffset(tuples);
      log.debug("Current offset of consumer {} on topic {} is {}", consumerName, redisTopic.topic(), currentOffset);

      Set<String> messages = redisTemplate.opsForZSet().rangeByScore(redisTopic.queue(), currentOffset, currentOffset);

      if (!messages.isEmpty()) {
        log.debug("Messages of consumer {} on topic {} are {}", consumerName, redisTopic.topic(), messages);

        try {
          messages.forEach(payload -> messageConsumer.accept(redisTopic.toMessage(payload)));
          redisTemplate.opsForValue().set(offset, String.valueOf(currentOffset + 1));
          log.debug("Forwarded offset of consumer {} on topic {} by 1", consumerName, redisTopic.topic());
        } catch (Exception e) {
          log.error("Failed to consume messages from Redis Persistent MQ: {}", messages, e);
        }
      }
    }, pollingInterval, pollingInterval, MILLISECONDS);
  }

  @Override
  public void shutdown() {
    scheduledExecutor.shutdownNow();
  }

  @Override
  public String topic() {
    return redisTopic.topic();
  }

  private long currentOffset(Set<TypedTuple<String>> tuples) {
    String value = redisTemplate.opsForValue().get(offset);
    long currentOffset = 0L;
    if (value != null) {
      currentOffset = Long.parseLong(value);
    }

    for (TypedTuple<String> tuple : tuples) {
      currentOffset = Math.max(currentOffset, tuple.getScore().longValue());
    }
    return currentOffset;
  }
}

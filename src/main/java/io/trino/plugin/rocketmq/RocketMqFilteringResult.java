/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.rocketmq;

import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Map;

/**
 * rocketmq filter result
 */
public class RocketMqFilteringResult {

    private final List<MessageQueue> messageQueues;
    private final Map<MessageQueue, TopicOffset> messageQueueTopicOffsets;

    public RocketMqFilteringResult(List<MessageQueue> messageQueues, Map<MessageQueue, TopicOffset> messageQueueTopicOffsets){
        this.messageQueues = messageQueues;
        this.messageQueueTopicOffsets = messageQueueTopicOffsets;
    }

    public List<MessageQueue> getMessageQueues() {
        return messageQueues;
    }

    public Map<MessageQueue, TopicOffset> getMessageQueueTopicOffsets() {
        return messageQueueTopicOffsets;
    }
}

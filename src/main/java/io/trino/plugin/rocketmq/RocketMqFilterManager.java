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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.rocketmq.client.RocketMqConsumerFactory;
import io.trino.plugin.rocketmq.handle.RocketMqColumnHandle;
import io.trino.plugin.rocketmq.handle.RocketMqTableHandle;
import io.trino.plugin.rocketmq.split.Range;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.rocketmq.common.admin.RollbackStats;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.rocketmq.RocketMqInternalFieldManager.OFFSET_TIMESTAMP_FIELD;
import static io.trino.plugin.rocketmq.RocketMqInternalFieldManager.QUEUE_ID_FIELD;
import static io.trino.plugin.rocketmq.RocketMqInternalFieldManager.QUEUE_OFFSET_FIELD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;

/**
 * rocketmq filter manager
 */
public class RocketMqFilterManager {

    private static final long INVALID_RANGE_INDEX = -1;
    private RocketMqConsumerFactory consumerFactory;

    @Inject
    public RocketMqFilterManager(RocketMqConsumerFactory consumerFactory){
        this.consumerFactory = requireNonNull(consumerFactory, "RocketMq consumer factory is null");
    }

    /**
     * get filter result
     * @param session
     * @param rocketMQTableHandle
     * @param messageQueues
     * @param messageQueueTopicOffsets
     * @return
     */
    public RocketMqFilteringResult filter(ConnectorSession session,
                                          RocketMqTableHandle rocketMQTableHandle,
                                          List<MessageQueue> messageQueues,
                                          Map<MessageQueue, TopicOffset> messageQueueTopicOffsets){
        requireNonNull(session, "session is null");
        requireNonNull(rocketMQTableHandle, "Rocketmq table handle is null");
        requireNonNull(messageQueues, "Message queues is null");
        requireNonNull(messageQueueTopicOffsets, "messageQueueTopicOffsets is null");

        // begin offset
        Map<MessageQueue, Long> messageQueueBeginOffsets = messageQueueTopicOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry<MessageQueue, TopicOffset>::getKey, topicOffset-> topicOffset.getValue().getMinOffset()));
        // end offset
        Map<MessageQueue, Long> messageQueueEndOffsets = messageQueueTopicOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry<MessageQueue, TopicOffset>::getKey, topicOffset-> topicOffset.getValue().getMaxOffset()));

        TupleDomain<ColumnHandle> constraint = rocketMQTableHandle.getConstraint();
        verify(!constraint.isNone(), "constraint is none");
        TupleDomain<ColumnHandle> tupleDomain = rocketMQTableHandle.getConstraint();
        if(!tupleDomain.isAll()){
            Set<Long> messageIds = messageQueues.stream().map(messageQueue -> (long) messageQueue.getQueueId()).collect(toImmutableSet());
            Optional<Range> offsetRanged = Optional.empty();
            Optional<Range> offsetTimestampRanged = Optional.empty();
            Set<Long> messageIdsFiltered = messageIds;

            Optional<Map<ColumnHandle, Domain>> domains = constraint.getDomains();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
                RocketMqColumnHandle columnHandle = (RocketMqColumnHandle) entry.getKey();
                if (!columnHandle.isInternal()) {
                    continue;
                }
                switch (columnHandle.getName()) {
                    case QUEUE_OFFSET_FIELD:
                        offsetRanged = filterRangeByDomain(entry.getValue());
                        break;
                    case QUEUE_ID_FIELD:
                        messageIds = filterValuesByDomain(entry.getValue(), messageIds);
                        break;
                    case OFFSET_TIMESTAMP_FIELD:
                        offsetTimestampRanged = filterRangeByDomain(entry.getValue());
                        break;
                    default:
                        break;
                }
            }
            // push down offset
            if (offsetRanged.isPresent()) {
                Range range = offsetRanged.get();
                messageQueueBeginOffsets = overrideMessageQueueBeginOffsets(
                        messageQueueBeginOffsets,
                        partition -> (range.getBegin() != INVALID_RANGE_INDEX) ? Optional.of(range.getBegin()) : Optional.empty());
                messageQueueEndOffsets = overrideMessageQueueEndOffsets(
                        messageQueueEndOffsets,
                        partition -> (range.getEnd() != INVALID_RANGE_INDEX) ? Optional.of(range.getEnd()) : Optional.empty());
            }

            // push down timestamp if possible
            if (offsetTimestampRanged.isPresent()) {
                DefaultMQAdminExt rocketMQAdmin ;
                try  {
                    rocketMQAdmin = this.consumerFactory.admin(session);
                    Optional<Range> finalOffsetTimestampRanged = offsetTimestampRanged;
                    if (offsetTimestampRanged.get().getBegin() > INVALID_RANGE_INDEX) {
                        messageQueueBeginOffsets = overrideMessageQueueBeginOffsets(
                                messageQueueBeginOffsets,
                                messageQueue -> findOffsetsForTimestamp(rocketMQAdmin, messageQueue, finalOffsetTimestampRanged.get().getBegin()));
                    }
                    if (offsetTimestampRanged.get().getEnd() > INVALID_RANGE_INDEX) {
                        messageQueueEndOffsets = overrideMessageQueueEndOffsets(messageQueueEndOffsets,
                                messageQueue -> findOffsetsForTimestamp(rocketMQAdmin, messageQueue, finalOffsetTimestampRanged.get().getEnd()));
                    }
                    if (rocketMQAdmin != null){
                        rocketMQAdmin.shutdown();
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            // push down messageQueues
            final Set<Long> finalMessageQueueIdsFiltered = messageIdsFiltered;
            List<MessageQueue> messageQueueFilteredInfos = messageQueues.stream()
                    .filter(messageQueue -> finalMessageQueueIdsFiltered.contains((long) messageQueue.getQueueId()))
                    .collect(toImmutableList());
            overrideMessageQueueOffsets(messageQueueTopicOffsets, messageQueueBeginOffsets, messageQueueEndOffsets);
            return new RocketMqFilteringResult(messageQueueFilteredInfos, messageQueueTopicOffsets);
        }
        overrideMessageQueueOffsets(messageQueueTopicOffsets, messageQueueBeginOffsets, messageQueueEndOffsets);
        return new RocketMqFilteringResult(messageQueues, messageQueueTopicOffsets);

    }


    /**
     * Override message queue offsets
     * @param messageQueueTopicOffsets
     * @param messageQueueBeginOffsets
     * @param messageQueueEndOffsets
     * @return
     */
    private static Map<MessageQueue, TopicOffset> overrideMessageQueueOffsets(
            Map<MessageQueue, TopicOffset> messageQueueTopicOffsets,
            Map<MessageQueue, Long> messageQueueBeginOffsets,
            Map<MessageQueue, Long> messageQueueEndOffsets)
    {

        for (Map.Entry<MessageQueue, TopicOffset> entry: messageQueueTopicOffsets.entrySet()){
            if (messageQueueBeginOffsets.containsKey(entry.getKey())){
                entry.getValue().setMinOffset(messageQueueBeginOffsets.get(entry.getKey()));
            }
            if (messageQueueEndOffsets.containsKey(entry.getKey())) {
                entry.getValue().setMaxOffset(messageQueueEndOffsets.get(entry.getKey()));
            }
        }
        return messageQueueTopicOffsets;
    }



    /**
     * Override message queue begin offsets
     * @param messageQueueBeginOffsets
     * @param overrideFunction
     * @return
     */
    private static Map<MessageQueue, Long> overrideMessageQueueBeginOffsets(Map<MessageQueue, Long> messageQueueBeginOffsets,
                                                                            Function<MessageQueue, Optional<Long>> overrideFunction) {
        ImmutableMap.Builder<MessageQueue, Long> messageQueueFilteredBeginOffsets = ImmutableMap.builder();
        messageQueueBeginOffsets.forEach((partition, partitionIndex) -> {
            Optional<Long> newOffset = overrideFunction.apply(partition);
            messageQueueFilteredBeginOffsets.put(partition, newOffset.map(index -> Long.max(partitionIndex, index)).orElse(partitionIndex));
        });
        return messageQueueFilteredBeginOffsets.buildOrThrow();
    }

    /**
     * Override message queue end offsets
     * @param messageQueueEndOffsets
     * @param overrideFunction
     * @return
     */
    private static Map<MessageQueue, Long> overrideMessageQueueEndOffsets(Map<MessageQueue, Long> messageQueueEndOffsets,
                                                                          Function<MessageQueue, Optional<Long>> overrideFunction) {
        ImmutableMap.Builder<MessageQueue, Long> filteredEndOffsets = ImmutableMap.builder();
        messageQueueEndOffsets.forEach((messageQueue, offset) -> {
            Optional<Long> newOffset = overrideFunction.apply(messageQueue);
            filteredEndOffsets.put(messageQueue, newOffset.map(index -> Long.min(offset, index)).orElse(offset));
        });
        return filteredEndOffsets.buildOrThrow();
    }


    private static Optional<Long> findOffsetsForTimestamp(DefaultMQAdminExt adminExt, MessageQueue messageQueue, long timestamp) {
        final long transferTimestamp = floorDiv(timestamp, MICROSECONDS_PER_MILLISECOND);
        try {
            List<RollbackStats> rollbackStats = adminExt.resetOffsetByTimestampOld(adminExt.getAdminExtGroup(), messageQueue.getTopic(), transferTimestamp, true);
            for (RollbackStats stats : rollbackStats) {
                if (stats.getQueueId() == messageQueue.getQueueId()
                        && stats.getBrokerName().equals(messageQueue.getBrokerName())) {
                  return Optional.of(stats.getConsumerOffset());
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<Range> filterRangeByDomain(Domain domain)
    {
        Long low = INVALID_RANGE_INDEX;
        Long high = INVALID_RANGE_INDEX;
        if (domain.isSingleValue()) {
            // still return range for single value case like (_partition_offset=XXX or _timestamp=XXX)
            low = (long) domain.getSingleValue();
            high = (long) domain.getSingleValue();
        }
        else {
            ValueSet valueSet = domain.getValues();
            if (valueSet instanceof SortedRangeSet) {
                // still return range for single value case like (_partition_offset in (XXX1,XXX2) or _timestamp in XXX1, XXX2)
                Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                List<io.trino.spi.predicate.Range> rangeList = ranges.getOrderedRanges();
                if (rangeList.stream().allMatch(io.trino.spi.predicate.Range::isSingleValue)) {
                    List<Long> values = rangeList.stream()
                            .map(range -> (Long) range.getSingleValue())
                            .collect(toImmutableList());
                    low = Collections.min(values);
                    high = Collections.max(values);
                } else {
                    io.trino.spi.predicate.Range span = ranges.getSpan();
                    low = getLowIncludedValue(span).orElse(low);
                    high = getHighIncludedValue(span).orElse(high);
                }
            }
        }
        if (high != INVALID_RANGE_INDEX) {
            high = high + 1;
        }
        return Optional.of(new Range(low, high));
    }


    public static Set<Long> filterValuesByDomain(Domain domain, Set<Long> sourceValues) {
        requireNonNull(sourceValues, "sourceValues is none");
        if (domain.isSingleValue()) {
            long singleValue = (long) domain.getSingleValue();
            return sourceValues.stream().filter(sourceValue -> sourceValue == singleValue).collect(toImmutableSet());
        }
        ValueSet valueSet = domain.getValues();
        if (valueSet instanceof SortedRangeSet) {
            Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
            List<io.trino.spi.predicate.Range> rangeList = ranges.getOrderedRanges();
            if (rangeList.stream().allMatch(io.trino.spi.predicate.Range::isSingleValue)) {
                return rangeList.stream()
                        .map(range -> (Long) range.getSingleValue())
                        .filter(sourceValues::contains)
                        .collect(toImmutableSet());
            }
            // still return values for range case like (_partition_id > 1)
            io.trino.spi.predicate.Range span = ranges.getSpan();
            long low = getLowIncludedValue(span).orElse(0L);
            long high = getHighIncludedValue(span).orElse(Long.MAX_VALUE);
            return sourceValues.stream()
                    .filter(item -> item >= low && item <= high)
                    .collect(toImmutableSet());
        }
        return sourceValues;
    }

    private static Optional<Long> getLowIncludedValue(io.trino.spi.predicate.Range range) {
        long step = nativeRepresentationGranularity(range.getType());
        return range.getLowValue()
                .map(Long.class::cast)
                .map(value -> range.isLowInclusive() ? value : value + step);
    }

    private static Optional<Long> getHighIncludedValue(io.trino.spi.predicate.Range range) {
        long step = nativeRepresentationGranularity(range.getType());
        return range.getHighValue()
                .map(Long.class::cast)
                .map(value -> range.isHighInclusive() ? value : value - step);
    }

    private static long nativeRepresentationGranularity(Type type) {
        if (type == BIGINT) {
            return 1;
        }
        if (type instanceof TimestampType && ((TimestampType) type).getPrecision() == 3) {
            return 1000;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}

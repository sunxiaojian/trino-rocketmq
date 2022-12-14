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
package io.trino.plugin.rocketmq.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.min;

public class Range {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Range.class).instanceSize();

    private final long begin; // inclusive
    private final long end; // exclusive

    @JsonCreator
    public Range(@JsonProperty("begin") long begin, @JsonProperty("end") long end)
    {
        this.begin = begin;
        this.end = end;
    }

    @JsonProperty
    public long getBegin()
    {
        return begin;
    }

    @JsonProperty
    public long getEnd()
    {
        return end;
    }

    public List<Range> partition(int partitionSize)
    {
        ImmutableList.Builder<Range> partitions = ImmutableList.builder();
        long position = begin;
        while (position <= end) {
            partitions.add(new Range(position, min(position + partitionSize, end)));
            position += partitionSize;
        }
        return partitions.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("begin", begin)
                .add("end", end)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}

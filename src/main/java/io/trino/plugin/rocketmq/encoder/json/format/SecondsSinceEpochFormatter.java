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
package io.trino.plugin.rocketmq.encoder.json.format;

import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;

import static io.trino.plugin.rocketmq.encoder.json.format.util.TimeConversions.scaleEpochMillisToSeconds;
import static io.trino.plugin.rocketmq.encoder.json.format.util.TimeConversions.scalePicosToSeconds;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;

public class SecondsSinceEpochFormatter implements JsonDateTimeFormatter {
    public static boolean isSupportedType(Type type) {
        return type.equals(TIME_MILLIS) ||
                type.equals(TIMESTAMP_MILLIS) ||
                type.equals(TIMESTAMP_TZ_MILLIS);
    }

    @Override
    public String formatTime(SqlTime value, int precision) {
        return String.valueOf(scalePicosToSeconds(value.getPicos()));
    }

    @Override
    public String formatTimestamp(SqlTimestamp value) {
        return String.valueOf(scaleEpochMillisToSeconds(value.getMillis()));
    }

    @Override
    public String formatTimestampWithZone(SqlTimestampWithTimeZone value) {
        return String.valueOf(scaleEpochMillisToSeconds(value.getEpochMillis()));
    }
}

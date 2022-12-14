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
package io.trino.plugin.rocketmq.encoder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.trino.plugin.rocketmq.encoder.avro.AvroEncoderModule;
import io.trino.plugin.rocketmq.encoder.json.JsonRowEncoder;
import io.trino.plugin.rocketmq.encoder.json.JsonRowEncoderFactory;
import io.trino.plugin.rocketmq.encoder.raw.RawRowEncoder;
import io.trino.plugin.rocketmq.encoder.raw.RawRowEncoderFactory;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class EncoderModule implements Module {
    public static MapBinder<String, RowEncoderFactory> encoderFactory(Binder binder) {
        return newMapBinder(binder, String.class, RowEncoderFactory.class);
    }

    @Override
    public void configure(Binder binder) {
        MapBinder<String, RowEncoderFactory> encoderFactoriesByName = encoderFactory(binder);
        encoderFactoriesByName.addBinding(RawRowEncoder.NAME).to(RawRowEncoderFactory.class).in(SINGLETON);
        encoderFactoriesByName.addBinding(JsonRowEncoder.NAME).to(JsonRowEncoderFactory.class).in(SINGLETON);
        binder.install(new AvroEncoderModule());
        binder.bind(DispatchingRowEncoderFactory.class).in(SINGLETON);
    }
}

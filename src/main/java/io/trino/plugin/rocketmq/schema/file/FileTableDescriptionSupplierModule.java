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
package io.trino.plugin.rocketmq.schema.file;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.rocketmq.encoder.EncoderModule;
import io.trino.plugin.rocketmq.schema.ContentSchemaReader;
import io.trino.plugin.rocketmq.schema.TableDescriptionSupplier;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * File table description supplier module
 */
public class FileTableDescriptionSupplierModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {

        // Bind file table description supplier
        configBinder(binder).bindConfig(FileTableDescriptionSupplierConfig.class);
        binder.bind(TableDescriptionSupplier.class).toProvider(FileTableDescriptionSupplier.class).in(Scopes.SINGLETON);

        // Install decoder and encoder
        install(new DecoderModule());
        install(new EncoderModule());

        // Bind file schema reader
        binder.bind(ContentSchemaReader.class).to(FileContentSchemaReader.class).in(Scopes.SINGLETON);
    }
}

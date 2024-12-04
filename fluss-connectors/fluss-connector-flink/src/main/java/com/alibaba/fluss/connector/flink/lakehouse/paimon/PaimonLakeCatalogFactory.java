/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.lakehouse.paimon;

import com.alibaba.fluss.connector.flink.lakehouse.LakeCatalog;
import com.alibaba.fluss.connector.flink.lakehouse.LakeCatalogFactory;

import java.util.Map;

/** Factory to create {@link PaimonLakeCatalog}. */
public class PaimonLakeCatalogFactory extends LakeCatalogFactory {

    public static final String IDENTIFIER = "paimon";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeCatalog create(
            String catalogName, Map<String, String> catalogProperties, ClassLoader classLoader) {
        return new PaimonLakeCatalog(catalogName, catalogProperties, classLoader);
    }
}
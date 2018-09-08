/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bson.codecs.pojo;

import org.bson.codecs.Codec;

import java.util.Collection;

import static java.lang.String.format;

final class CollectionPropertyCodecProvider implements PropertyCodecProvider {
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Collection.class.isAssignableFrom(type.getType()) && type.getTypeParameters().size() == 1) {
            return new CollectionCodec(type.getType(), registry.get(type.getTypeParameters().get(0)));
        } else {
            return null;
        }
    }

}

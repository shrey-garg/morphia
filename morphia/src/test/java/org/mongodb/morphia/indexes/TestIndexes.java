/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.mongodb.morphia.indexes;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Collation;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.utils.IndexType;

import static com.mongodb.client.model.CollationAlternate.SHIFTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestIndexes extends TestBase {

    @Test
    public void testIndexes() {

        final Datastore datastore = getDatastore();
        datastore.deleteMany(datastore.find(TestWithIndexOption.class));

        final MongoCollection<TestWithIndexOption> indexOptionColl = getDatastore().getCollection(TestWithIndexOption.class);
        indexOptionColl.drop();
        assertFalse(indexOptionColl.listIndexes().iterator().hasNext());

        final MongoCollection<TestWithHashedIndex> hashIndexColl = getDatastore().getCollection(TestWithHashedIndex.class);
        hashIndexColl.drop();
        assertFalse(hashIndexColl.listIndexes().iterator().hasNext());

        if (serverIsAtLeastVersion(3.4)) {
            datastore.ensureIndexes(TestWithIndexOption.class, true);
            assertEquals(2, count(indexOptionColl.listIndexes().iterator()));
            ListIndexesIterable<Document> indexInfo = indexOptionColl.listIndexes();
            assertBackground(indexInfo);
            for (Document document : indexInfo) {
                if (document.get("name").equals("collated")) {
                    assertEquals(Document.parse("{ name : { $exists : true } }"),
                        document.get("partialFilterExpression"));
                    Document collation = (Document) document.get("collation");
                    assertEquals("en_US", collation.get("locale"));
                    assertEquals("upper", collation.get("caseFirst"));
                    assertEquals("shifted", collation.get("alternate"));
                    Assert.assertTrue(collation.getBoolean("backwards"));
                    assertEquals("upper", collation.get("caseFirst"));
                    Assert.assertTrue(collation.getBoolean("caseLevel"));
                    assertEquals("space", collation.get("maxVariable"));
                    Assert.assertTrue(collation.getBoolean("normalization"));
                    Assert.assertTrue(collation.getBoolean("numericOrdering"));
                    assertEquals(5, collation.get("strength"));
                }
            }
        }

        datastore.ensureIndexes(TestWithHashedIndex.class);
        assertEquals(2, count(hashIndexColl.listIndexes().iterator()));
        assertHashed(hashIndexColl.listIndexes());
    }

    private void assertBackground(final ListIndexesIterable<Document> indexInfo) {
        for (final Document Document : indexInfo) {
            if (!Document.getString("name").equals("_id_")) {
                Assert.assertTrue(Document.getBoolean("background"));
            }
        }
    }

    private void assertHashed(final ListIndexesIterable<Document> indexInfo) {
        for (final Document Document : indexInfo) {
            if (!Document.getString("name").equals("_id_")) {
                assertEquals(((Document) Document.get("key")).get("hashedValue"), "hashed");
            }
        }
    }

    @Entity(noClassnameStored = true)
    @Indexes({@Index(options = @IndexOptions(name = "collated",
        partialFilter = "{ name : { $exists : true } }",
        collation = @Collation(locale = "en_US", alternate = SHIFTED, backwards = true,
            caseFirst = CollationCaseFirst.UPPER, caseLevel = true, maxVariable = CollationMaxVariable.SPACE, normalization = true,
            numericOrdering = true, strength = CollationStrength.IDENTICAL)),
        fields = {@Field(value = "name")})})
    private static class TestWithIndexOption {
        private String name;

    }

    @Entity(noClassnameStored = true)
    @Indexes({@Index(options = @IndexOptions(), fields = {@Field(value = "hashedValue", type = IndexType.HASHED)})})
    private static class TestWithHashedIndex {
        private String hashedValue;
    }


}

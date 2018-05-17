/*
  Copyright (C) 2010 Olafur Gauti Gudmundsson
  <p/>
  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may
  obtain a copy of the License at
  <p/>
  http://www.apache.org/licenses/LICENSE-2.0
  <p/>
  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.
 */

package org.mongodb.morphia.indexes;

import com.mongodb.MongoWriteException;
import com.mongodb.client.ListIndexesIterable;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.entities.IndexOnValue;
import org.mongodb.morphia.entities.NamedIndexOnValue;
import org.mongodb.morphia.entities.UniqueIndexOnValue;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.utils.IndexType;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.testutil.IndexMatcher.doesNotHaveIndexNamed;
import static org.mongodb.morphia.testutil.IndexMatcher.hasIndexNamed;

public class TestIndexed extends TestBase {
    @Before
    @Override
    public void setUp() {
        super.setUp();
        getMorphia().map(UniqueIndexOnValue.class, IndexOnValue.class, NamedIndexOnValue.class);
    }

    @Test
    public void shouldNotCreateAnIndexWhenAnIndexedEntityIsMarkedAsNotSaved() {
        // given
        getMorphia().map(IndexOnValue.class, NoIndexes.class);
        Datastore ds = getDatastore();

        // when
        ds.ensureIndexes();
        ds.save(new IndexOnValue());
        ds.save(new NoIndexes());

        // then
        ListIndexesIterable<Document> indexes = getDatabase().getCollection("NoIndexes").listIndexes();
        assertEquals(1, count(indexes.iterator()));
    }

    @Test
    public void shouldThrowExceptionWhenAddingADuplicateValueForAUniqueIndex() {
        getMorphia().map(UniqueIndexOnValue.class);
        getDatastore().ensureIndexes();
        long value = 7L;

        try {
            final UniqueIndexOnValue entityWithUniqueName = new UniqueIndexOnValue();
            entityWithUniqueName.setValue(value);
            entityWithUniqueName.setUnique(1);
            getDatastore().save(entityWithUniqueName);

            final UniqueIndexOnValue entityWithSameName = new UniqueIndexOnValue();
            entityWithSameName.setValue(value);
            entityWithSameName.setUnique(2);
            getDatastore().save(entityWithSameName);

            Assert.fail("Should have gotten a duplicate key exception");
        } catch (Exception ignored) {
        }

        value = 10L;
        try {
            final UniqueIndexOnValue first = new UniqueIndexOnValue();
            first.setValue(1);
            first.setUnique(value);
            getDatastore().save(first);

            final UniqueIndexOnValue second = new UniqueIndexOnValue();
            second.setValue(2);
            second.setUnique(value);
            getDatastore().save(second);

            Assert.fail("Should have gotten a duplicate key exception");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testCanCreate2dSphereIndexes() {
        getMorphia().map(Place.class);

        getDatastore().ensureIndexes();

        ListIndexesIterable<Document> indexInfo = getDatastore().getCollection(Place.class).listIndexes();
        assertThat(count(indexInfo.iterator()), is(2));
        assertThat(indexInfo, hasIndexNamed("location_2dsphere"));
    }

    @Test
    public void testCanCreate2dSphereIndexesOnLegacyCoordinatePairs() {
        getMorphia().map(LegacyPlace.class);
        getDatastore().ensureIndexes();
        assertThat(getDatastore().getCollection(LegacyPlace.class).listIndexes(), hasIndexNamed("location_2dsphere"));
    }

    @Test
    public void testEmbeddedIndex() {
        final MappedClass mc = getMorphia().getMapper().addMappedClass(ContainsIndexedEmbed.class);

        ListIndexesIterable<Document> actual = getDatabase().getCollection(mc.getCollectionName()).listIndexes();
        assertThat(actual.toString(), actual, doesNotHaveIndexNamed("e.name_-1"));

        getDatastore().ensureIndexes(ContainsIndexedEmbed.class);
        actual = getDatabase().getCollection(mc.getCollectionName()).listIndexes();
        assertThat("dude", actual, hasIndexNamed("e.name_-1"));
    }

    @Test
    public void testIndexedEntity() {
        getDatastore().ensureIndexes();
        assertThat(getDatastore().getCollection(IndexOnValue.class).listIndexes(), hasIndexNamed("value_1"));

        getDatastore().save(new IndexOnValue());
        getDatastore().ensureIndexes();
        assertThat(getDatastore().getCollection(IndexOnValue.class).listIndexes(), hasIndexNamed("value_1"));
    }

    @Test
    public void testIndexedRecursiveEntity() {
        final MappedClass mc = getMorphia().getMapper().getMappedClass(CircularEmbeddedEntity.class);
        getDatastore().ensureIndexes();
        assertThat(getDatabase().getCollection(mc.getCollectionName()).listIndexes(), hasIndexNamed("a_1"));
    }

    @Test
    public void testIndexes() {
        final MappedClass mc = getMorphia().getMapper().addMappedClass(Ad2.class);

        assertThat(getDatabase().getCollection(mc.getCollectionName()).listIndexes(), doesNotHaveIndexNamed("active_1_lastMod_-1"));
        getDatastore().ensureIndexes(Ad2.class);
        assertThat(getDatabase().getCollection(mc.getCollectionName()).listIndexes(), hasIndexNamed("active_1_lastMod_-1"));
    }

    @Test
    public void testNamedIndexEntity() {
        getDatastore().ensureIndexes();

        assertThat(getDatastore().getCollection(NamedIndexOnValue.class).listIndexes(), hasIndexNamed("value_ascending"));
    }

    @Test(expected = MongoWriteException.class)
    public void testUniqueIndexedEntity() {
        getDatastore().ensureIndexes();
        assertThat(getDatastore().getCollection(UniqueIndexOnValue.class).listIndexes(), hasIndexNamed("l_ascending"));
        getDatastore().save(new UniqueIndexOnValue("a"));

        // this should throw...
        getDatastore().save(new UniqueIndexOnValue("v"));
    }

    @SuppressWarnings("unused")
    private static class Place {
        @Id
        private long id;

        @Indexed(IndexType.GEO2DSPHERE)
        private Object location;
    }

    @SuppressWarnings("unused")
    private static class LegacyPlace {
        @Id
        private long id;

        @Indexed(IndexType.GEO2DSPHERE)
        private double[] location;
    }

    private static class Ad {
        @Id
        private long id;

        @Property("lastMod")
        @Indexed
        private long lastModified;

        @Indexed
        private boolean active;
    }

    @Indexes(@Index(fields = {@Field("active"), @Field(value = "lastModified", type = IndexType.DESC)},
                       options = @IndexOptions(unique = true)))
    private static class Ad2 {
        @Id
        private long id;

        @Property("lastMod")
        @Indexed
        private long lastModified;

        @Indexed
        private boolean active;
    }

    @Embedded
    private static class IndexedEmbed {
        @Indexed(IndexType.DESC)
        private String name;
    }

    private static class ContainsIndexedEmbed {
        @Id
        private ObjectId id;
        private IndexedEmbed e;
    }

    private static class CircularEmbeddedEntity {
        @Id
        private ObjectId id = new ObjectId();
        private String name;
        @Indexed
        private CircularEmbeddedEntity a;
    }

    @Entity
    private static class NoIndexes {
        @Id
        private ObjectId id;

        @NotSaved
        private IndexOnValue indexedClass;
    }


    @Entity
    private static class MixedIndexDefinitions {
        @Id
        private ObjectId id;
        @Indexed(options = @IndexOptions(unique = true))
        private String name;
    }
}

package org.mongodb.morphia;


import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.IndexOptions;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.annotations.Indexes;

import java.util.Date;

public class TestExpireAfterSeconds extends TestBase {

    @Test
    public void testClassAnnotation() {
        getMapper().map(ClassAnnotation.class);
        getDatastore().ensureIndexes();

        getDatastore().save(new ClassAnnotation());

        final MongoDatabase db = getDatastore().getDatabase();
        final MongoCollection<Document> dbCollection = db.getCollection("ClassAnnotation");
        final ListIndexesIterable<Document> indexes = dbCollection.listIndexes();

        Assert.assertNotNull(indexes);
        Assert.assertEquals(2, count(indexes.iterator()));
        Document index = null;
        for (final Document candidateIndex : indexes) {
            if (candidateIndex.containsKey("expireAfterSeconds")) {
                index = candidateIndex;
            }
        }
        Assert.assertNotNull(index);
        Assert.assertTrue(index.containsKey("expireAfterSeconds"));
        Assert.assertEquals(5, ((Number) index.get("expireAfterSeconds")).intValue());
    }

    @Test
    public void testIndexedField() {
        getMapper().map(HasExpiryField.class);
        getDatastore().ensureIndexes();

        getDatastore().save(new HasExpiryField());

        final MongoDatabase db = getDatastore().getDatabase();
        final MongoCollection<Document> dbCollection = db.getCollection("HasExpiryField");
        final ListIndexesIterable<Document> indexes = dbCollection.listIndexes();

        Assert.assertNotNull(indexes);
        Assert.assertEquals(2, count(indexes.iterator()));
        Document index = null;
        for (final Document candidateIndex : indexes) {
            if (candidateIndex.containsKey("expireAfterSeconds")) {
                index = candidateIndex;
            }
        }
        Assert.assertNotNull(index);
        Assert.assertEquals(5, ((Number) index.get("expireAfterSeconds")).intValue());
    }

    @Entity
    public static class HasExpiryField {
        @Indexed(options = @IndexOptions(expireAfterSeconds = 5))
        private final Date offerExpiresAt = new Date();
        @Id
        private ObjectId id;
    }

    @Entity
    @Indexes(@Index(fields = @Field("offerExpiresAt"), options = @IndexOptions(expireAfterSeconds = 5)))
    public static class ClassAnnotation {
        private final Date offerExpiresAt = new Date();
        @Id
        private ObjectId id;
    }
}

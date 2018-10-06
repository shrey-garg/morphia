package xyz.morphia;


import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import xyz.morphia.annotations.Entity;
import xyz.morphia.annotations.Field;
import xyz.morphia.annotations.Id;
import xyz.morphia.annotations.Index;
import xyz.morphia.annotations.IndexOptions;
import xyz.morphia.annotations.Indexed;
import xyz.morphia.annotations.Indexes;

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

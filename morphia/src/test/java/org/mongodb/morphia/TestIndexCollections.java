package org.mongodb.morphia;


import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Field;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Property;

import static org.mongodb.morphia.utils.IndexType.DESC;


public class TestIndexCollections extends TestBase {

    @Test
    @Ignore("Until the question of embedded indexes is settled")
    public void testEmbedded() {
        AdvancedDatastore ads = getAds();
        MongoDatabase db = getDatabase();
        getMorphia().map(HasEmbeddedIndex.class);
        ads.ensureIndexes();

//        ads.ensureIndexes("b_2", HasEmbeddedIndex.class);
        Document[] indexes = new Document[]{
            new Document("name", 1),
            new Document("embeddedIndex.color", -1),
            new Document("embeddedIndex.name", 1),
        };

        testIndex(db.getCollection("b_2").listIndexes(), indexes);
        testIndex(ads.getCollection(HasEmbeddedIndex.class).listIndexes(), indexes);
    }

    private void testIndex(final ListIndexesIterable<Document> indexInfo, final Document... indexes) {
        MongoCursor<Document> iterator = indexInfo.iterator();
        while (iterator.hasNext()) {
            final Document document = iterator.next();
            if (document.get("name").equals("_id_")) {
                iterator.remove();
            } else {
                for (final Document index : indexes) {
                    final Document key = (Document) document.get("key");
                    if (key.equals(index)) {
                        iterator.remove();
                    }
                }
            }
        }
        Assert.assertFalse("Should have found all the indexes.  Remaining: " + indexInfo, indexInfo.iterator().hasNext());
    }

    @Entity
    @Indexes({@Index(fields = @Field(value = "field2", type = DESC)), @Index(fields = @Field("field3"))})
    private static class SingleFieldIndex {
        @Id
        private ObjectId id;
        @Indexed
        private String field;
        @Property
        private String field2;
        @Property("f3")
        private String field3;
    }

    @Entity
    @Indexes({@Index(fields = @Field(value = "field2", type = DESC)), @Index(fields = @Field("field3"))})
    private static class OldStyleIndexing {
        @Id
        private ObjectId id;
        @Indexed
        private String field;
        @Property
        private String field2;
        @Property("f3")
        private String field3;
    }

    @Entity
    private static class HasEmbeddedIndex {
        @Id
        private ObjectId id;
        @Indexed
        private String name;
        private EmbeddedIndex embeddedIndex;
    }

    @Embedded
    @Indexes(@Index(fields = @Field(value = "color", type = DESC)))
    private static class EmbeddedIndex {
        @Indexed
        private String name;
        private String color;
    }
}

package xyz.morphia.indexes;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ListIndexesIterable;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Entity;
import xyz.morphia.annotations.Field;
import xyz.morphia.annotations.Id;
import xyz.morphia.annotations.Index;
import xyz.morphia.annotations.IndexOptions;
import xyz.morphia.annotations.Indexes;
import xyz.morphia.annotations.Property;
import xyz.morphia.annotations.Text;

import static xyz.morphia.utils.IndexType.TEXT;

@SuppressWarnings("unused")
public class TestTextIndexing extends TestBase {
    @Test(expected = MongoCommandException.class)
    public void shouldNotAllowMultipleTextIndexes() {
        Class<MultipleTextIndexes> clazz = MultipleTextIndexes.class;
        getMapper().map(clazz);
        getDatastore().getCollection(clazz).drop();
        getDatastore().ensureIndexes();
    }

    @Test
    public void testSingleAnnotation() {
        getMapper().map(CompoundTextIndex.class);
        getDatastore().getCollection(CompoundTextIndex.class).drop();
        getDatastore().ensureIndexes();

        ListIndexesIterable<Document> indexInfo = getDatastore().getCollection(CompoundTextIndex.class).listIndexes();
        Assert.assertEquals(2, count(indexInfo.iterator()));
        boolean found = false;
        for (Document document : indexInfo) {
            if (document.get("name").equals("indexing_test")) {
                found = true;
                Assert.assertEquals(document.toString(), "russian", document.get("default_language"));
                Assert.assertEquals(document.toString(), "nativeTongue", document.get("language_override"));
                Assert.assertEquals(document.toString(), 1, ((Document) document.get("weights")).get("name"));
                Assert.assertEquals(document.toString(), 10, ((Document) document.get("weights")).get("nick"));
                Assert.assertEquals(document.toString(), 1, ((Document) document.get("key")).get("age"));
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testTextAnnotation() {
        Class<SingleFieldTextIndex> clazz = SingleFieldTextIndex.class;

        getMapper().map(clazz);
        getDatastore().getCollection(clazz).drop();
        getDatastore().ensureIndexes();

        ListIndexesIterable<Document> indexInfo = getDatastore().getCollection(clazz).listIndexes();
        Assert.assertEquals(2, count(indexInfo.iterator()));
        boolean found = false;
        for (Document document : indexInfo) {
            if (document.get("name").equals("single_annotation")) {
                found = true;
                Assert.assertEquals(document.toString(), "english", document.get("default_language"));
                Assert.assertEquals(document.toString(), "nativeTongue", document.get("language_override"));
                Assert.assertEquals(document.toString(), 10, ((Document) document.get("weights")).get("nickName"));
            }
        }
        Assert.assertTrue(indexInfo.toString(), found);

    }

    @Test
    public void testTextIndexOnNamedCollection() {
        getMapper().map(TextIndexAll.class);
        getAds().ensureIndexes("randomCollection", TextIndexAll.class, false);

        ListIndexesIterable<Document> indexInfo = getDatabase().getCollection("randomCollection").listIndexes();
        Assert.assertEquals(2, count(indexInfo.iterator()));
        for (Document document : indexInfo) {
            if (!document.get("name").equals("_id_")) {
                Assert.assertEquals(1, ((Document) document.get("weights")).get("$**"));
                Assert.assertEquals("english", document.get("default_language"));
                Assert.assertEquals("language", document.get("language_override"));
            }
        }
    }

    @Entity
    @Indexes(@Index(fields = @Field(value = "$**", type = TEXT)))
    private static class TextIndexAll {
        @Id
        private ObjectId id;
        private String name;
        private String nickName;
    }

    @Entity
    @Indexes(@Index(fields = {@Field(value = "name", type = TEXT),
                              @Field(value = "nick", type = TEXT, weight = 10),
                              @Field(value = "age")}, options = @IndexOptions(name = "indexing_test", language = "russian",
                                                                                                     languageOverride = "nativeTongue")))
    private static class CompoundTextIndex {
        @Id
        private ObjectId id;
        private String name;
        private Integer age;
        @Property("nick")
        private String nickName;
        private String nativeTongue;
    }

    @Entity
    private static class SingleFieldTextIndex {
        @Id
        private ObjectId id;
        private String name;
        @Text(value = 10, options = @IndexOptions(name = "single_annotation", languageOverride = "nativeTongue"))
        private String nickName;

    }

    @Entity
    @Indexes({@Index(fields = @Field(value = "name", type = TEXT)),
              @Index(fields = @Field(value = "nickName", type = TEXT))})
    private static class MultipleTextIndexes {
        @Id
        private ObjectId id;
        private String name;
        private String nickName;
    }
}

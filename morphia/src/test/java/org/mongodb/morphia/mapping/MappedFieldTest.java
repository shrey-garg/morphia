package org.mongodb.morphia.mapping;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import java.util.List;
import java.util.Map;

public class MappedFieldTest extends TestBase {
    @Test
    public void arrayFieldMapping() {
        final MappedField field = getMappedField(TestEntity.class, "arrayOfInt");

        Assert.assertFalse(field.isSingleValue());
        Assert.assertTrue(field.isMultipleValues());
        Assert.assertTrue(field.isArray());
        Assert.assertTrue(field.getType().isArray());
        Assert.assertEquals("arrayOfInt", field.getJavaFieldName());
        Assert.assertEquals("arrayOfInt", field.getNameToStore());
    }

    private MappedField getMappedField(final Class<?> type, final String name) {
        return getMorphia().getMapper().getMappedClass(type).getMappedField(name);
    }

    @Test
    public void basicFieldMapping() {
        final MappedField field = getMappedField(TestEntity.class, "name");

        Assert.assertTrue(field.isSingleValue());
        Assert.assertTrue(String.class == field.getType());
        Assert.assertEquals("name", field.getJavaFieldName());
        Assert.assertEquals("n", field.getNameToStore());
    }

    @Test
    public void collectionFieldMapping() {
        final MappedField field = getMappedField(TestEntity.class, "listOfString");

        Assert.assertFalse(field.isSingleValue());
        Assert.assertTrue(field.isMultipleValues());
        Assert.assertFalse(field.isArray());
        Assert.assertTrue(List.class == field.getType());
        Assert.assertTrue(String.class == field.getSpecializedType());
        Assert.assertEquals("listOfString", field.getJavaFieldName());
        Assert.assertEquals("listOfString", field.getNameToStore());
    }

    @Test
    public void idFieldMapping() {
        final MappedField field = getMappedField(TestEntity.class, "id");

        Assert.assertTrue(field.isSingleValue());
        Assert.assertTrue(ObjectId.class == field.getType());
        Assert.assertEquals("id", field.getJavaFieldName());
        Assert.assertEquals("_id", field.getNameToStore());
    }

    @Entity
    private static class TestEntity {

        @Id
        private ObjectId id;
        @Property("n")
        private String name;
        private List<String> listOfString;
        private List<List<String>> listOfListOfString;
        private int[] arrayOfInt;
        private Map<String, Integer> mapOfInts;
        private List<Embed> listOfEmbeds;
    }

    @Embedded
    private static class Embed {

        private String embedName;
        private List<Embed> embeddeds;
    }
}

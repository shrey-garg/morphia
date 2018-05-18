package org.mongodb.morphia.issue463;


import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;


public class TestIssue463 extends TestBase {
    @Test
    public void save() {
        getMorphia().map(Class1.class, Class2.class);

        final Class2 class2 = new Class2();
        class2.setId(new ObjectId());
        class2.setText("hello world");
        getDatastore().save(class2);

        Assert.assertNull(getDatastore().find(Class1.class).filter("_id", class2.getId()).get());
        Assert.assertNotNull(getDatastore().find(Class2.class).filter("_id", class2.getId()).get());
    }

    @Entity(value = "class1", noClassnameStored = true)
    public static class Class1 {
        @Id
        private ObjectId id;
        private String text;

        public ObjectId getId() {
            return id;
        }

        public void setId(final ObjectId id) {
            this.id = id;
        }

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }
    }

    @Entity(value = "class2", noClassnameStored = true)
    public static class Class2 extends Class1 {

    }
}

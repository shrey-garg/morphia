package org.mongodb.morphia.mapping.validation.fieldrules;


import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.PrePersist;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Transient;
import org.mongodb.morphia.mapping.validation.ConstraintViolationException;
import org.mongodb.morphia.testutil.TestEntity;

public class PropertyAndEmbeddedTest extends TestBase {
    @Test(expected = ConstraintViolationException.class)
    public void testCheck() {

        final E e = new E();
        getDatastore().save(e);

        Assert.assertTrue(e.document.contains("myFunkyR"));

        getMorphia().map(E2.class);
    }

    public static class E extends TestEntity {
        @Property("myFunkyR")
        private R r = new R();
        @Transient
        private String document;

        @PrePersist
        public void prePersist(final Document o) {
            document = o.toString();
        }
    }

    @Embedded
    public static class E2 extends TestEntity {
        @Property("myFunkyR")
        private String s;
    }

    @Embedded
    public static class R {
        private String foo = "bar";
    }
}

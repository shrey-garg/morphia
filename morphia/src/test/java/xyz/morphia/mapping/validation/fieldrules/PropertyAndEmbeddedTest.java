package xyz.morphia.mapping.validation.fieldrules;


import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Embedded;
import xyz.morphia.annotations.PrePersist;
import xyz.morphia.annotations.Property;
import xyz.morphia.annotations.Transient;
import xyz.morphia.mapping.validation.ConstraintViolationException;
import xyz.morphia.testutil.TestEntity;

public class PropertyAndEmbeddedTest extends TestBase {
    @Test(expected = ConstraintViolationException.class)
    public void testCheck() {

        final E e = new E();
        getDatastore().save(e);

        Assert.assertTrue(e.document.contains("myFunkyR"));

        getMapper().map(E2.class);
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

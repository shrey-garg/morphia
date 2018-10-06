package xyz.morphia.mapping.validation.fieldrules;


import org.bson.Document;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Serialized;
import xyz.morphia.annotations.Transient;
import xyz.morphia.testutil.TestEntity;

@Ignore("@Serialized might be removed altogether")
public class SerializedNameTest extends TestBase {
    @Test
    public void testCheck() {

        final E e = new E();
        getDatastore().save(e);

        Assert.assertTrue(e.document.contains("changedName"));
    }

    public static class E extends TestEntity {
        @Serialized("changedName")
        private final byte[] b = "foo".getBytes();
        @Transient
        private String document;

        public void preSave(final Document o) {
            document = o.toString();
        }
    }
}

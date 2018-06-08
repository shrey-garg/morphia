package org.mongodb.morphia.issue47;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.testutil.TestEntity;


public class TestEmbedLoop extends TestBase {

    @Test
    @Ignore
    public void testCircularRefs() {

        getMapper().map(A.class);

        A a = new A();
        a.b = new B();
        a.b.a = a;

        Assert.assertSame(a, a.b.a);

        getDatastore().save(a);
        a = getDatastore().find(A.class).filter("_id", a.getId()).get();
        Assert.assertSame(a, a.b.a);
    }

    @Entity
    static class A extends TestEntity {
        private B b;
    }

    @Embedded
    static class B extends TestEntity {
        private String someProperty = "someThing";

        // produces stack overflow, might be detectable?
        // @Reference this would be right way to do it.

        private A a;
    }
}

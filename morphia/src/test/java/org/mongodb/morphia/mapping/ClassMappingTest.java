package org.mongodb.morphia.mapping;


import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

public class ClassMappingTest extends TestBase {

    @Test
    public void testClassQueries() {
        E e = new E();

        e.testClass2 = LinkedList.class;
        getDatastore().save(e);

        Assert.assertNull(getDatastore().find(E.class).field("testClass2").equal(ArrayList.class).get());
    }

    @Test
    public void testMapping() throws Exception {
        E e = new E();

        e.testClass = LinkedList.class;
        getDatastore().save(e);

        e = getDatastore().get(e);
        Assert.assertEquals(LinkedList.class, e.testClass);
    }

    @Test
    public void testMappingWithoutAnnotation() throws Exception {
        E e = new E();

        e.testClass2 = LinkedList.class;
        getDatastore().save(e);

        e = getDatastore().get(e);
        Assert.assertEquals(LinkedList.class, e.testClass2);
    }

    public static class E {
        @Id
        private ObjectId id;

        @Property
        private Class<? extends Collection> testClass;
        private Class<? extends Collection> testClass2;
    }
}

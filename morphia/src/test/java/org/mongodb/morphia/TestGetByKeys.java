package org.mongodb.morphia;


import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.testutil.TestEntity;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;

public class TestGetByKeys extends TestBase {
    @Test
    public final void testGetByKeys() {
        final A a1 = new A();
        final A a2 = new A();

        final List<Key<A>> keys = getDatastore().saveMany(asList(a1, a2));

        final List<A> reloaded = getDatastore().getByKeys(keys);

        final Iterator<A> i = reloaded.iterator();
        Assert.assertNotNull(i.next());
        Assert.assertNotNull(i.next());
        Assert.assertFalse(i.hasNext());
    }

    private static class A extends TestEntity {
        private String foo = "bar";
    }

}

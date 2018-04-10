package org.mongodb.morphia.query;


import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.TestMapping.BaseEntity;
import org.mongodb.morphia.annotations.Entity;

import static java.util.Arrays.asList;


public class TestStringPatternQueries extends TestBase {
    @Test
    public void testContains() {

        getDatastore().saveMany(asList(new E("xBA"), new E("xa"), new E("xAb"), new E("xab"), new E("xcB"), new E("aba")));

        Assert.assertEquals(3, getDatastore().find(E.class).field("name").contains("b").count());
        Assert.assertEquals(5, getDatastore().find(E.class).field("name").containsIgnoreCase("b").count());
    }

    @Test
    public void testEndsWith() {

        getDatastore().saveMany(asList(new E("bxA"), new E("xba"), new E("xAb"), new E("xab"), new E("xcB"), new E("aba")));

        Assert.assertEquals(2, getDatastore().find(E.class).field("name").endsWith("b").count());
        Assert.assertEquals(3, getDatastore().find(E.class).field("name").endsWithIgnoreCase("b").count());
    }

    @Test
    public void testStartsWith() {

        getDatastore().saveMany(asList(new E("A"), new E("a"), new E("Ab"), new E("ab"), new E("c")));

        Assert.assertEquals(2, getDatastore().find(E.class).field("name").startsWith("a").count());
        Assert.assertEquals(4, getDatastore().find(E.class).field("name").startsWithIgnoreCase("a").count());
        Assert.assertEquals(4, getDatastore().find(E.class).field("name").startsWithIgnoreCase("A").count());
    }

    @Entity
    static class E extends BaseEntity {
        private final String name;

        E(final String name) {
            this.name = name;
        }

        protected E() {
            name = null;
        }
    }

}

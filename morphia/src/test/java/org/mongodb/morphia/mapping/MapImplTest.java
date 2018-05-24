package org.mongodb.morphia.mapping;


import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Id;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MapImplTest extends TestBase {

    @Test
    public void testEmbeddedMap() {
        getMorphia().map(ContainsMapOfEmbeddedGoos.class, ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");
        final ContainsMapOfEmbeddedGoos cmoeg = new ContainsMapOfEmbeddedGoos();
        cmoeg.values.put("first", g1);
        getDatastore().save(cmoeg);
        //check className in the map values.

        final MongoCollection<ContainsMapOfEmbeddedGoos> collection = getDatastore().getCollection(
            ContainsMapOfEmbeddedGoos.class);
        final MongoCollection<Document> docCollection = getDatabase().getCollection(collection.getNamespace().getCollectionName());

        final Document next = docCollection
                                  .find()
                                  .limit(1)
                                  .iterator().next();
        final Document values = (Document) next
                                               .get("values");
        final Document goo = (Document) values
                                            .get("first");
        assertFalse(goo.containsKey(Mapper.CLASS_NAME_FIELDNAME));
    }

    @Test //@Ignore("waiting on issue 184")
    public void testEmbeddedMapUpdateOperations() {
        getMorphia().map(ContainsMapOfEmbeddedGoos.class, ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");
        final Goo g2 = new Goo("Ralph");

        final ContainsMapOfEmbeddedGoos cmoeg = new ContainsMapOfEmbeddedGoos();
        cmoeg.values.put("first", g1);
        getDatastore().save(cmoeg);
        getDatastore().update(cmoeg, getDatastore().createUpdateOperations(ContainsMapOfEmbeddedGoos.class).set("values.second", g2));
        //check className in the map values.

        final MongoCollection<ContainsMapOfEmbeddedGoos> collection = getDatastore().getCollection(
            ContainsMapOfEmbeddedGoos.class);
        final MongoCollection<Document> docCollection = getDatabase().getCollection(collection.getNamespace().getCollectionName());

        final Document goo = (Document) ((Document) docCollection
                                                        .find()
                                                        .limit(1)
                                                        .iterator().next()
                                                        .get("values"))
                                            .get("second");
        assertFalse("className should not be here.", goo.containsKey(Mapper.CLASS_NAME_FIELDNAME));
    }

    @Test
    public void testEmbeddedMapUpdateOperationsOnInterfaceValue() {
        getMorphia().map(ContainsMapOfEmbeddedGoos.class, ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");
        final Goo g2 = new Goo("Ralph");

        final ContainsMapOfEmbeddedInterfaces cmoei = new ContainsMapOfEmbeddedInterfaces();
        cmoei.values.put("first", g1);
        getDatastore().save(cmoei);
        getDatastore().update(cmoei, getDatastore().createUpdateOperations(ContainsMapOfEmbeddedInterfaces.class).set("values.second", g2));
        //check className in the map values.
        final MongoCollection<ContainsMapOfEmbeddedInterfaces> collection = getDatastore().getCollection(
            ContainsMapOfEmbeddedInterfaces.class);
        final MongoCollection<Document> docCollection = getDatabase().getCollection(collection.getNamespace().getCollectionName());

        final Document goo = (Document) ((Document) docCollection
                                                        .find()
                                                        .limit(1)
                                                        .iterator().next()
                                                        .get("values"))
                                            .get("second");
        assertTrue("className should be here.", goo.containsKey(Mapper.CLASS_NAME_FIELDNAME));
    }

    @Test
    public void testEmbeddedMapWithValueInterface() {
        getMorphia().map(ContainsMapOfEmbeddedGoos.class, ContainsMapOfEmbeddedInterfaces.class);
        final Goo g1 = new Goo("Scott");

        final ContainsMapOfEmbeddedInterfaces cmoei = new ContainsMapOfEmbeddedInterfaces();
        cmoei.values.put("first", g1);
        getDatastore().save(cmoei);
        //check className in the map values.
        final MongoCollection<ContainsMapOfEmbeddedInterfaces> collection = getDatastore().getCollection(
            ContainsMapOfEmbeddedInterfaces.class);
        final MongoCollection<Document> docCollection = getDatabase().getCollection(collection.getNamespace().getCollectionName());

        final Document goo = (Document) ((Document) docCollection
                                                        .find()
                                                        .limit(1)
                                                        .iterator().next()
                                                        .get("values"))
                                            .get("first");
        final boolean hasF = goo.containsKey(Mapper.CLASS_NAME_FIELDNAME);
        assertTrue(hasF);
    }

    @Test
    public void testMapping() {
        E e = new E();
        e.mymap.put("1", "a");
        e.mymap.put("2", "b");

        getDatastore().save(e);

        e = getDatastore().get(e);
        Assert.assertEquals("a", e.mymap.get("1"));
        Assert.assertEquals("b", e.mymap.get("2"));
    }

    private static class ContainsMapOfEmbeddedInterfaces {
        @Id
        private ObjectId id;
        private final Map<String, Object> values = new HashMap<>();
    }

    private static class ContainsMapOfEmbeddedGoos {
        @Id
        private ObjectId id;
        private final Map<String, Goo> values = new HashMap<>();
    }

    @Embedded(useDiscriminator = false)
    private static class Goo {
        private String name;

        Goo() {
        }

        Goo(final String n) {
            name = n;
        }
    }

    private static class E {
        @Id
        private ObjectId id;
        private final MyMap mymap = new MyMap();
    }

    @Embedded
    private static class MyMap extends HashMap<String, String> {
    }
}

package org.mongodb.morphia.generics;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.testutil.TestEntity;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestJavaMaps extends TestBase {
    @Test
    public void mapperTest() {
        getMorphia().map(Employee.class);

        for (boolean nulls : new boolean[]{true, false}) {
            for (boolean empties : new boolean[]{true, false}) {
                getMapper().getOptions().setStoreNulls(nulls);
                getMapper().getOptions().setStoreEmpties(empties);
                empties();
            }
        }
    }

    private void empties() {
        Datastore ds = getDatastore();
        ds.deleteMany(ds.find(Employee.class));
        Employee employee = new Employee();
        HashMap<String, Byte> byteMap = new HashMap<>();
        byteMap.put("b", (byte) 1);
        employee.byteMap = byteMap;
        ds.save(employee);

        Employee loaded = ds.find(Employee.class).get();

        assertEquals(Byte.valueOf((byte) 1), loaded.byteMap.get("b"));
        assertNull(loaded.floatMap);
    }

    @Test
    public void emptyModel() {
        getMapper().getOptions().setStoreEmpties(true);
        getMapper().getOptions().setStoreNulls(false);

        TestEmptyModel model = new TestEmptyModel();
        model.text = "text";
        model.wrapped = new TestEmptyModel.Wrapped();
        model.wrapped.text = "textWrapper";
        getDatastore().save(model);
        TestEmptyModel model2 = getDatastore().find(TestEmptyModel.class).filter("id", model.id).get();
        Assert.assertNull(model.wrapped.others);
        Assert.assertNull(model2.wrapped.others);
    }

    @Test
    public void testKeyOrdering() {
        getMorphia().map(LinkedHashMapTestEntity.class);
        final LinkedHashMapTestEntity expectedEntity = new LinkedHashMapTestEntity();
        for (int i = 100; i >= 0; i--) {
            expectedEntity.getLinkedHashMap().put(i, "a" + i);
        }
        getDatastore().save(expectedEntity);
        LinkedHashMapTestEntity storedEntity = getDatastore().find(LinkedHashMapTestEntity.class).get();
        Assert.assertNotNull(storedEntity);
        Assert.assertEquals(expectedEntity.getLinkedHashMap(), storedEntity.getLinkedHashMap());
    }

    @Entity
    static class TestEmptyModel{
        @Id
        private ObjectId id;
        private String text;
        private Wrapped wrapped;

        private static class Wrapped {
            private Map<String, Wrapped> others;
            private String text;
        }
    }

    @Entity("employees")
    static class Employee {
        @Id
        private ObjectId id;

        private Map<String, Float> floatMap;
        private Map<String, Byte> byteMap;
    }

    @Entity
    static class LinkedHashMapTestEntity extends TestEntity {

        @Property(concreteClass = java.util.LinkedHashMap.class)
        private final Map<Integer, String> linkedHashMap = new LinkedHashMap<>();
        private Map<Integer, String> getLinkedHashMap() {
            return linkedHashMap;
        }

    }

}

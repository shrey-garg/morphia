package xyz.morphia.mapping.validation.classrules;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Serialized;
import xyz.morphia.testutil.TestEntity;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
@Ignore("@Serialized might be removed altogether")
public class SerializedMapTest extends TestBase {

    @Test
    public void testSerialization() {
        Map1 map1 = new Map1();
        map1.shouldBeOk.put(3, new Foo("peter"));
        map1.shouldBeOk.put(27, new Foo("paul"));

        getDatastore().save(map1);
        map1 = getDatastore().get(map1);

        Assert.assertEquals("peter", map1.shouldBeOk.get(3).id);
        Assert.assertEquals("paul", map1.shouldBeOk.get(27).id);

    }

    @Test
    public void testSerialization2() {
        Map2 map2 = new Map2();
        map2.shouldBeOk.put(3, new Foo("peter"));
        map2.shouldBeOk.put(27, new Foo("paul"));

        getDatastore().save(map2);
        map2 = getDatastore().get(map2);

        Assert.assertEquals("peter", map2.shouldBeOk.get(3).id);
        Assert.assertEquals("paul", map2.shouldBeOk.get(27).id);

    }

    private static class Map1 extends TestEntity {
        @Serialized()
        private final Map<Integer, Foo> shouldBeOk = new HashMap();

    }

    private static class Map2 extends TestEntity {
        @Serialized(disableCompression = true)
        private final Map<Integer, Foo> shouldBeOk = new HashMap();

    }

    static class Foo {

        private final String id;

        Foo(final String id) {
            this.id = id;
        }
    }
}

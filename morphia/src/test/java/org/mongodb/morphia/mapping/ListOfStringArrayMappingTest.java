package org.mongodb.morphia.mapping;


import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;

import java.util.ArrayList;
import java.util.List;

public class ListOfStringArrayMappingTest extends TestBase {
    @Test
    public void testMapping() {
        getMorphia().map(ContainsListStringArray.class);
        final ContainsListStringArray ent = new ContainsListStringArray();
        ent.listOfStrings.add(new String[]{"a", "b"});
        ent.arrayOfStrings = new String[]{"only", "the", "lonely"};
        ent.string = "raw string";

        getDatastore().save(ent);
        final ContainsListStringArray loaded = getDatastore().get(ent);
        Assert.assertNotNull(loaded.id);
        Assert.assertArrayEquals(ent.listOfStrings.get(0), loaded.listOfStrings.get(0));
        Assert.assertArrayEquals(ent.arrayOfStrings, loaded.arrayOfStrings);
        Assert.assertEquals(ent.string, loaded.string);
    }

    private static class ContainsListStringArray {
        private final List<String[]> listOfStrings = new ArrayList<>();
        @Id
        private ObjectId id;
        private String[] arrayOfStrings;
        private String string;
    }
}

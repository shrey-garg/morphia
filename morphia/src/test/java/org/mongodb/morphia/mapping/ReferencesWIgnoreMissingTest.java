package org.mongodb.morphia.mapping;


import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Reference;

import java.util.List;

@Ignore("References are not currently supported")
public class ReferencesWIgnoreMissingTest extends TestBase {
    @Test
    public void testMissingReference() {
        final Container c = new Container();
        c.refs = new StringHolder[]{new StringHolder(), new StringHolder()};
        getDatastore().save(c);
        getDatastore().save(c.refs[0]);

        Container reloadedContainer = getDatastore().find(Container.class).get();
        Assert.assertNotNull(reloadedContainer);
        Assert.assertNotNull(reloadedContainer.refs);
        Assert.assertEquals(1, reloadedContainer.refs.length);

        reloadedContainer = getDatastore().get(c);
        Assert.assertNotNull(reloadedContainer);
        Assert.assertNotNull(reloadedContainer.refs);
        Assert.assertEquals(1, reloadedContainer.refs.length);

        final List<Container> cs = getDatastore().find(Container.class).asList();
        Assert.assertNotNull(cs);
        Assert.assertEquals(1, cs.size());

    }

    @Entity
    static class Container {
        @Id
        private ObjectId id;
        @Reference(ignoreMissing = true)
        private StringHolder[] refs;
    }

    @Entity
    static class StringHolder {
        @Id
        private ObjectId id = new ObjectId();
    }
}

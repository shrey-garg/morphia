package org.mongodb.morphia.query;


import org.junit.Test;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.TestMapping.MissingId;


public class TestMandatoryId extends TestBase {
    @Test(expected = ValidationException.class)
    public final void testMissingIdNoImplicitMapCall() {
        final Key<MissingId> save = getDatastore().save(new MissingId());

        getDatastore().getByKey(MissingId.class, save);
    }
}

package org.mongodb.morphia.query;


import org.junit.Test;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.TestMapping.MissingId;
import org.mongodb.morphia.mapping.validation.ConstraintViolationException;


public class TestMandatoryId extends TestBase {
    @Test(expected = ConstraintViolationException.class)
    public final void testMissingIdNoImplicitMapCall() {
        final Key<MissingId> save = getDatastore().save(new MissingId());

        getDatastore().getByKey(MissingId.class, save);
    }
}

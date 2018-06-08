package org.mongodb.morphia.mapping.validation.classrules;


import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Version;
import org.mongodb.morphia.mapping.validation.ConstraintViolationException;
import org.mongodb.morphia.testutil.TestEntity;

public class MultipleVersionsTest extends TestBase {

    @Test(expected = ConstraintViolationException.class)
    public void testCheck() {
        getMapper().map(OK1.class);
        getMapper().map(Fail1.class);
    }

    public static class Fail1 extends TestEntity {
        @Version
        private long v1;
        @Version
        private long v2;
    }

    public static class OK1 extends TestEntity {
        @Version
        private long v1;
    }
}

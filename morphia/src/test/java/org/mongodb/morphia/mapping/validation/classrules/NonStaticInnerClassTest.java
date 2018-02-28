package org.mongodb.morphia.mapping.validation.classrules;


import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.mapping.MappingException;

public class NonStaticInnerClassTest extends TestBase {

    @Test(expected = MappingException.class)
    public void testInValidInnerClass() {
        getMorphia().map(InValid.class);
    }

    @Test
    public void testValidInnerClass() {
        getMorphia().map(Valid.class);
    }

    static class Valid {
        @Id
        private ObjectId id;
    }

    class InValid {
        @Id
        private ObjectId id;
    }
}

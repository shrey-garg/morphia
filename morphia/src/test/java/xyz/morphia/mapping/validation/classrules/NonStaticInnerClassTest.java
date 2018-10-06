package xyz.morphia.mapping.validation.classrules;


import org.bson.types.ObjectId;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Id;
import xyz.morphia.mapping.MappingException;

public class NonStaticInnerClassTest extends TestBase {

    @Test(expected = MappingException.class)
    public void testInValidInnerClass() {
        getMapper().map(InValid.class);
    }

    @Test
    public void testValidInnerClass() {
        getMapper().map(Valid.class);
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

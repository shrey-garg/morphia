package xyz.morphia.mapping.validation.classrules;


import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.types.ObjectId;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Entity;
import xyz.morphia.annotations.Id;
import xyz.morphia.annotations.Property;
import xyz.morphia.mapping.validation.ConstraintViolationException;

import java.util.Map;

public class DuplicatePropertyNameTest extends TestBase {
    @Test(expected = CodecConfigurationException.class)
    public void testDuplicatedPropertyNameDifferentType() {
        getMapper().map(DuplicatedPropertyName2.class);
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDuplicatedPropertyNameSameType() {
        getMapper().map(DuplicatedPropertyName.class);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testDuplicatedPropertyNameShadowedFields() {
        getMapper().map(Extends.class);
    }

    @Entity
    public static class DuplicatedPropertyName {
        @Id
        private String id;

        @Property(value = "value")
        private String content1;
        @Property(value = "value")
        private String content2;
    }

    @Entity
    public static class DuplicatedPropertyName2 {
        @Id
        private String id;

        @Property(value = "value")
        private Map<String, Integer> content1;
        @Property(value = "value")
        private String content2;
    }

    @Entity
    public static class Super {
        @Id
        private ObjectId id;

        private String foo;
    }

    public static class Extends extends Super {
        private String foo;
    }

}

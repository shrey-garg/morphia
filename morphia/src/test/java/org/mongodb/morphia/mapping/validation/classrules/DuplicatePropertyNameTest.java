package org.mongodb.morphia.mapping.validation.classrules;


import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import java.util.Map;

public class DuplicatePropertyNameTest extends TestBase {
    @Test(expected = CodecConfigurationException.class)
    public void testDuplicatedPropertyNameDifferentType() {
        getMorphia().map(DuplicatedPropertyName2.class);
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDuplicatedPropertyNameSameType() {
        getMorphia().map(DuplicatedPropertyName.class);
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDuplicatedPropertyNameShadowedFields() {
        getMorphia().map(Extends.class);
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
        private String foo;
    }

    public static class Extends extends Super {
        private String foo;
    }

}

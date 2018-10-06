package xyz.morphia.issue50;

import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Id;
import xyz.morphia.testutil.TestEntity;

public class TestIdTwice extends TestBase {

    @Test(expected = CodecConfigurationException.class)
    public final void shouldThrowExceptionIfThereIsMoreThanOneId() {
        getMapper().map(A.class);
    }

    public static class A extends TestEntity {
        @Id
        private String extraId;
        @Id
        private String broken;
    }

}

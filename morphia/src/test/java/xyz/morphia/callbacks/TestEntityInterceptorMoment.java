package xyz.morphia.callbacks;


import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import xyz.morphia.EntityInterceptor;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Id;
import xyz.morphia.annotations.PrePersist;
import xyz.morphia.mapping.Mapper;

public class TestEntityInterceptorMoment extends TestBase {

    @Test
    public void testGlobalEntityInterceptorWorksAfterEntityCallback() {
        getMapper().map(E.class);
        getMapper().addInterceptor(new Interceptor());

        getDatastore().save(new E());
    }

    static class E {
        @Id
        private final ObjectId id = new ObjectId();

        private boolean called;

        @PrePersist
        void entityCallback() {
            called = true;
        }
    }

    public static class Interceptor implements EntityInterceptor {
        @Override
        public void prePersist(final Object ent, final Document document, final Mapper mapper) {
            Assert.assertTrue(((E) ent).called);
        }
    }
}

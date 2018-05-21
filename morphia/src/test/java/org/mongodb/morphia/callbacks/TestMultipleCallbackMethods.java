package org.mongodb.morphia.callbacks;


import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.PostLoad;
import org.mongodb.morphia.annotations.PostPersist;
import org.mongodb.morphia.annotations.PreLoad;
import org.mongodb.morphia.annotations.PrePersist;


public class TestMultipleCallbackMethods extends TestBase {
    private static int load;

    @Test
    public void testMultipleCallbackAnnotation() {
        final SomeEntity entity = new SomeEntity();
        getDatastore().save(entity);

        Assert.assertEquals(4, entity.getPersist());
        Assert.assertEquals(0, load);

        final SomeEntity someEntity = getDatastore()
                                          .find(SomeEntity.class)
                                          .filter("_id", entity.getId())
                                          .get();

        Assert.assertEquals(4, entity.getPersist());

        Assert.assertEquals(-3, someEntity.getPersist());
        Assert.assertEquals(2, load);
    }

    abstract static class CallbackAbstractEntity {
        @Id
        private final ObjectId id = new ObjectId();
        private int persist;

        public ObjectId getId() {
            return id;
        }

        int getPersist() {
            return persist;
        }

        void setPersist(final int persist) {
            this.persist = persist;
        }

        @PrePersist
        void prePersist1() {
            persist++;
        }

        @PrePersist
        void prePersist2() {
            persist++;
        }

        @PostPersist
        void postPersist1() {
            persist++;
        }

        @PostPersist
        void postPersist2() {
            persist++;
        }

        @PreLoad
        void preLoad1() {
            load++;
        }

        @PreLoad
        void preLoad2() {
            load++;
        }

        @PostLoad
        void postLoad1() {
            persist--;
        }

        @PostLoad
        void postLoad2() {
            persist--;
        }

        @PostLoad
        void postLoad3() {
            persist--;
        }
    }

    static class SomeEntity extends CallbackAbstractEntity {

    }
}

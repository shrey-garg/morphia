package org.mongodb.morphia;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.morphia.annotations.CappedAt;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCapped extends TestBase {
    @Test
    public void testCappedEntity() {
        // given
        getMorphia().map(CurrentStatus.class);
        getDatastore().ensureCaps();

        // when-then
        final CurrentStatus cs = new CurrentStatus("All Good");
        getDatastore().save(cs);
        assertEquals(1, getDatastore().getCount(CurrentStatus.class));
        getDatastore().save(new CurrentStatus("Kinda Bad"));
        assertEquals(1, getDatastore().getCount(CurrentStatus.class));
        assertTrue(getDatastore().find(CurrentStatus.class).get().message.contains("Bad"));
        getDatastore().save(new CurrentStatus("Kinda Bad2"));
        assertEquals(1, getDatastore().getCount(CurrentStatus.class));
        getDatastore().save(new CurrentStatus("Kinda Bad3"));
        assertEquals(1, getDatastore().getCount(CurrentStatus.class));
        getDatastore().save(new CurrentStatus("Kinda Bad4"));
        assertEquals(1, getDatastore().getCount(CurrentStatus.class));
    }

    @Entity(cap = @CappedAt(count = 1))
    private static class CurrentStatus {
        @Id
        private ObjectId id;
        private String message;

        private CurrentStatus() {
        }

        CurrentStatus(final String msg) {
            message = msg;
        }
    }

}

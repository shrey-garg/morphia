package org.mongodb.morphia.query;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.TestBase;

import java.util.concurrent.atomic.AtomicInteger;

public class QueryFactoryTest extends TestBase {

    @Test
    public void changeQueryFactory() {
        final QueryFactory current = getDatastore().getQueryFactory();
        final QueryFactory custom = new DefaultQueryFactory();

        getDatastore().setQueryFactory(custom);

        Assert.assertNotSame(current, getDatastore().getQueryFactory());
        Assert.assertSame(custom, getDatastore().getQueryFactory());
    }

    @Test
    public void createQuery() {

        final AtomicInteger counter = new AtomicInteger();

        final QueryFactory queryFactory = new DefaultQueryFactory() {
            @Override
            public <T> Query<T> createQuery(final Datastore datastore, final DBCollection collection, final Class<T> type,
                                            final DBObject query) {

                counter.incrementAndGet();
                return super.createQuery(datastore, collection, type, query);
            }
        };

        getDatastore().setQueryFactory(queryFactory);

        final Query<String> query = getDatastore().find(String.class);
        final Query<String> other = getDatastore().find(String.class);

        Assert.assertNotSame(other, query);
        Assert.assertEquals(2, counter.get());
    }
}

package xyz.morphia.query;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import xyz.morphia.Datastore;
import xyz.morphia.TestBase;
import xyz.morphia.entities.SimpleEntity;

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
            public <T> Query<T> createQuery(final Datastore datastore, final MongoCollection<T> collection, final Class<T> type,
                                            final Document query) {

                counter.incrementAndGet();
                return super.createQuery(datastore, collection, type, query);
            }
        };

        getDatastore().setQueryFactory(queryFactory);

        final Query<SimpleEntity> query = getDatastore().find(SimpleEntity.class);
        final Query<SimpleEntity> other = getDatastore().find(SimpleEntity.class);

        Assert.assertNotSame(other, query);
        Assert.assertEquals(2, counter.get());
    }
}

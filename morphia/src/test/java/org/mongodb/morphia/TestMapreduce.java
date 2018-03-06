package org.mongodb.morphia;


import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.client.model.ValidationOptions;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.aggregation.AggregationTest.Book;
import org.mongodb.morphia.aggregation.AggregationTest.CountResult;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.testmodel.Circle;
import org.mongodb.morphia.testmodel.Rectangle;
import org.mongodb.morphia.testmodel.Shape;

import java.util.Iterator;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.junit.Assert.fail;


public class TestMapreduce extends TestBase {

    @Test(expected = MongoException.class)
    public void testBadMR() {
        final String map = "function () { if(this['radius']) { doEmit('circle', {count:1}); return; } emit('rect', {count:1}); }";
        final String reduce = "function (key, values) { var total = 0; for ( var i=0; i<values.length; i++ ) {total += values[i].count;} "
                              + "return { count : total }; }";

        getDatastore().mapReduce(new MapReduceOptions<ResultEntity>()
                              .resultType(ResultEntity.class)
                              .outputType(OutputType.REPLACE)
                              .query(getAds().find(Shape.class))
                              .map(map)
                              .reduce(reduce));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testOldMapReduce() {
        final Random rnd = new Random();

        //create 100 circles and rectangles
        for (int i = 0; i < 100; i++) {
            getAds().insert("shapes", new Circle(rnd.nextDouble()));
            getAds().insert("shapes", new Rectangle(rnd.nextDouble(), rnd.nextDouble()));
        }
        final String map = "function () { if(this['radius']) { emit('circle', {count:1}); return; } emit('rect', {count:1}); }";
        final String reduce = "function (key, values) { var total = 0; for ( var i=0; i<values.length; i++ ) {total += values[i].count;} "
                              + "return { count : total }; }";

        final MapReduceIterable<ResultEntity> mrRes = getDatastore().mapReduce(new MapReduceOptions<ResultEntity>()
                                                       .outputType(MapreduceType.REPLACE.toOutputType())
                                                       .query(getDatastore().find(Shape.class))
                                                       .map(map)
                                                       .reduce(reduce)
                                                       .resultType(ResultEntity.class));
        mrRes.toCollection();

        final Query<ResultEntity> query = getDatastore().find(ResultEntity.class);
        Assert.assertEquals(2, query.countAll());
        Assert.assertEquals(100, query.get().getValue().count, 0);


        final MapReduceIterable<ResultEntity> inline =
            getDatastore().mapReduce(new MapReduceOptions<ResultEntity>()
                                         .outputType(MapreduceType.INLINE.toOutputType())
                                         .query(getAds().find(Shape.class))
                                         .map(map)
                                         .reduce(reduce)
                                         .resultType(ResultEntity.class));
        final MongoCursor<ResultEntity> iterator = inline.iterator();
        Assert.assertEquals(2, count(iterator));
        Assert.assertEquals(100, inline.iterator().next().getValue().count, 0);
    }

    @Test
    public void testMapReduce() {
        final Random rnd = new Random();

        //create 100 circles and rectangles
        for (int i = 0; i < 100; i++) {
            getAds().insert("shapes", new Circle(rnd.nextDouble()));
            getAds().insert("shapes", new Rectangle(rnd.nextDouble(), rnd.nextDouble()));
        }
        final String map = "function () { if(this['radius']) { emit('circle', {count:1}); return; } emit('rect', {count:1}); }";
        final String reduce = "function (key, values) { var total = 0; for ( var i=0; i<values.length; i++ ) {total += values[i].count;} "
                              + "return { count : total }; }";

        final MapReduceIterable<ResultEntity> mrRes =
            getDatastore().mapReduce(new MapReduceOptions<ResultEntity>()
                                  .outputType(OutputType.REPLACE)
                                  .query(getAds().find(Shape.class))
                                  .map(map)
                                  .reduce(reduce)
                                  .resultType(ResultEntity.class));
        mrRes.toCollection();

        final Query<ResultEntity> query = getDatastore().find(ResultEntity.class);
        Assert.assertEquals(2, query.count());
        Assert.assertEquals(100, query.get().getValue().count, 0);


        final MapReduceIterable<ResultEntity> inline =
            getDatastore().mapReduce(new MapReduceOptions<ResultEntity>()
                                  .outputType(OutputType.INLINE)
                                  .query(getAds().find(Shape.class)).map(map).reduce(reduce)
                                  .resultType(ResultEntity.class));
        final Iterator<ResultEntity> iterator = inline.iterator();
        Assert.assertEquals(2, count(iterator));
        Assert.assertEquals(100, inline.iterator().next().getValue().count, 0);
    }

    @Test
    public void testCollation() {
        checkMinServerVersion(3.4);
        getDatastore().save(asList(new Book("The Banquet", "Dante", 2),
                            new Book("Divine Comedy", "Dante", 1),
                            new Book("Eclogues", "Dante", 2),
                            new Book("The Odyssey", "Homer", 10),
                            new Book("Iliad", "Homer", 10)));

        final String map = "function () { emit(this.author, 1); return; }";
        final String reduce = "function (key, values) { return values.length }";

        Query<Book> query = getAds().find(Book.class)
            .field("author").equal("dante");
        MapReduceOptions<CountResult> options = new MapReduceOptions<CountResult>()
            .resultType(CountResult.class)
            .outputType(OutputType.INLINE)
            .query(query)
            .map(map)
            .reduce(reduce);
        Iterator<CountResult> iterator = getDatastore().mapReduce(options).iterator();

        Assert.assertEquals(0, count(iterator));

        options
            .collation(Collation.builder()
                         .locale("en")
                         .collationStrength(CollationStrength.SECONDARY)
                         .build());
        iterator = getDatastore().mapReduce(options).iterator();
        CountResult result = iterator.next();
        Assert.assertEquals("Dante", result.getAuthor());
        Assert.assertEquals(3D, result.getCount(), 0);
    }

    @Test
    public void testBypassDocumentValidation() {
        checkMinServerVersion(3.4);
        getDatastore().save(asList(new Book("The Banquet", "Dante", 2),
                            new Book("Divine Comedy", "Dante", 1),
                            new Book("Eclogues", "Dante", 2),
                            new Book("The Odyssey", "Homer", 10),
                            new Book("Iliad", "Homer", 10)));

        Document validator = Document.parse("{ count : { $gt : '10' } }");
        ValidationOptions validationOptions = new ValidationOptions()
            .validator(validator)
            .validationLevel(ValidationLevel.STRICT)
            .validationAction(ValidationAction.ERROR);
        MongoDatabase database = getMongoClient().getDatabase(TEST_DB_NAME);
        database.getCollection("counts").drop();
        database.createCollection("counts", new CreateCollectionOptions().validationOptions(validationOptions));


        final String map = "function () { emit(this.author, 1); return; }";
        final String reduce = "function (key, values) { return values.length }";

        MapReduceOptions<CountResult> options = new MapReduceOptions<CountResult>()
            .query(getDatastore().find(Book.class))
            .resultType(CountResult.class)
            .outputType(OutputType.REPLACE)
            .map(map)
            .reduce(reduce);
        try {
            getDatastore().mapReduce(options);
            fail("Document validation should have complained.");
        } catch (MongoCommandException e) {
            // expected
        }

        getDatastore().mapReduce(options.bypassDocumentValidation(true));
        Assert.assertEquals(2, count(getDatastore().find(CountResult.class).iterator()));
    }

    @Entity("mr_results")
    private static class ResultEntity extends ResultBase<String, HasCount> {
    }

    public static class ResultBase<T, V> {
        @Id
        private T type;
        @Embedded
        private V value;

        public T getType() {
            return type;
        }

        public void setType(final T type) {
            this.type = type;
        }

        public V getValue() {
            return value;
        }

        public void setValue(final V value) {
            this.value = value;
        }
    }

    private static class HasCount {
        private double count;
    }
}

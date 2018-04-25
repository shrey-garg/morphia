package org.mongodb.morphia;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.util.Iterator;

public abstract class TestBase {
    protected static final String TEST_DB_NAME = "morphia_test";
    private final MongoClient mongoClient;
    private final Morphia morphia;
    private final Datastore ds;

    protected TestBase() {
        this(new MongoClient(new MongoClientURI(getMongoURI())));
    }

    protected TestBase(final MongoClient client) {
        mongoClient = client;
        morphia = new Morphia(getMongoClient());
        ds = getMorphia().createDatastore(TEST_DB_NAME);
    }

    static String getMongoURI() {
        return System.getProperty("MONGO_URI", "mongodb://localhost:27017");
    }

    public AdvancedDatastore getAds() {
        return (AdvancedDatastore) getDatastore();
    }

    public MongoDatabase getDatabase() {
        return ds.getDatabase();
    }

    public Datastore getDatastore() {
        return ds;
    }

    protected MongoClient getMongoClient() {
        return mongoClient;
    }

    public Morphia getMorphia() {
        return morphia;
    }

    boolean isReplicaSet() {
        return runIsMaster().get("setName") != null;
    }

    @Before
    public void setUp() {
        cleanup();
    }

    @After
    public void tearDown() {
        cleanup();
        getMongoClient().close();
    }

    protected void checkMinServerVersion(final double version) {
        Assume.assumeTrue(serverIsAtLeastVersion(version));
    }

    protected void cleanup() {
        MongoDatabase db = getDatabase();
        if (db != null) {
            db.drop();
        }
    }

    protected int count(final Iterator<?> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        return count;
    }

    /**
     * @param version must be a major version, e.g. 1.8, 2,0, 2.2
     * @return true if server is at least specified version
     */
    protected boolean serverIsAtLeastVersion(final double version) {
        return Double.parseDouble(getServerVersion().substring(0, 3)) >= version;
    }

    /**
     * @param version must be a major version, e.g. 1.8, 2,0, 2.2
     * @return true if server is at least specified version
     */
    protected boolean serverIsAtMostVersion(final double version) {
        return Double.parseDouble(getServerVersion().substring(0, 3)) <= version;
    }

    private String getServerVersion() {
        return (String) getMongoClient().getDatabase("admin")
                                        .runCommand(new Document("serverStatus", 1))
                                        .get("version");
    }

    private Document runIsMaster() {
        // Check to see if this is a replica set... if not, get out of here.
        return mongoClient.getDatabase("admin").runCommand(new Document("ismaster", 1));
    }

    protected Document toDocument(Object entity) {
        return getMorphia().getMapper().toDocument(entity);
    }

    protected <T> T fromDocument(final Datastore datastore,
                                 final Class<T> clazz,
                                 final Document document) {
        return null;
    }
}

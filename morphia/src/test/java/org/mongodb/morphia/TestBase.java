package org.mongodb.morphia;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.mongodb.morphia.mapping.Mapper;

import java.util.Iterator;
import java.util.List;

public abstract class TestBase {
    protected static final String TEST_DB_NAME = "morphia_test";
    private final MongoClient mongoClient;
    private final Datastore ds;

    protected TestBase() {
        this(new MongoClient(new MongoClientURI(getMongoURI())));
    }

    protected TestBase(final MongoClient client) {
        mongoClient = client;
        ds = Morphia.createDatastore(mongoClient, TEST_DB_NAME);
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

    boolean isReplicaSet() {
        final Document document = runIsMaster();
        final List<String> hosts = (List<String>) document.get("hosts");
        return document.get("setName") != null && hosts != null && hosts.size() > 1;
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

    private void cleanup() {
        MongoDatabase db = getDatabase();
        if (db != null) {
            db.drop();
        }
    }

    protected int count(final Iterable<?> iterable) {
        return count(iterable.iterator());
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

    private String getServerVersion() {
        return (String) getMongoClient().getDatabase("admin")
                                        .runCommand(new Document("serverStatus", 1))
                                        .get("version");
    }

    private Document runIsMaster() {
        // Check to see if this is a replica set... if not, get out of here.
        return mongoClient.getDatabase("admin").runCommand(new Document("ismaster", 1));
    }

    Document toDocument(Object entity) {
        return getDatastore().getMapper().toDocument(entity);
    }

    <T> T fromDocument(final Class<T> clazz,
                       final Document document) {
        final CodecRegistry codecRegistry = getDatastore().getMapper().getCodecRegistry();
        return codecRegistry.get(clazz)
                     .decode(new BsonDocumentReader(document.toBsonDocument(Document.class, codecRegistry)),
                         DecoderContext.builder().build());
    }

    protected CodecRegistry getCodecRegistry() {
        return getDatastore().getMapper().getCodecRegistry();
    }

    protected Mapper getMapper() {
        return getDatastore().getMapper();
    }
}

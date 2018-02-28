package org.mongodb.morphia.optimisticlocks;


import com.mongodb.client.result.UpdateResult;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Version;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.validation.ConstraintViolationException;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.testutil.TestEntity;

import java.util.ConcurrentModificationException;

public class VersionTest extends TestBase {


    @Test(expected = ConcurrentModificationException.class)
    public void testConcurrentModDetection() {
        getMorphia().map(ALongPrimitive.class);

        final ALongPrimitive a = new ALongPrimitive();
        Assert.assertEquals(0, a.version);
        getDatastore().save(a);

        getDatastore().save(getDatastore().get(a));

        getDatastore().save(a);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testConcurrentModDetectionLong() {
        final ALong a = new ALong();
        Assert.assertEquals(null, a.v);
        getDatastore().save(a);

        getDatastore().save(getDatastore().get(a));

        getDatastore().save(a);
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testConcurrentModDetectionLongWithMerge() {
        final ALong a = new ALong();
        Assert.assertEquals(null, a.v);
        getDatastore().save(a);

        a.text = " foosdfds ";
        final ALong a2 = getDatastore().get(a);
        getDatastore().save(a2);

        getDatastore().merge(a);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testInvalidVersionUse() {
        getMorphia().map(InvalidVersionUse.class);
    }

    @Test
    public void testVersionFieldNameContribution() {
        final MappedField mappedFieldByJavaField = getMorphia().getMapper().getMappedClass(ALong.class).getMappedFieldByJavaField("v");
        Assert.assertEquals("versionNameContributedByAnnotation", mappedFieldByJavaField.getNameToStore());
    }

    @Test
    public void testVersionInHashcode() {
        getMorphia().mapPackage("com.example");

        final VersionInHashcode model = new VersionInHashcode();
        model.data = "whatever";
        getDatastore().save(model);
        Assert.assertNotNull(model.version);
    }

    @Test
    public void testVersions() {
        final ALongPrimitive a = new ALongPrimitive();
        Assert.assertEquals(0, a.version);
        getDatastore().save(a);
        Assert.assertTrue(a.version > 0);
        final long version1 = a.version;

        getDatastore().save(a);
        Assert.assertTrue(a.version > 0);
        final long version2 = a.version;

        Assert.assertFalse(version1 == version2);
    }

    @Test
    public void testVersionsWithFindAndModify() {
        final ALongPrimitive initial = new ALongPrimitive();
        Datastore ds = getDatastore();
        ds.save(initial);

        Query<ALongPrimitive> query = ds.find(ALongPrimitive.class)
                                     .field("id").equal(initial.getId());
        UpdateOperations<ALongPrimitive> update = ds.createUpdateOperations(ALongPrimitive.class)
                                                    .set("text", "some new value");
        ALongPrimitive postUpdate = ds.findAndModify(query, update);

        Assert.assertEquals(initial.version + 1, postUpdate.version);
    }

    @Test
    public void testVersionsWithUpdate() {
        final ALongPrimitive initial = new ALongPrimitive();
        Datastore ds = getDatastore();
        ds.save(initial);

        Query<ALongPrimitive> query = ds.find(ALongPrimitive.class)
                                     .field("id").equal(initial.getId());
        UpdateOperations<ALongPrimitive> update = ds.createUpdateOperations(ALongPrimitive.class)
                                                    .set("text", "some new value");
        UpdateResult results = ds.updateOne(query, update);
        Assert.assertEquals(1, results.getModifiedCount());
        ALongPrimitive postUpdate = ds.get(ALongPrimitive.class, initial.getId());

        Assert.assertEquals(initial.version + 1, postUpdate.version);
    }

    @Entity
    public static class VersionInHashcode {
        @Id
        private ObjectId id;
        @Version
        private Long version;

        private String data;

        @Override
        public int hashCode() {
            final int dataHashCode = (data == null) ? 0 : data.hashCode();
            final int versionHashCode = (version == null) ? 0 : version.hashCode();
            return dataHashCode + versionHashCode;
        }
    }

    public static class ALongPrimitive extends TestEntity {

        @Version
        private long version;

        private String text;

        public long getVersion() {
            return version;
        }

        public void setVersion(final long version) {
            this.version = version;
        }

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }
    }

    public static class ALong extends TestEntity {
        @Version("versionNameContributedByAnnotation")
        private Long v;

        private String text;
    }

    @Entity
    static class InvalidVersionUse {
        @Id
        private String id;
        @Version
        private long version1;
        @Version
        private long version2;

    }

}

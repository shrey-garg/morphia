package org.mongodb.morphia;


import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.mapping.MappingException;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.TestQuery.ContainsPic;
import org.mongodb.morphia.query.TestQuery.Pic;
import org.mongodb.morphia.query.TestQuery.PicWithObjectId;


public class TestQueriesOnReferences extends TestBase {
    @Test
    public void testKeyExists() {
        final ContainsPic cpk = new ContainsPic();
        final Pic p = new Pic();
        cpk.setPic(p);
        getDatastore().save(p);
        getDatastore().save(cpk);

        Assert.assertNotNull(getDatastore().find(ContainsPic.class)
                                           .field("pic").exists()
                                           .project("pic", true).get());
        Assert.assertNull(getDatastore().find(ContainsPic.class)
                                        .field("pic").doesNotExist()
                                        .project("pic", true).get());
    }

    @Test(expected = MappingException.class)
    public void testMissingReferences() {
        final ContainsPic cpk = new ContainsPic();
        final Pic p = new Pic();
        cpk.setPic(p);
        getDatastore().save(p);
        getDatastore().save(cpk);

        getDatastore().delete(p);

        getDatastore().find(ContainsPic.class).asList();
    }

    @Test
    public void testQueryOverLazyReference() {

        final ContainsPic cpk = new ContainsPic();
        final Pic p = new Pic();
        getDatastore().save(p);
        final PicWithObjectId withObjectId = new PicWithObjectId();
        getDatastore().save(withObjectId);
        cpk.setLazyPic(p);
        cpk.setLazyObjectIdPic(withObjectId);
        getDatastore().save(cpk);

        Query<ContainsPic> query = getDatastore().find(ContainsPic.class);
        Assert.assertNotNull(query.field("lazyPic")
                                  .equal(p)
                                  .get());

        query = getDatastore().find(ContainsPic.class);
        Assert.assertNotNull(query.field("lazyObjectIdPic")
                                  .equal(withObjectId)
                                  .get());
    }

    @Test
    public void testQueryOverReference() {

        final ContainsPic cpk = new ContainsPic();
        final Pic p = new Pic();
        getDatastore().save(p);
        cpk.setPic(p);
        getDatastore().save(cpk);

        final Query<ContainsPic> query = getDatastore().find(ContainsPic.class);
        final ContainsPic object = query.field("pic")
                                        .equal(p)
                                        .get();
        Assert.assertNotNull(object);

    }

    @Test
    public void testWithKeyQuery() {
        final ContainsPic cpk = new ContainsPic();
        final Pic p = new Pic();
        cpk.setPic(p);
        getDatastore().save(p);
        getDatastore().save(cpk);

        ContainsPic containsPic = getDatastore().find(ContainsPic.class)
                                                .field("pic").equal(new Key<Pic>(Pic.class, "Pic", p.getId()))
                                                .get();
        Assert.assertEquals(cpk.getId(), containsPic.getId());

        containsPic = getDatastore().find(ContainsPic.class).field("pic").equal(new Key<Pic>(Pic.class, "Pic", p.getId())).get();
        Assert.assertEquals(cpk.getId(), containsPic.getId());
    }
}


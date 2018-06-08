package org.mongodb.morphia.mapping;


import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.testutil.TestEntity;

public class ReferencesInEmbeddedTest extends TestBase {
    @Test
    public void testLazyReferencesInEmbedded() {
        final Container container = new Container();
        container.name = "lazy";
        getDatastore().save(container);
        final ReferencedEntity referencedEntity = new ReferencedEntity();
        getDatastore().save(referencedEntity);

        container.embed = new EmbedContainingReference();
        container.embed.lazyRef = referencedEntity;
        getDatastore().save(container);

        final Container reloadedContainer = getDatastore().get(container);
        Assert.assertNotNull(reloadedContainer);
    }

    @Test
    public void testMapping() {
        getMapper().map(Container.class);
        getMapper().map(ReferencedEntity.class);
    }

    @Test
    public void testNonLazyReferencesInEmbedded() {
        final Container container = new Container();
        container.name = "nonLazy";
        getDatastore().save(container);
        final ReferencedEntity referencedEntity = new ReferencedEntity();
        getDatastore().save(referencedEntity);

        container.embed = new EmbedContainingReference();
        container.embed.ref = referencedEntity;
        getDatastore().save(container);

        final Container reloadedContainer = getDatastore().get(container);
        Assert.assertNotNull(reloadedContainer);
    }

    @Entity
    private static class Container extends TestEntity {
        private String name;
        private EmbedContainingReference embed;
    }

    @Embedded
    private static class EmbedContainingReference {
        private String name;
        @Reference
        private ReferencedEntity ref;

        @Reference
        private ReferencedEntity lazyRef;
    }

    @Entity
    public static class ReferencedEntity extends TestEntity {
        private String foo;
    }
}

package org.mongodb.morphia.mapping.lazy;


import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.testutil.TestEntity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;


/**
 * @author josephpachod
 */
public class LazyInEmbeddedTest extends TestBase {
    @Test
    public void testLoadingOfRefInField() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        getMorphia().map(ContainerWithRefInField.class);
        getMorphia().map(OtherEntity.class);

        OtherEntity otherEntity = new OtherEntity();
        ContainerWithRefInField containerWithRefInField = new ContainerWithRefInField();

        getDatastore().save(asList(otherEntity, containerWithRefInField));

        otherEntity = getDatastore().get(otherEntity);
        containerWithRefInField = getDatastore().get(containerWithRefInField);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(containerWithRefInField);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        containerWithRefInField.embedWithRef = embedWithRef;

        getDatastore().save(containerWithRefInField);

        containerWithRefInField = getDatastore().get(containerWithRefInField);
        Assert.assertNotNull(containerWithRefInField);

    }

    @Test
    public void testLoadingOfRefInList() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        getMorphia().map(ContainerWithRefList.class);
        getMorphia().map(OtherEntity.class);

        OtherEntity otherEntity = new OtherEntity();
        ContainerWithRefList containerWithRefInList = new ContainerWithRefList();

        getDatastore().save(asList(otherEntity, containerWithRefInList));

        otherEntity = getDatastore().get(otherEntity);
        containerWithRefInList = getDatastore().get(containerWithRefInList);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(containerWithRefInList);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        containerWithRefInList.embedWithRef.add(embedWithRef);

        getDatastore().save(asList(otherEntity, containerWithRefInList));

        containerWithRefInList = getDatastore().get(containerWithRefInList);
        Assert.assertNotNull(containerWithRefInList);

        final Query<ContainerWithRefList> createQuery = getDatastore().find(ContainerWithRefList.class);
        containerWithRefInList = createQuery.get();
        Assert.assertNotNull(containerWithRefInList);

    }

    @Test
    public void testLoadingOfRefThroughInheritanceInField() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        getMorphia().map(ContainerWithRefInField.class);
        getMorphia().map(OtherEntityChild.class);

        OtherEntityChild otherEntity = new OtherEntityChild();
        ContainerWithRefInField containerWithRefInField = new ContainerWithRefInField();

        getDatastore().save(asList(otherEntity, containerWithRefInField));

        otherEntity = getDatastore().get(otherEntity);
        final ContainerWithRefInField reload = getDatastore().get(containerWithRefInField);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(reload);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        reload.embedWithRef = embedWithRef;

        getDatastore().save(reload);
        getDatastore().get(reload);
        containerWithRefInField = getDatastore().get(containerWithRefInField);
        Assert.assertNotNull(containerWithRefInField);

    }

    @Test
    public void testLoadingOfRefThroughInheritanceInList() throws Exception {
        // TODO us: exclusion does not work properly with maven + junit4
        if (!LazyFeatureDependencies.testDependencyFullFilled()) {
            return;
        }

        getMorphia().map(ContainerWithRefList.class);
        getMorphia().map(OtherEntityChild.class);

        OtherEntityChild otherEntity = new OtherEntityChild();
        ContainerWithRefList containerWithRefInList = new ContainerWithRefList();

        getDatastore().save(asList(otherEntity, containerWithRefInList));

        otherEntity = getDatastore().get(otherEntity);
        final ContainerWithRefList reload = getDatastore().get(containerWithRefInList);
        Assert.assertNotNull(otherEntity);
        Assert.assertNotNull(reload);

        final EmbedWithRef embedWithRef = new EmbedWithRef();
        embedWithRef.otherEntity = otherEntity;
        reload.embedWithRef.add(embedWithRef);

        getDatastore().save(asList(otherEntity, reload));

        getDatastore().get(reload);

        containerWithRefInList = getDatastore().get(reload);
        Assert.assertNotNull(containerWithRefInList);
        final Query<ContainerWithRefList> createQuery = getDatastore().find(ContainerWithRefList.class);
        containerWithRefInList = createQuery.get();
        Assert.assertNotNull(containerWithRefInList);

    }

    public enum SomeEnum {
        B,
        A
    }

    @Entity
    public static class ContainerWithRefInField extends TestEntity {
        @Embedded
        private EmbedWithRef embedWithRef;
    }

    @Entity
    public static class ContainerWithRefList extends TestEntity {
        @Embedded
        private final List<EmbedWithRef> embedWithRef = new ArrayList<EmbedWithRef>();
    }

    @Entity
    public static class OtherEntity extends TestEntity {
        @Property(value = "some")
        private SomeEnum someEnum;

        protected OtherEntity() {
        }

        public OtherEntity(final SomeEnum someEnum) {
            this.someEnum = someEnum;

        }
    }

    @Entity
    public static class OtherEntityChild extends OtherEntity {
        @Property
        private String name;

        public OtherEntityChild() {
            super(SomeEnum.A);
        }
    }

    public static class EmbedWithRef implements Serializable {

        @Reference(lazy = true)
        private OtherEntity otherEntity;
    }
}

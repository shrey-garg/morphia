package org.mongodb.morphia;

import org.junit.Test;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.testmappackage.AbstractBaseClass;
import org.mongodb.morphia.testmappackage.ClassWithoutEntityAnnotation;
import org.mongodb.morphia.testmappackage.SimpleEntity;
import org.mongodb.morphia.testmappackage.testmapsubpackage.SimpleEntityInSubPackage;
import org.mongodb.morphia.testmappackage.testmapsubpackage.testmapsubsubpackage.SimpleEntityInSubSubPackage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MorphiaTest extends TestBase {

    @Test
    public void testSubPackagesMapping() {
        // when
        final Datastore datastore = Morphia.createDatastore("subpackage");
        final Mapper mapper = datastore.getMapper();
        mapper.getOptions().setMapSubPackages(true);
        mapper.mapPackage("org.mongodb.morphia.testmappackage");

        // then
        Collection<MappedClass> mappedClasses = mapper.getMappedClasses();
        assertThat(mappedClasses.stream().map(c->c.getClassModel().getName()).collect(Collectors.toList()).toString(),
            mappedClasses.size(), is(5));
        Collection<Class<?>> classes = new ArrayList<>();
        for (MappedClass mappedClass : mappedClasses) {
            classes.add(mappedClass.getClazz());
        }
        assertTrue(classes.contains(AbstractBaseClass.class));
        assertTrue(classes.contains(ClassWithoutEntityAnnotation.class));
        assertTrue(classes.contains(SimpleEntity.class));
        assertTrue(classes.contains(SimpleEntityInSubPackage.class));
        assertTrue(classes.contains(SimpleEntityInSubSubPackage.class));
    }

}

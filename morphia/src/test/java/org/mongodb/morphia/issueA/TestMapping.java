package org.mongodb.morphia.issueA;


import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Id;

import java.io.Serializable;


/**
 * Test from email to mongodb-users list.
 */
public class TestMapping extends TestBase {

    @Test
    public void testMapping() {
        getMorphia().map(ClassLevelThree.class);
        final ClassLevelThree sp = new ClassLevelThree();

        //Old way
        final Document wrapObj = getMorphia().getMapper().toDocument(sp);  //the error points here from the user
        getDatastore().getDatabase().getCollection("testColl").insertOne(wrapObj);


        //better way
        getDatastore().save(sp);

    }

    private interface InterfaceOne<K> {
        K getK();
    }

    private static class ClassLevelOne<K> implements InterfaceOne<K>, Serializable {
        private K k;

        @Override
        public K getK() {
            return k;
        }
    }

    private static class ClassLevelTwo extends ClassLevelOne<String> {

    }

    private static class ClassLevelThree {
        @Id
        private ObjectId id;

        private String name;

        @Embedded
        private ClassLevelTwo value;
    }

}

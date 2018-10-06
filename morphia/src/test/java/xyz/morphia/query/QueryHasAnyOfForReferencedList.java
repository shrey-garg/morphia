package xyz.morphia.query;

import org.bson.types.ObjectId;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Entity;
import xyz.morphia.annotations.Id;
import xyz.morphia.annotations.Property;
import xyz.morphia.annotations.Reference;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class QueryHasAnyOfForReferencedList extends TestBase {

    @Test
    public void testInQuery() {

        Plan plan1 = new Plan();
        plan1.name = "Trial";

        Plan plan2 = new Plan();
        plan2.name = "Trial";

        getDatastore().save(plan1);
        getDatastore().save(plan2);

        Org org1 = new Org();
        org1.plan = plan1;
        org1.name = "Test Org1";

        Org org2 = new Org();
        org2.plan = plan2;
        org2.name = "Test Org2";

        getDatastore().save(org1);
        getDatastore().save(org2);

        assertEquals(1, getDatastore().find(Org.class).field("name").equal("Test Org1").count());

        assertEquals(1, getDatastore().find(Org.class)
                                      .field("plan").hasAnyOf(singletonList(plan1))
                                      .count());

        assertEquals(2, getDatastore().find(Org.class)
                                      .field("plan").hasAnyOf(asList(plan1, plan2))
                                      .count());
    }

    @Entity(useDiscriminator = false)
    private static class Plan {

        @Id
        private ObjectId id;
        @Property("name")
        private String name;
    }

    @Entity(useDiscriminator = false)
    private static class Org {
        @Id
        private ObjectId id;
        @Property("name")
        private String name;
        @Reference("plan")
        private Plan plan;
    }

}

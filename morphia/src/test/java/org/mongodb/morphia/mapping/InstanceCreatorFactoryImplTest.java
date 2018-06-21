package org.mongodb.morphia.mapping;

import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PropertyModel;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

@SuppressWarnings("unchecked")
public class InstanceCreatorFactoryImplTest extends TestBase {
    @Test
    public void noargs() {
        final MappedClass mappedClass = getDatastore().getMapper().getMappedClass(NoArgClass.class);
        final ClassModel<NoArgClass> model = (ClassModel<NoArgClass>) mappedClass.getClassModel();
        final InstanceCreator<NoArgClass> creator = model.getInstanceCreator();

        creator.set(12, (PropertyModel) model.getPropertyModel("count"));
        creator.set("my name", (PropertyModel) model.getPropertyModel("name"));

        final NoArgClass instance = creator.getInstance();
        final NoArgClass expected = new NoArgClass();
        expected.count = 12;
        expected.name = "my name";

        Assert.assertEquals(expected, instance);
    }

    private static class NoArgClass {
        @Id
        private ObjectId id;
        @Property("howmany")
        private int count;
        @Property
        private String name;

        public ObjectId getId() {
            return id;
        }

        public void setId(final ObjectId id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final NoArgClass that = (NoArgClass) o;

            if (count != that.count) {
                return false;
            }
            if (id != null ? !id.equals(that.id) : that.id != null) {
                return false;
            }
            return name != null ? name.equals(that.name) : that.name == null;
        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + count;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }
}
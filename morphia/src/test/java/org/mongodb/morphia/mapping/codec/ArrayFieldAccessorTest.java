package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.pojo.TypeData;
import org.junit.Test;
import org.mongodb.morphia.mapping.CollectionOfValuesTest.City;

import java.lang.reflect.Field;

import static java.util.Arrays.asList;

public class ArrayFieldAccessorTest {
    @Test
    public void arrays() throws ReflectiveOperationException {
        final City city = new City();

        final Field cells = City.class.getField("cells");
        final ArrayFieldAccessor accessor = new ArrayFieldAccessor(TypeData.newInstance(cells), cells);

        final Object[] value = new Object[]{
            asList(
                asList( new Integer(0), new Integer(1)),
                asList( new Integer(10), new Integer(11))),
            asList(
                asList( new Integer(100), new Integer(101)),
                asList( new Integer(110), new Integer(111))),
            };

        accessor.set(city, value);
    }

}
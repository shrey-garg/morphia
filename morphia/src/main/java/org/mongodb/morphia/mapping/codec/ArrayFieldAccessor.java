package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.pojo.TypeData;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.List;

public class ArrayFieldAccessor extends FieldAccessor {

    private TypeData typeData;
    private Class<?> componentType;

    public ArrayFieldAccessor(final TypeData typeData, final Field field) {
        super(field);
        this.typeData = typeData;
        componentType = field.getType().getComponentType();
    }

    @Override
    public void set(final Object instance, final Object value) {
        Object newValue = value;
        if (value.getClass().getComponentType() != componentType) {
            newValue = convert((Object[]) value);
        }
        super.set(instance, newValue);
    }

    private Object convert(final Object[] value) {
        Object[] array = value;
        final Object newArray = Array.newInstance(componentType, array.length);
        for (int i = 0; i < array.length; i++) {
            Array.set(newArray, i, convert(array[i], componentType));
        }
        return newArray;
    }

    private Object convert(final Object o, final Class type) {
        if(o instanceof List) {
            List list = (List) o;
            final Object newArray = Array.newInstance(type.getComponentType(), list.size());
            for (int i = 0; i < list.size(); i++) {
                Array.set(newArray, i, convert(list.get(i), type.getComponentType()));
            }

            return newArray;
        }
        final Object convert = Conversions.convert(o.getClass(), type, o);
        return convert;
    }
}

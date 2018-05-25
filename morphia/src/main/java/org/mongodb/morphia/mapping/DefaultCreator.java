package org.mongodb.morphia.mapping;


import org.bson.Document;
import org.mongodb.morphia.ObjectFactory;
import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;

import java.lang.reflect.Constructor;


public class DefaultCreator implements ObjectFactory {

    private static final Logger LOG = MorphiaLoggerFactory.get(DefaultCreator.class);

    private static <T> Constructor<T> getNoArgsConstructor(final Class<T> type) {
        try {
            final Constructor<T> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor;
        } catch (NoSuchMethodException e) {
            throw new MappingException("No usable constructor for " + type.getName(), e);
        }
    }

    @Override
    public <T> T createInstance(final Class<T> clazz) {
        try {
            return getNoArgsConstructor(clazz).newInstance();
        } catch (Exception e) {
            throw new MappingException("No usable constructor for " + clazz.getName(), e);
        }
    }

    @Override
    public <T> T createInstance(final Class<T> clazz, final Document document) {
        Class<T> c = getClass(document);
        if (c == null) {
            c = clazz;
        }
        return createInstance(c);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object createInstance(final Mapper mapper, final MappedField mf, final Document document) {
        Class c = getClass(document);
        if (c == null) {
            c = mf.getNormalizedType();
            if (c.equals(Object.class)) {
                c = mf.getType();
            }
        }
        return createInstance(c, document);
    }

    private ClassLoader getClassLoaderForClass() {
        return Thread.currentThread().getContextClassLoader();
    }

    @SuppressWarnings("unchecked")
    private <T> Class<T> getClass(final Document document) {
        // see if there is a className value
        Class c = null;
        if (document.containsKey(Mapper.CLASS_NAME_FIELDNAME)) {
            try {
                c = Class.forName((String) document.get(Mapper.CLASS_NAME_FIELDNAME), true, getClassLoaderForClass());
            } catch (ClassNotFoundException e) {
                if (LOG.isWarningEnabled()) {
                    LOG.warning("Class not found defined in document: ", e);
                }
            }
        }
        return c;
    }
}
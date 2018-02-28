package org.mongodb.morphia.mapping;


import org.bson.Document;
import org.mongodb.morphia.ObjectFactory;
import org.mongodb.morphia.annotations.ConstructorArgs;
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
    public <T> T createInstance(final Class<T> clazz, final Document dbObj) {
        Class<T> c = getClass(dbObj);
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
            c = mf.isSingleValue() ? mf.getConcreteType() : mf.getSpecializedType();
            if (c.equals(Object.class)) {
                c = mf.getConcreteType();
            }
        }
        try {
            return createInstance(c, document);
        } catch (RuntimeException e) {
            final ConstructorArgs argAnn = mf.getAnnotation(ConstructorArgs.class);
            if (argAnn == null) {
                throw e;
            }
            //TODO: now that we have a mapper, get the arg types that way by getting the fields by name. + Validate names
            final Object[] args = new Object[argAnn.value().length];
            final Class[] argTypes = new Class[argAnn.value().length];
            for (int i = 0; i < argAnn.value().length; i++) {
                // TODO: run converters and stuff against these. Kinda like the List of List stuff,
                // using a fake MappedField to hold the value
                final Object val = document.get(argAnn.value()[i]);
                args[i] = val;
                argTypes[i] = val.getClass();
            }
            try {
                final Constructor constructor = c.getDeclaredConstructor(argTypes);
                constructor.setAccessible(true);
                return constructor.newInstance(args);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
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
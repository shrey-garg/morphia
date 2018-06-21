package org.mongodb.morphia.mapping;

import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.InstanceCreatorFactory;
import org.mongodb.morphia.Datastore;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class InstanceCreatorFactoryImpl<T> implements InstanceCreatorFactory<T> {
    private Constructor<T> noArgsConstructor = null;
    private Datastore datastore;
    private Map<String, PropertyHandler> handlers = new HashMap<>();
    private final MapperOptions options;

    InstanceCreatorFactoryImpl(final Datastore datastore, final ClassModelBuilder builder) {
        this.datastore = datastore;
        options = datastore.getMapper().getOptions();
        for (Constructor<?> constructor : builder.getType().getDeclaredConstructors()) {
            if (constructor.getParameterTypes().length == 0) {
                noArgsConstructor = (Constructor<T>) constructor;
                noArgsConstructor.setAccessible(true);
            }
        }
    }

    @Override
    public MorphiaInstanceCreator<T> create() {
        return new NoArgCreator<>(datastore, noArgsConstructor, handlers);
    }

    void register(final PropertyHandler handler) {
        handlers.put(handler.getPropertyName(), handler);
    }

}

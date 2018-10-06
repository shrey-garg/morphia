package xyz.morphia.mapping;

import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.InstanceCreatorFactory;
import xyz.morphia.Datastore;
import xyz.morphia.mapping.experimental.PropertyHandler;
import xyz.morphia.utils.ReflectionUtils;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Morphia's implementation of {@link InstanceCreatorFactory}
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class InstanceCreatorFactoryImpl<T> implements InstanceCreatorFactory<T> {
    private Constructor<T> noArgsConstructor;
    private Datastore datastore;
    private Map<String, PropertyHandler> handlers = new HashMap<>();

    InstanceCreatorFactoryImpl(final Datastore datastore, final ClassModelBuilder builder) {
        this.datastore = datastore;
        try {
            noArgsConstructor = builder.getType().getDeclaredConstructor();
            noArgsConstructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            if (ReflectionUtils.isConcrete(builder.getType())) {
                throw new MappingException(builder.getType().getName() + " must have a 0 argument constructor");
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

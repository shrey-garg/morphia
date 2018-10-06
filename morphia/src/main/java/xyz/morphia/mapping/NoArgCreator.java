package xyz.morphia.mapping;

import org.bson.codecs.pojo.PropertyModel;
import xyz.morphia.Datastore;
import xyz.morphia.mapping.experimental.PropertyHandler;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

class NoArgCreator<E> implements MorphiaInstanceCreator<E> {
    private final E instance;
    private Datastore datastore;
    private Map<String, PropertyHandler> handlerMap;
    private List<BiConsumer<Datastore, Map<Object, Object>>> handlers = new ArrayList<>();

    NoArgCreator(final Datastore datastore, final Constructor<E> noArgsConstructor, final Map<String, PropertyHandler> handlerMap) {
        this.datastore = datastore;
        this.handlerMap = handlerMap;
        try {
            instance = noArgsConstructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new MappingException(e.getMessage(), e);
        }
    }

    @Override
    public <S> void set(final S value, final PropertyModel<S> propertyModel) {
        final PropertyHandler propertyHandler = getHandler(propertyModel);
        if (propertyHandler != null) {
            defer((datastore, entityCache) -> propertyHandler.set(instance, propertyModel, value, datastore, entityCache));
        } else {
            propertyModel.getPropertyAccessor().set(instance, value);
        }
    }

    @Override
    public E getInstance() {
        Map<Object, Object> cache = new HashMap<>();
        for (BiConsumer<Datastore, Map<Object, Object>> deferral : handlers) {
            deferral.accept(datastore, cache);
        }
        return instance;
    }

    @Override
    public void defer(final BiConsumer<Datastore, Map<Object, Object>> function) {
        handlers.add(function);
    }

    @Override
    public <S> PropertyHandler getHandler(final PropertyModel<S> propertyModel) {
        return handlerMap.get(propertyModel.getName());
    }

}

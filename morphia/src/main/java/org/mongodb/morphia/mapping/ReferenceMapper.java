package org.mongodb.morphia.mapping;


import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;


@SuppressWarnings({"unchecked", "rawtypes"})
class ReferenceMapper implements CustomMapper {
    public static final Logger LOG = MorphiaLoggerFactory.get(ReferenceMapper.class);

    @Override
    public void fromDatabase(/*final Datastore datastore, final DBObject dbObject, final MappedField mf, final Object entity,
                             final EntityCache cache, final Mapper mapper*/) {
/*
        final Class fieldType = mf.getType();

        final Reference refAnn = mf.getAnnotation(Reference.class);
        if (mf.isMap()) {
            readMap(datastore, mapper, entity, refAnn, cache, mf, dbObject);
        } else if (mf.isMultipleValues()) {
            readCollection(datastore, mapper, dbObject, mf, entity, refAnn, cache);
        } else {
            readSingle(datastore, mapper, entity, fieldType, refAnn, cache, mf, dbObject);
        }
*/

    }

    @Override
    public void toDatabase(/*final Object entity, final MappedField mf, final DBObject dbObject, final Map<Object, DBObject> involvedObjects,
                           final Mapper mapper*/) {
/*
        final String name = mf.getNameToStore();

        final Object fieldValue = mf.getFieldValue(entity);

        if (fieldValue == null && !mapper.getOptions().isStoreNulls()) {
            return;
        }

        final Reference refAnn = mf.getAnnotation(Reference.class);
        if (mf.isMap()) {
            writeMap(mf, dbObject, name, fieldValue, refAnn, mapper);
        } else if (mf.isMultipleValues()) {
            writeCollection(mf, dbObject, name, fieldValue, refAnn, mapper);
        } else {
            writeSingle(dbObject, name, fieldValue, refAnn, mapper);
        }
*/

    }

}

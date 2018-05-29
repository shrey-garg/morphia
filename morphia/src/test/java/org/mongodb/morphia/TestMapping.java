/*
  Copyright (C) 2010 Olafur Gauti Gudmundsson
  <p/>
  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may
  obtain a copy of the License at
  <p/>
  http://www.apache.org/licenses/LICENSE-2.0
  <p/>
  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.
 */
package org.mongodb.morphia;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestInheritanceMappings.MapLike;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Serialized;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.MappingException;
import org.mongodb.morphia.mapping.codec.DocumentWriter;
import org.mongodb.morphia.testmodel.Article;
import org.mongodb.morphia.testmodel.Circle;
import org.mongodb.morphia.testmodel.RecursiveChild;
import org.mongodb.morphia.testmodel.RecursiveParent;
import org.mongodb.morphia.testmodel.Translation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMapping extends TestBase {

    @Test
    public void testBadMappings() {
        try {
            getMorphia().map(MissingId.class);
            fail("Validation: Missing @Id field not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMorphia().map(IdOnEmbedded.class);
            fail("Validation: @Id field on @Embedded not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMorphia().map(RenamedEmbedded.class);
            fail("Validation: @Embedded(\"name\") not caught on Class");
        } catch (MappingException e) {
            // good
        }

        try {
            getMorphia().map(MissingIdRenamed.class);
            fail("Validation: Missing @Id field not not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMorphia().map(NonStaticInnerClass.class);
            fail("Validation: Non-static inner class allowed");
        } catch (MappingException e) {
            // good
        }
    }

    @Test
    public void testBaseEntityValidity() {
        getMorphia().map(UsesBaseEntity.class);
    }

    @Test
    public void testByteArrayMapping() {
        getMorphia().map(ContainsByteArray.class);
        final Key<ContainsByteArray> savedKey = getDatastore().save(new ContainsByteArray());
        final ContainsByteArray loaded = getDatastore().get(ContainsByteArray.class, savedKey.getId());
        assertEquals(new String((new ContainsByteArray()).bytes), new String(loaded.bytes));
        assertNotNull(loaded.id);
    }

    @Test
    public void testCollectionMapping() {
        getMorphia().map(ContainsCollection.class);

        final ContainsCollection entity = new ContainsCollection();
        final Key<ContainsCollection> savedKey = getDatastore().save(entity);
        final ContainsCollection loaded = getDatastore().get(ContainsCollection.class, savedKey.getId());

        assertEquals(loaded.coll, entity.coll);
        assertNotNull(loaded.id);
    }

/*
    @Test
    public void testDbRefMapping() throws Exception {
        getMorphia().map(ContainsRef.class).map(Rectangle.class);
        final DBCollection stuff = getDb().getCollection("stuff");
        final DBCollection rectangles = getDb().getCollection("rectangles");

        assertTrue("'ne' field should not be persisted!", !getMorphia().getMapper().getMCMap().get(ContainsRef.class.getName())
                                                                       .containsJavaFieldName("ne"));

        final Rectangle r = new Rectangle(1, 1);
        final Document rDocument = toDocument(r);
        rDocument.put("_ns", rectangles.getName());
        rectangles.save(rDocument);

        final ContainsRef cRef = new ContainsRef();
        cRef.rect = new DBRef((String) rDocument.get("_ns"), rDocument.get("_id"));
        final Document cRefDocument = toDocument(cRef);
        stuff.save(cRefDocument);
        final Document cRefDocumentLoaded = (Document) stuff.find(DocumentBuilder.start("_id", cRefDocument.get("_id"))
                                                                                                   .get());
        final ContainsRef cRefLoaded = fromDocument(getDs(), ContainsRef.class, cRefDocumentLoaded, new DefaultEntityCache());
        assertNotNull(cRefLoaded);
        assertNotNull(cRefLoaded.rect);
        assertNotNull(cRefLoaded.rect.getId());
        assertNotNull(cRefLoaded.rect.getCollectionName());
        assertEquals(cRefLoaded.rect.getId(), cRef.rect.getId());
        assertEquals(cRefLoaded.rect.getCollectionName(), cRef.rect.getCollectionName());
    }
*/

    @Test
    public void testEmbeddedArrayElementHasNoClassname() {
        getMorphia().map(ContainsEmbeddedArray.class);
        final ContainsEmbeddedArray cea = new ContainsEmbeddedArray();
        cea.res = new RenamedEmbedded[]{new RenamedEmbedded()};

        final Document res = (Document) ((List) toDocument(cea).get("res")).get(0);
        assertFalse(res.containsKey(Mapper.CLASS_NAME_FIELDNAME));
    }

    @Test
    public void testEmbeddedDocument() {
        getMorphia().map(ContainsDocument.class);
        getDatastore().save(new ContainsDocument());
        assertNotNull(getDatastore().find(ContainsDocument.class).get());
    }

    @Test
    public void testEmbeddedEntity() {
        getMorphia().map(ContainsEmbeddedEntity.class);
        getDatastore().save(new ContainsEmbeddedEntity());
        final ContainsEmbeddedEntity ceeLoaded = getDatastore().find(ContainsEmbeddedEntity.class).get();
        assertNotNull(ceeLoaded);
        assertNotNull(ceeLoaded.id);
        assertNotNull(ceeLoaded.cil);
        assertNull(ceeLoaded.cil.id);

    }

    @Test
    public void testEmbeddedEntityDocumentHasNoClassname() {
        getMorphia().map(ContainsEmbeddedEntity.class);
        final ContainsEmbeddedEntity cee = new ContainsEmbeddedEntity();
        cee.cil = new ContainsIntegerList();
        cee.cil.intList = Collections.singletonList(1);
        assertTrue(!((Document) toDocument(cee).get("cil")).containsKey(Mapper.CLASS_NAME_FIELDNAME));
    }

    @Test
    public void testEnumKeyedMap() {
        final ContainsEnum1KeyMap map = new ContainsEnum1KeyMap();
        map.values.put(Enum1.A, "I'm a");
        map.values.put(Enum1.B, "I'm b");
        map.embeddedValues.put(Enum1.A, "I'm a");
        map.embeddedValues.put(Enum1.B, "I'm b");

        final Key<?> mapKey = getDatastore().save(map);

        final ContainsEnum1KeyMap mapLoaded = getDatastore().get(ContainsEnum1KeyMap.class, mapKey.getId());

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(Enum1.A));
        assertNotNull(mapLoaded.values.get(Enum1.B));
        assertEquals(2, mapLoaded.embeddedValues.size());
        assertNotNull(mapLoaded.embeddedValues.get(Enum1.A));
        assertNotNull(mapLoaded.embeddedValues.get(Enum1.B));
    }

    @Test
    public void testFinalField() {
        getMorphia().map(ContainsFinalField.class);
        final Key<ContainsFinalField> savedKey = getDatastore().save(new ContainsFinalField("blah"));
        final ContainsFinalField loaded = getDatastore().get(ContainsFinalField.class, savedKey.getId());
        assertNotNull(loaded);
        assertNotNull(loaded.name);
        assertEquals("blah", loaded.name);
    }

    @Test
    public void testFinalFieldNotPersisted() {
        getMapper().getOptions().setIgnoreFinals(true);
        getMorphia().map(ContainsFinalField.class);
        final ContainsFinalField blah = new ContainsFinalField("blah");
        blah.setColor(System.currentTimeMillis() + "");
        final Key<ContainsFinalField> savedKey = getDatastore().save(blah);
        final ContainsFinalField loaded = getDatastore().get(ContainsFinalField.class, savedKey.getId());
        assertNotNull(loaded);
        assertNotNull(loaded.name);
        assertEquals("foo", loaded.name);
    }

    @Test
    public void testFinalIdField() {
        getMorphia().map(HasFinalFieldId.class);
        final Key<HasFinalFieldId> savedKey = getDatastore().save(new HasFinalFieldId(12));
        final HasFinalFieldId loaded = getDatastore().get(HasFinalFieldId.class, savedKey.getId());
        assertNotNull(loaded);
        assertEquals(12, loaded.id);
    }

    @Test
    @Ignore("need to add this feature")
    @SuppressWarnings("unchecked")
    public void testGenericKeyedMap() {
        final ContainsXKeyMap<Integer> map = new ContainsXKeyMap<>();
        map.values.put(1, "I'm 1");
        map.values.put(2, "I'm 2");

        final Key<ContainsXKeyMap<Integer>> mapKey = getDatastore().save(map);

        final ContainsXKeyMap<Integer> mapLoaded = getDatastore().get(ContainsXKeyMap.class, mapKey.getId());

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(1));
        assertNotNull(mapLoaded.values.get(2));
    }

    @Test
    public void testIdFieldWithUnderscore() {
        getMorphia().map(StrangelyNamedIdField.class);
    }

    @Test
    @Ignore
    public void testIntKeySetStringMap() {
        final ContainsIntKeySetStringMap map = new ContainsIntKeySetStringMap();
        map.values.put(1, Collections.singleton("I'm 1"));
        map.values.put(2, Collections.singleton("I'm 2"));

        final Key<?> mapKey = getDatastore().save(map);

        final ContainsIntKeySetStringMap mapLoaded = getDatastore().get(ContainsIntKeySetStringMap.class, mapKey.getId());

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(1));
        assertNotNull(mapLoaded.values.get(2));
        assertEquals(1, mapLoaded.values.get(1).size());

        assertNotNull(getDatastore().find(ContainsIntKeyMap.class).field("values.2").exists());
        assertEquals(0, getDatastore().find(ContainsIntKeyMap.class).field("values.2").doesNotExist().count());
        assertNotNull(getDatastore().find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
        assertEquals(0, getDatastore().find(ContainsIntKeyMap.class).field("values.4").exists().count());
    }

    @Test
    public void testIntKeyedMap() {
        final ContainsIntKeyMap map = new ContainsIntKeyMap();
        map.values.put(1, "I'm 1");
        map.values.put(2, "I'm 2");

        final Key<?> mapKey = getDatastore().save(map);

        final ContainsIntKeyMap mapLoaded = getDatastore().get(ContainsIntKeyMap.class, mapKey.getId());

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(1));
        assertNotNull(mapLoaded.values.get(2));

        assertNotNull(getDatastore().find(ContainsIntKeyMap.class).field("values.2").exists());
        assertEquals(0, getDatastore().find(ContainsIntKeyMap.class).field("values.2").doesNotExist().count());
        assertNotNull(getDatastore().find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
        assertEquals(0, getDatastore().find(ContainsIntKeyMap.class).field("values.4").exists().count());
    }

    @Test
    public void testIntLists() {
        ContainsIntegerList cil = new ContainsIntegerList();
        getDatastore().save(cil);
        ContainsIntegerList cilLoaded = getDatastore().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(cilLoaded.intList.size(), cil.intList.size());


        cil = new ContainsIntegerList();
        cil.intList = null;
        getDatastore().save(cil);
        cilLoaded = getDatastore().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(0, cilLoaded.intList.size());

        cil = new ContainsIntegerList();
        cil.intList.add(1);
        getDatastore().save(cil);
        cilLoaded = getDatastore().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(1, cilLoaded.intList.size());
        assertEquals(1, (int) cilLoaded.intList.get(0));
    }

    @Test
    public void testLongArrayMapping() {
        getMorphia().map(ContainsLongAndStringArray.class);
        getDatastore().save(new ContainsLongAndStringArray());
        ContainsLongAndStringArray loaded = getDatastore().find(ContainsLongAndStringArray.class).get();
        assertArrayEquals(loaded.longs, (new ContainsLongAndStringArray()).longs);
        assertArrayEquals(loaded.strings, (new ContainsLongAndStringArray()).strings);

        final ContainsLongAndStringArray array = new ContainsLongAndStringArray();
        array.strings = new String[]{"a", "B", "c"};
        array.longs = new Long[]{4L, 5L, 4L};
        final Key<ContainsLongAndStringArray> k1 = getDatastore().save(array);
        loaded = getDatastore().getByKey(ContainsLongAndStringArray.class, k1);
        assertArrayEquals(loaded.longs, array.longs);
        assertArrayEquals(loaded.strings, array.strings);

        assertNotNull(loaded.id);
    }

    @Test
    public void testMapLike() {
        final ContainsMapLike ml = new ContainsMapLike();
        ml.m.put("first", "test");
        getDatastore().save(ml);
        final ContainsMapLike mlLoaded = getDatastore().find(ContainsMapLike.class).get();
        assertNotNull(mlLoaded);
        assertNotNull(mlLoaded.m);
        assertTrue(mlLoaded.m.containsKey("first"));
    }

    @Test
    public void testMapWithEmbeddedInterface() {
        final ContainsMapWithEmbeddedInterface aMap = new ContainsMapWithEmbeddedInterface();
        final Foo f1 = new Foo1();
        final Foo f2 = new Foo2();

        aMap.embeddedValues.put("first", f1);
        aMap.embeddedValues.put("second", f2);

        final CodecRegistry codecRegistry = getCodecRegistry();
        final Codec<ContainsMapWithEmbeddedInterface> codec = codecRegistry
                                                                          .get(ContainsMapWithEmbeddedInterface.class);
        final DocumentWriter writer = new DocumentWriter();
        codec.encode(writer, aMap, EncoderContext.builder().build());

        final Document root = writer.getRoot();
        codec.decode(new BsonDocumentReader(root.toBsonDocument(Document.class, codecRegistry)), DecoderContext.builder().build());
        getDatastore().save(aMap);

        final ContainsMapWithEmbeddedInterface mapLoaded = getDatastore().find(ContainsMapWithEmbeddedInterface.class).get();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.embeddedValues.size());
        assertTrue(mapLoaded.embeddedValues.get("first") instanceof Foo1);
        assertTrue(mapLoaded.embeddedValues.get("second") instanceof Foo2);

    }

    @Test
    public void testObjectIdKeyedMap() {
        getMorphia().map(ContainsObjectIdKeyMap.class);
        final ContainsObjectIdKeyMap map = new ContainsObjectIdKeyMap();
        final ObjectId o1 = new ObjectId("111111111111111111111111");
        final ObjectId o2 = new ObjectId("222222222222222222222222");
        map.values.put(o1, "I'm 1s");
        map.values.put(o2, "I'm 2s");

        final Key<?> mapKey = getDatastore().save(map);

        final ContainsObjectIdKeyMap mapLoaded = getDatastore().get(ContainsObjectIdKeyMap.class, mapKey.getId());

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(o1));
        assertNotNull(mapLoaded.values.get(o2));

        assertNotNull(getDatastore().find(ContainsIntKeyMap.class).field("values.111111111111111111111111").exists());
        assertEquals(0, getDatastore().find(ContainsIntKeyMap.class).field("values.111111111111111111111111").doesNotExist().count());
        assertNotNull(getDatastore().find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
        assertEquals(0, getDatastore().find(ContainsIntKeyMap.class).field("values.4").exists().count());
    }

    @Test
    public void testPrimMap() {
        final ContainsPrimitiveMap primMap = new ContainsPrimitiveMap();
        primMap.embeddedValues.put("first", 1L);
        primMap.embeddedValues.put("second", 2L);
        primMap.values.put("first", 1L);
        primMap.values.put("second", 2L);
        final Key<ContainsPrimitiveMap> primMapKey = getDatastore().save(primMap);

        final ContainsPrimitiveMap primMapLoaded = getDatastore().get(ContainsPrimitiveMap.class, primMapKey.getId());

        assertNotNull(primMapLoaded);
        assertEquals(2, primMapLoaded.embeddedValues.size());
        assertEquals(2, primMapLoaded.values.size());
    }

    @Test
    public void testPrimMapWithNullValue() {
        final ContainsPrimitiveMap primMap = new ContainsPrimitiveMap();
        primMap.embeddedValues.put("first", null);
        primMap.embeddedValues.put("second", 2L);
        primMap.values.put("first", null);
        primMap.values.put("second", 2L);
        final Key<ContainsPrimitiveMap> primMapKey = getDatastore().save(primMap);

        final ContainsPrimitiveMap primMapLoaded = getDatastore().get(ContainsPrimitiveMap.class, primMapKey.getId());

        assertNotNull(primMapLoaded);
        assertEquals(2, primMapLoaded.embeddedValues.size());
        assertEquals(2, primMapLoaded.values.size());
    }

    @Test
    @Ignore("References are not currently supported")
    public void testRecursiveReference() {
        final MongoCollection<Document> stuff = getDatabase().getCollection("stuff");

        getMorphia().map(RecursiveParent.class, RecursiveChild.class);

        final RecursiveParent parent = new RecursiveParent();
        final Document parentDocument = toDocument(parent);
        stuff.insertOne(parentDocument);

        final RecursiveChild child = new RecursiveChild();
        final Document childDocument = toDocument(child);
        stuff.insertOne(childDocument);

        final RecursiveParent parentLoaded = fromDocument(RecursiveParent.class,
            stuff.find(new Document(Mapper.ID_KEY, parentDocument.get(Mapper.ID_KEY))).iterator().tryNext());
        final RecursiveChild childLoaded = fromDocument(RecursiveChild.class,
            stuff.find(new Document(Mapper.ID_KEY, childDocument.get(Mapper.ID_KEY))).iterator().tryNext());

        parentLoaded.setChild(childLoaded);
        childLoaded.setParent(parentLoaded);

        stuff.insertOne(toDocument(parentLoaded));
        stuff.insertOne(toDocument(childLoaded));

        final RecursiveParent finalParentLoaded = fromDocument(RecursiveParent.class,
            stuff.find(new Document(Mapper.ID_KEY, parentDocument.get(Mapper.ID_KEY))).iterator().tryNext());
        final RecursiveChild finalChildLoaded = fromDocument(RecursiveChild.class,
            stuff.find(new Document(Mapper.ID_KEY, childDocument.get(Mapper.ID_KEY))).iterator().tryNext());

        assertNotNull(finalParentLoaded.getChild());
        assertNotNull(finalChildLoaded.getParent());
    }

    @Test(expected = MappingException.class)
    @Ignore("References are not currently supported")
    public void testReferenceWithoutIdValue() {
        final RecursiveParent parent = new RecursiveParent();
        final RecursiveChild child = new RecursiveChild();
        child.setId(null);
        parent.setChild(child);
        getDatastore().save(parent);

    }

    @Test
    @Ignore("@Serialized might be removed altogether")
    public void testSerializedMapping() {
        getMorphia().map(ContainsSerializedData.class);
        final Key<ContainsSerializedData> savedKey = getDatastore().save(new ContainsSerializedData());
        final ContainsSerializedData loaded = getDatastore().get(ContainsSerializedData.class, savedKey.getId());
        assertNotNull(loaded.data);
        assertEquals(loaded.data.someString, (new ContainsSerializedData()).data.someString);
        assertNotNull(loaded.id);
    }

    @Test
    public void testUUID() {
        //       getMorphia().map(ContainsUUID.class);
        final ContainsUUID uuid = new ContainsUUID();
        final UUID before = uuid.uuid;
        getDatastore().save(uuid);
        final ContainsUUID loaded = getDatastore().find(ContainsUUID.class).get();
        assertNotNull(loaded);
        assertNotNull(loaded.id);
        assertNotNull(loaded.uuid);
        assertEquals(before, loaded.uuid);
    }

    @Test
    public void testUuidId() {
        getMorphia().map(ContainsUuidId.class);
        final ContainsUuidId uuidId = new ContainsUuidId();
        final UUID before = uuidId.id;
        getDatastore().save(uuidId);
        final ContainsUuidId loaded = getDatastore().get(ContainsUuidId.class, before);
        assertNotNull(loaded);
        assertNotNull(loaded.id);
        assertEquals(before, loaded.id);
    }

    public enum Enum1 {
        A,
        B
    }

    @Embedded
    private interface Foo {
    }

    public abstract static class BaseEntity {
        @Id
        private ObjectId id;

        public String getId() {
            return id.toString();
        }

        public void setId(final String id) {
            this.id = new ObjectId(id);
        }
    }

    @Entity
    public static class MissingId {
        private String id;
    }

    private static class MissingIdStill {
        private String id;
    }

    @Entity("no-id")
    private static class MissingIdRenamed {
        private String id;
    }

    @Embedded
    private static class IdOnEmbedded {
        @Id
        private ObjectId id;
    }

    @Embedded(value = "no-id", useDiscriminator = false)
    private static class RenamedEmbedded {
        private String name;
    }

    private static class StrangelyNamedIdField {
        //CHECKSTYLE:OFF
        @Id
        private ObjectId id_ = new ObjectId();
        //CHECKSTYLE:ON
    }

    private static class ContainsEmbeddedArray {
        @Id
        private ObjectId id = new ObjectId();
        private RenamedEmbedded[] res;
    }

    private static class NotEmbeddable {
        private String noImNot = "no, I'm not";
    }

    private static class SerializableClass {
        private final String someString = "hi, from the ether.";
    }

    private static class ContainsRef {
        @Id
        private ObjectId id;
        private DBRef rect;
    }

    private static class HasFinalFieldId {
        @Id
        private final long id;
        private String name = "some string";

        //only called when loaded by the persistence framework.
        protected HasFinalFieldId() {
            id = -1;
        }

        HasFinalFieldId(final long id) {
            this.id = id;
        }
    }

    private static class ContainsFinalField {
        @Id
        private ObjectId id;
        private String color;
        private final String name;

        protected ContainsFinalField() {
            name = "foo";
        }

        ContainsFinalField(final String name) {
            this.name = name;
        }

        public String getColor() {
            return color;
        }

        public void setColor(final String color) {
            this.color = color;
        }
    }

    private static class ContainsDocument {
        @Id
        private ObjectId id;
        private Document document = new Document("field", "val");
    }

    private static class ContainsByteArray {
        private final byte[] bytes = "Scott".getBytes();
        @Id
        private ObjectId id;
    }

    private static class ContainsSerializedData {
        @Serialized
        private final SerializableClass data = new SerializableClass();
        @Id
        private ObjectId id;
    }

    private static class ContainsLongAndStringArray {
        @Id
        private ObjectId id;
        private Long[] longs = {0L, 1L, 2L};
        private String[] strings = {"Scott", "Rocks"};
    }

    private static final class ContainsCollection {
        private final Collection<String> coll = new ArrayList<>();
        @Id
        private ObjectId id;

        private ContainsCollection() {
            coll.add("hi");
            coll.add("Scott");
        }
    }

    private static class ContainsPrimitiveMap {
        private final Map<String, Long> embeddedValues = new HashMap<>();
        private final Map<String, Long> values = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class Foo1 implements Foo {
        private String s;
    }

    private static class Foo2 implements Foo {
        private int i;
    }

    private static class ContainsMapWithEmbeddedInterface {
        private final Map<String, Foo> embeddedValues = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class ContainsEmbeddedEntity {
        @Id
        private final ObjectId id = new ObjectId();
        private ContainsIntegerList cil = new ContainsIntegerList();
    }

    @Entity(value = "cil", useDiscriminator = false)
    private static class ContainsIntegerList {
        @Id
        private ObjectId id;
        private List<Integer> intList = new ArrayList<>();
    }

    private static class ContainsIntegerListNewAndOld {
        @Id
        private ObjectId id;
        private List<Integer> intList = new ArrayList<>();
        private List<Integer> integers = new ArrayList<>();
    }

    @Entity(value = "cil", useDiscriminator = false)
    private static class ContainsIntegerListNew {
        @Id
        private ObjectId id;
        private final List<Integer> integers = new ArrayList<>();
    }

    @Entity(useDiscriminator = false)
    private static class ContainsUUID {
        private final UUID uuid = UUID.randomUUID();
        @Id
        private ObjectId id;
    }

    @Entity(useDiscriminator = false)
    private static class ContainsUuidId {
        @Id
        private final UUID id = UUID.randomUUID();
    }

    private static class ContainsEnum1KeyMap {
        private final Map<Enum1, String> values = new HashMap<>();
        private final Map<Enum1, String> embeddedValues = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class ContainsIntKeyMap {
        private final Map<Integer, String> values = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class ContainsIntKeySetStringMap {
        private final Map<Integer, Set<String>> values = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class ContainsObjectIdKeyMap {
        private final Map<ObjectId, String> values = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class ContainsXKeyMap<T> {
        private final Map<T, String> values = new HashMap<>();
        @Id
        private ObjectId id;
    }

    private static class ContainsMapLike {
        private final MapLike m = new MapLike();
        @Id
        private ObjectId id;
    }

    @Entity
    private static class UsesBaseEntity extends BaseEntity {

    }

    private static class MapSubclass extends LinkedHashMap<String, Object> {
        @Id
        private ObjectId id;
    }

    private class NonStaticInnerClass {
        @Id
        private long id = 1;
    }
}

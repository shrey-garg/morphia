package org.mongodb.morphia.mapping.codec;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

class DocumentReader implements BsonReader {
    private Document document;

    DocumentReader(final Document document) {
        this.document = document;
    }

    @Override
    public BsonType getCurrentBsonType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrentName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonBinary readBinaryData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte peekBinarySubType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int peekBinarySize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonBinary readBinaryData(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBoolean(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonType readBsonType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readDateTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readDateTime(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readEndArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readEndDocument() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt32() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt32(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readInt64() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readInt64(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Decimal128 readDecimal128() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Decimal128 readDecimal128(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readJavaScript() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readJavaScript(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readJavaScriptWithScope() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readJavaScriptWithScope(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readMaxKey() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readMaxKey(final String name) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readMinKey() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readMinKey(final String name) {
        throw new UnsupportedOperationException();

    }

    @Override
    public String readName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readName(final String name) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readNull() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readNull(final String name) {
        throw new UnsupportedOperationException();

    }

    @Override
    public ObjectId readObjectId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectId readObjectId(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonRegularExpression readRegularExpression() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonRegularExpression readRegularExpression(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonDbPointer readDBPointer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonDbPointer readDBPointer(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readStartArray() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readStartDocument() {
    }

    @Override
    public String readString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readString(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readSymbol() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readSymbol(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonTimestamp readTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonTimestamp readTimestamp(final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readUndefined() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void readUndefined(final String name) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void skipName() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void skipValue() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void mark() {
        throw new UnsupportedOperationException();

    }

    @Override
    public BsonReaderMark getMark() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void close() {

    }
    
    private static class JavaTypeToBsonTypeMap extends HashMap<Class, BsonType> {
        public JavaTypeToBsonTypeMap() {
            put(List.class, BsonType.ARRAY);
            put(Binary.class, BsonType.BINARY);
            put(Boolean.class, BsonType.BOOLEAN);
            put(Date.class, BsonType.DATE_TIME);
            put(BsonDbPointer.class, BsonType.DB_POINTER);
            put(Document.class, BsonType.DOCUMENT);
            put(Double.class, BsonType.DOUBLE);
            put(Integer.class, BsonType.INT32);
            put(Long.class, BsonType.INT64);
            put(Decimal128.class, BsonType.DECIMAL128);
            put(MaxKey.class, BsonType.MAX_KEY);
            put(MinKey.class, BsonType.MIN_KEY);
            put(Code.class, BsonType.JAVASCRIPT);
            put(CodeWithScope.class, BsonType.JAVASCRIPT_WITH_SCOPE);
            put(ObjectId.class, BsonType.OBJECT_ID);
            put(BsonRegularExpression.class, BsonType.REGULAR_EXPRESSION);
            put(String.class, BsonType.STRING);
            put(Symbol.class, BsonType.SYMBOL);
            put(BsonTimestamp.class, BsonType.TIMESTAMP);
            put(BsonUndefined.class, BsonType.UNDEFINED);
        }
    }
}

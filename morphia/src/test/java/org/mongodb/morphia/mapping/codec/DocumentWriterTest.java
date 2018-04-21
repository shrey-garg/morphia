package org.mongodb.morphia.mapping.codec;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

public class DocumentWriterTest {
    @Test
    public void document() {
        final DocumentWriter writer = new DocumentWriter();
        writer.writeStartDocument("test");
        writer.writeSymbol("hello");
        writer.writeEndDocument();
        final Document document = writer.getRoot();

        Assert.assertEquals(new Document("test", "hello").toString(), document.toString());
    }

}
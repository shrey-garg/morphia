package org.mongodb.morphia.mapping;

import org.junit.Test;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.entities.SimpleEntity;

public class PojoCodecTest {
    @Test
    public void mapping() {
        final Morphia morphia = new Morphia();
        morphia.map(SimpleEntity.class);
    }
}

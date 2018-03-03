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


import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Test;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.testmodel.Circle;
import org.mongodb.morphia.testmodel.Rectangle;
import org.mongodb.morphia.testmodel.Shape;
import org.mongodb.morphia.testmodel.ShapeShifter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestInterfaces extends TestBase {

    @Test
    public void testDynamicInstantiation() {
        final MongoCollection<Document> shapes = getDatabase().getCollection("shapes");
        final MongoCollection<Document> shapeshifters = getDatabase().getCollection("shapeshifters");

        getMorphia().map(Circle.class, Rectangle.class, ShapeShifter.class);

        final Shape rectangle = new Rectangle(2, 5);

        final Document rectangleDoc = getMorphia().toDBObject(rectangle);
        shapes.insertOne(rectangleDoc);

        final Document rectangleDocLoaded = shapes.find(new Document(Mapper.ID_KEY, rectangleDoc.get(Mapper.ID_KEY)))
                                                  .iterator()
                                                  .tryNext();
        final Shape rectangleLoaded = getMorphia().fromDBObject(getDatastore(), Shape.class, rectangleDocLoaded);

        assertTrue(rectangle.getArea() == rectangleLoaded.getArea());
        assertTrue(rectangleLoaded instanceof Rectangle);

        final ShapeShifter shifter = new ShapeShifter();
        shifter.setReferencedShape(rectangleLoaded);
        shifter.setMainShape(new Circle(2.2));
        shifter.getAvailableShapes().add(new Rectangle(3, 3));
        shifter.getAvailableShapes().add(new Circle(4.4));

        final Document shifterDoc = getMorphia().toDBObject(shifter);
        shapeshifters.insertOne(shifterDoc);

        final Document shifterDocLoaded = shapeshifters.find(new Document(Mapper.ID_KEY, shifterDoc.get(Mapper.ID_KEY)))
                                                       .iterator()
                                                       .tryNext();
        final ShapeShifter shifterLoaded = getMorphia().fromDBObject(getDatastore(), ShapeShifter.class, shifterDocLoaded);
        assertNotNull(shifterLoaded);
        assertNotNull(shifterLoaded.getReferencedShape());
        assertNotNull(rectangle);

        assertTrue(rectangle.getArea() == shifterLoaded.getReferencedShape().getArea());
        assertTrue(shifterLoaded.getReferencedShape() instanceof Rectangle);
        assertTrue(shifter.getMainShape().getArea() == shifterLoaded.getMainShape().getArea());
        assertEquals(shifter.getAvailableShapes().size(), shifterLoaded.getAvailableShapes().size());
    }
}

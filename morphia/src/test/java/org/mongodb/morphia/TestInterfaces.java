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


import org.bson.codecs.EncoderContext;
import org.junit.Test;
import org.mongodb.morphia.mapping.codec.DocumentWriter;
import org.mongodb.morphia.testmodel.Circle;
import org.mongodb.morphia.testmodel.ShapeShifter;

import static org.junit.Assert.assertEquals;

public class TestInterfaces extends TestBase {

    @Test
    public void testDynamicInstantiation() {
        getMapper().map(ShapeShifter.class/*, Circle.class, Rectangle.class*/);

/*
        final Rectangle rectangle = new Rectangle(2, 5);
        getDatastore().save(rectangle);

        assertEquals(rectangle, getDatastore().find(Rectangle.class)
                                              .filter("_id ==", rectangle.getId())
                                              .get());

*/
        final ShapeShifter shifter = new ShapeShifter();
/*
        shifter.setReferencedShape(getDatastore().find(Rectangle.class)
                                                 .filter("_id ==", rectangle.getId())
                                                 .get());
*/
        shifter.setMainShape(new Circle(2.2));
//        shifter.getAvailableShapes().add(new Rectangle(3, 3));
//        shifter.getAvailableShapes().add(new Circle(4.4));

        getCodecRegistry().get(ShapeShifter.class)
                          .encode(new DocumentWriter(), shifter, EncoderContext.builder().build());

        getDatastore().save(shifter);

        final ShapeShifter shifterLoaded = getDatastore().find(ShapeShifter.class)
                                                            .filter("_id ==", shifter.getId())
                                                            .get();
        assertEquals(shifter, shifterLoaded);
    }
}

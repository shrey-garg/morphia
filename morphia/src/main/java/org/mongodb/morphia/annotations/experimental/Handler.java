package org.mongodb.morphia.annotations.experimental;

import org.mongodb.morphia.mapping.experimental.PropertyHandler;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a special handler.
 *
 * This is an experimental feature whose API may shift without warning.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
@Documented
@Inherited
public @interface Handler {
    /**
     * @return the class definition of the handler
     */
    Class<? extends PropertyHandler> value();
}

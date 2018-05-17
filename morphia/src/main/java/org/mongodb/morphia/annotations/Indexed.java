package org.mongodb.morphia.annotations;


import org.mongodb.morphia.utils.IndexType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Specified on fields that should be Indexed.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Indexed {
    /**
     * @return "Direction" of the indexing.  Defaults to {@link IndexType#ASC}.
     * @see IndexType
     */
    IndexType value() default IndexType.ASC;

    /**
     * @return Options to apply to the index. Use of this field will ignore any of the deprecated options defined on {@link Index} directly.
     */
    IndexOptions options() default @IndexOptions();
}

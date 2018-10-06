package xyz.morphia;

import org.bson.types.ObjectId;
import org.hibernate.validator.constraints.Email;
import org.junit.Assert;
import org.junit.Test;
import xyz.morphia.annotations.Id;

import javax.validation.ValidationException;

public class TestMorphiaValidation extends TestBase {

    public static class E {
        @Id
        private ObjectId id;
        @Email
        private String email;
    }

    @Test
    public final void testPrePersist() {
        final E e = new E();
        e.email = "not an email";

        new ValidationExtension(getMorphia());

        try {
            getDs().save(e);
            Assert.fail("Should have failed validation");
        } catch (ValidationException exception) {
            // this is fine
        }

        e.email = "foo@bar.com";
        getDs().save(e);
    }
}

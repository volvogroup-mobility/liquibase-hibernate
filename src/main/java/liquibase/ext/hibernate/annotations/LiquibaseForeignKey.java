package liquibase.ext.hibernate.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LiquibaseForeignKey {

    boolean deferrable() default false;
    boolean initiallyDeferred() default false;
}


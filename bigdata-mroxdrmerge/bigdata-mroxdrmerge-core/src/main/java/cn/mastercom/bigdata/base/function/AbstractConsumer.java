package cn.mastercom.bigdata.base.function;

import java.util.Objects;

/**
 * Created by Kwong on 2018/7/13.
 */
public abstract class AbstractConsumer <T> {
    public abstract void accept(T var1);

    public AbstractConsumer<T> andThen(final AbstractConsumer<? super T> var1) {
        Objects.requireNonNull(var1);
        /*return (var2) -> {
            this.accept(var2);
            var1.accept(var2);
        };*/
        return new AbstractConsumer<T>() {
            @Override
            public void accept(T var2) {
                AbstractConsumer.this.accept(var2);
                var1.accept(var2);
            }
        };
    }
}

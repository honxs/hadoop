package cn.mastercom.bigdata.base.function;

import java.util.Objects;
//import java.util.function.Function;

/**
 * copy from java.util.function.Function from jdk8
 * 后续能使用java8时直接继承Function就能使用stream相关api，或者 使用并行流的Api 把container可分配的vcore利用起来
 * Created by Kwong on 2018/7/11.
 */
public abstract class AbstractFunction <T, R>/* implements Function*/{
    public abstract R apply(T var1);

    public <V> AbstractFunction<V, R> compose(final AbstractFunction<? super V, ? extends T> var1) {
        Objects.requireNonNull(var1);
        /*return (var2) -> {
            return this.apply(var1.apply(var2));
        };*/
        return new AbstractFunction<V, R>() {
            @Override
            public R apply(V var2) {
                return AbstractFunction.this.apply(var1.apply(var2));
            }
        };
    }

    public <V> AbstractFunction<T, V> andThen(final AbstractFunction<? super R, ? extends V> var1) {
        Objects.requireNonNull(var1);
       /* return (var2) -> {
            return var1.apply(this.apply(var2));
        };*/
       return new AbstractFunction<T, V>() {
           @Override
           public V apply(T var2) {
               return var1.apply(AbstractFunction.this.apply(var2));
           }
       };
    }

    public static <T> AbstractFunction<T, T> identity() {
        /*return (var0) -> {
            return var0;
        };*/
        return new AbstractFunction<T, T>() {
            @Override
            public T apply(T var0) {
                return var0;
            }
        };
    }
}

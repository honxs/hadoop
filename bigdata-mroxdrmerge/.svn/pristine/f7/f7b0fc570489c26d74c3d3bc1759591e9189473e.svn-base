package cn.mastercom.bigdata.base.function;

import java.util.Objects;
//import java.util.function.Predicate;

/**
 * copy from java.util.function.Predicate from jdk8
 * 后续能使用java8时直接继承Predicate就能使用stream相关api，或者 使用并行流的Api 把container可分配的vcore利用起来
 * Created by Kwong on 2018/7/11.
 */
public abstract class AbstractPredicate<T>  /*implements Predicate*/{
    /**
     * Evaluates this predicate on the given argument.
     *
     * @param t the input argument
     * @return {@code true} if the input argument matches the predicate,
     * otherwise {@code false}
     */
    public abstract boolean test(T t);

    /**
     * Returns a composed predicate that represents a short-circuiting logical
     * AND of this predicate and another.  When evaluating the composed
     * predicate, if this predicate is {@code false}, then the {@code other}
     * predicate is not evaluated.
     *
     * <p>Any exceptions thrown during evaluation of either predicate are relayed
     * to the caller; if evaluation of this predicate throws an exception, the
     * {@code other} predicate will not be evaluated.
     *
     * @param other a predicate that will be logically-ANDed with this
     *              predicate
     * @return a composed predicate that represents the short-circuiting logical
     * AND of this predicate and the {@code other} predicate
     * @throws NullPointerException if other is null
     */
    public AbstractPredicate<T> and(final AbstractPredicate<? super T> other) {
        Objects.requireNonNull(other);
//        return (t) -> test(t) && other.test(t);
        return new AbstractPredicate<T>() {
            @Override
            public boolean test(T t) {
                return AbstractPredicate.this.test(t) && other.test(t);
            }
        };
    }

    /**
     * Returns a predicate that represents the logical negation of this
     * predicate.
     *
     * @return a predicate that represents the logical negation of this
     * predicate
     */
    public AbstractPredicate<T> negate() {
//        return (t) -> !test(t);
        return new AbstractPredicate<T>() {
            @Override
            public boolean test(T t) {
                return !test(t);
            }
        };
    }

    /**
     * Returns a composed predicate that represents a short-circuiting logical
     * OR of this predicate and another.  When evaluating the composed
     * predicate, if this predicate is {@code true}, then the {@code other}
     * predicate is not evaluated.
     *
     * <p>Any exceptions thrown during evaluation of either predicate are relayed
     * to the caller; if evaluation of this predicate throws an exception, the
     * {@code other} predicate will not be evaluated.
     *
     * @param other a predicate that will be logically-ORed with this
     *              predicate
     * @return a composed predicate that represents the short-circuiting logical
     * OR of this predicate and the {@code other} predicate
     * @throws NullPointerException if other is null
     */
    public AbstractPredicate<T> or(final AbstractPredicate<? super T> other) {
        Objects.requireNonNull(other);
//        return (t) -> test(t) || other.test(t);
        return new AbstractPredicate<T>() {
            @Override
            public boolean test(T t) {
                return test(t) || other.test(t);
            }
        };
    }

    /**
     * Returns a predicate that tests if two arguments are equal according
     * to {@link Objects#equals(Object, Object)}.
     *
     * @param <T> the type of arguments to the predicate
     * @param targetRef the object reference with which to compare for equality,
     *               which may be {@code null}
     * @return a predicate that tests if two arguments are equal according
     * to {@link Objects#equals(Object, Object)}
     */
    public static <T> AbstractPredicate<T> isEqual(final Object targetRef) {
//        return (null == targetRef)
//                ? Objects::isNull
//                : object -> targetRef.equals(object);
        return new AbstractPredicate<T>() {
            @Override
            public boolean test(T object) {
                return (null == targetRef)
                ? object == null
                : targetRef.equals(object);
            }
        };
    }
}

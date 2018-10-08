package cn.mastercom.bigdata.base.function;

//import java.util.function.Supplier;

/**
 * copy from java.util.function.Supplier from jdk8
 * 后续能使用java8时直接继承Supplier就能使用stream相关api，或者 使用并行流的Api 把container可分配的vcore利用起来
 * Created by Kwong on 2018/7/11.
 */
public abstract class AbstractSupplier<T> /*implements Supplier*/{

    /**
     * Gets a result.
     *
     * @return a result
     */
    public abstract T get();
}

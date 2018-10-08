package cn.mastercom.bigdata.util.data;

/**
 * Created by kwong on 2017/11/24.
 */
public class Tuple2<A, B> {
    public final A first;
    public final B second;

    public Tuple2(A a, B b) {
        this.first = a;
        this.second = b;
    }
}

package cn.mastercom.bigdata.stat.impl.xdr;


import cn.mastercom.bigdata.base.function.AbstractPredicate;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;

/**
 * Created by Kwong on 2018/7/18.
 */
public class XdrPredicates {

    public static class EciPredicate extends AbstractPredicate<XdrDataBase> {

        @Override
        public boolean test(XdrDataBase xdr) {
            return xdr.ecgi > 0 && xdr.ecgi < Integer.MAX_VALUE;
        }
    }
}

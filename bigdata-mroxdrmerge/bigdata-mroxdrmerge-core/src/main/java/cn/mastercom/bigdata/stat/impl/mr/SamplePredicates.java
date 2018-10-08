package cn.mastercom.bigdata.stat.impl.mr;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.base.function.AbstractPredicate;

public class SamplePredicates {

    public static class EciPredicate extends AbstractPredicate<DT_Sample_4G>{

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.Eci > 0 && sample.Eci < Integer.MAX_VALUE;
        }
    }

    public static class GridPredicate extends AbstractPredicate<DT_Sample_4G>{

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.grid != null;
        }
    }

    public static class LngLatPredicate extends AbstractPredicate<DT_Sample_4G>{

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.ilongitude > 0 && sample.ilatitude > 0;
        }
    }

    public static class BuildidPredicate extends AbstractPredicate<DT_Sample_4G>{

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.ibuildingID > 0;
        }
    }

    public static class NCPredicate extends AbstractPredicate<DT_Sample_4G>{

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.tlte != null;
        }
    }

    public static class ConfidencePredicate extends AbstractPredicate<DT_Sample_4G>{

        int confidenceType;

        public ConfidencePredicate(int confidenceType){
            this.confidenceType = confidenceType;
        }

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.ConfidenceType == confidenceType;
        }
    }

    public static class SamStatePredicate extends AbstractPredicate<DT_Sample_4G>{

        int samState;

        public SamStatePredicate(int samState){
            this.samState = samState;
        }

        @Override
        public boolean test(DT_Sample_4G sample) {
            return sample.samState == samState;
        }
    }

    public static AbstractPredicate<DT_Sample_4G> and(Class<? extends AbstractPredicate>... classes) {

        AbstractPredicate<DT_Sample_4G> predicate = null;

        for (Class<? extends AbstractPredicate> clazz : classes) {

            try {
                if (predicate == null) {

                    predicate = (AbstractPredicate<DT_Sample_4G>) clazz.newInstance();

                }else{

                    predicate.and((AbstractPredicate<DT_Sample_4G>) clazz.newInstance());

                }
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return predicate;

    }
}

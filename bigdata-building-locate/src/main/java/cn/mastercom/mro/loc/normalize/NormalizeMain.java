package cn.mastercom.mro.loc.normalize;

import cn.mastercom.mro.loc.normalize.job.impl.MroLocNormalizeJob;

public class NormalizeMain {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.exit(-1);
        }

        MroLocNormalizeJob mroLocNormalizeJob = new MroLocNormalizeJob(args[0], args[1], args[2]);
        mroLocNormalizeJob.doJobAndStop();
    }
}

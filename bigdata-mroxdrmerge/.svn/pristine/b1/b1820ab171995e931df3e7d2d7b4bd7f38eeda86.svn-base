package cn.mastercom.bigdata.mapr.stat.userResident.homeBroadbandLoc;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.MergeUserResidentDeal;
import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.MergeUserResidentKey;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class MergeUserResidentReducer
{
	public static class MergeUserResidentReduce
			extends DataDealReducerV2<MergeUserResidentKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		private MergeUserResidentDeal mergeUserResidentDeal;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			MainModel.GetInstance().setConf(conf);
			mergeUserResidentDeal = new MergeUserResidentDeal(resultOutputer, conf);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void reduce(MergeUserResidentKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			mergeUserResidentDeal.deal(key, values);
		}
	}
}

package cn.mastercom.bigdata.mapr.stat.userResident;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.loc.userResident.ResidentUserLocDeal;
import cn.mastercom.bigdata.loc.userResident.UserResidentKey;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class UserResidentReducer
{
	public static class UserResidentReduce extends DataDealReducerV2<UserResidentKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		private ResidentUserLocDeal residentUserDeal;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			MainModel.GetInstance().setConf(conf);
			try
			{
				residentUserDeal = new ResidentUserLocDeal(resultOutputer, conf);
			}
			catch (Exception e)
			{
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void reduce(UserResidentKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			if (residentUserDeal != null)
			{
				residentUserDeal.deal(key, values);
			}
		}
	}
}

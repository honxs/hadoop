package cn.mastercom.bigdata.mapr.stat.imsifill;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.stat.imsifill.deal.ImsiIPKey;
import cn.mastercom.bigdata.stat.imsifill.deal.LocImsiFillDeal;

public class LocImsiFillReducer
{

	public static class StatReducer extends DataDealReducerV2<ImsiIPKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		public LocImsiFillDeal locImsiFillDeal;


		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			locImsiFillDeal = new LocImsiFillDeal(resultOutputer);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void reduce(ImsiIPKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			locImsiFillDeal.deal(key, values);
		}
	}
}

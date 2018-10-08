package cn.mastercom.bigdata.mapr.stat.villagestat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.village.MroVillageDeal;

public class VillageStatReducer
{

	public static class MroDataFileReducer extends DataDealReducerV2<GridTypeKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		private MroVillageDeal mroVillageDeal;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			MainModel.GetInstance().setConf(conf);
			mroVillageDeal = new MroVillageDeal(resultOutputer);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		@Override
		public void reduce(GridTypeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			mroVillageDeal.init();
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext())
			{
				String value = iterator.next().toString();
				mroVillageDeal.pushData(key.getDataType(), key.getTllong(), key.getTllat(), value);
			}
			mroVillageDeal.statData();
			mroVillageDeal.outData();
		}
	}
}

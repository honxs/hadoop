package cn.mastercom.bigdata.mapr.stat.eciFilter;

import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.util.ResultOutputer;
import java.io.IOException;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.eciFilter.EciFilterDeal;
import cn.mastercom.bigdata.stat.eciFilter.EciFilterKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class EciFilterReducer
{
	public static class EciFilterReduce extends DataDealReducerV2<EciFilterKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		private EciFilterDeal eciFilterDeal;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			MainModel.GetInstance().setConf(conf);
			eciFilterDeal = new EciFilterDeal(resultOutputer);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
		
		@Override
		protected void reduce(EciFilterKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			eciFilterDeal.deal(key, values);
		}
	}
}

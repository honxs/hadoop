package cn.mastercom.bigdata.mapr.stat.freqloc;

import java.io.IOException;

import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.stat.freqloc.FreqLocDeal;
import cn.mastercom.bigdata.stat.freqloc.FreqLocKey;

public class FreqLocReducer
{
	public static class FreqLocReduce extends DataDealReducerV2<FreqLocKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		private FreqLocDeal freqLocDeal;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			MainModel.GetInstance().setConf(conf);
			freqLocDeal = new FreqLocDeal(resultOutputer);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
		
		@Override
		protected void reduce(FreqLocKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			freqLocDeal.deal(key, values);
		}
	}
}

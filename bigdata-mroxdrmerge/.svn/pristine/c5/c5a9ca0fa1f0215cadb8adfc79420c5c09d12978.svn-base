package cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.buildIndoorCell.BuildIndoorCellDeal;
import cn.mastercom.bigdata.stat.userResident.buildIndoorCell.BuildIndoorCellKey;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;

public class BuildIndoorCellReducer
{
	public static class BuildIndoorCellReduce extends DataDealReducerV2<BuildIndoorCellKey, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		private BuildIndoorCellDeal buildIndoorCellDeal;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			MainModel.GetInstance().setConf(conf);
			try
			{
				buildIndoorCellDeal = new BuildIndoorCellDeal(resultOutputer, conf);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		protected void reduce(BuildIndoorCellKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			if (buildIndoorCellDeal != null)
			{
				buildIndoorCellDeal.deal(key, values);
			}
		}
	}
}

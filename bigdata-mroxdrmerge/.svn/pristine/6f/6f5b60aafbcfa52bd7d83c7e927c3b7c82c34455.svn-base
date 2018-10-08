package cn.mastercom.bigdata.mapr.mergestat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.mergestat.deal.MergeDataFactory;
import cn.mastercom.bigdata.mergestat.deal.MergeKey;

public class MergeStatReducer
{

	public static class StatReducer extends DataDealReducerV2<MergeKey, Text, NullWritable, Text>
	{
		private String[] strs;
		private ResultOutputer resultOutputer;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);

		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void reduce(MergeKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			IMergeDataDo mergeResult = null;
			IMergeDataDo dataTemp = null;
			try
			{
				dataTemp = MergeDataFactory.GetInstance().getMergeDataObject(key.getDataType());
			}
			catch (Exception e)
			{
				throw new InterruptedException("init data type error ");
			}

			for (Text value : values)
			{
				strs = value.toString().split("\t", -1);

				dataTemp.fillData(strs, 0);

				if (mergeResult == null)
				{
					mergeResult = dataTemp;
					try
					{
						dataTemp = MergeDataFactory.GetInstance().getMergeDataObject(key.getDataType());
					}
					catch (Exception e)
					{
						throw new InterruptedException("init data type error ");
					}
				}

				mergeResult.mergeData(dataTemp);
			}
			
			try
			{
				resultOutputer.pushData(key.getDataType(),mergeResult.getData());
			}
			catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}

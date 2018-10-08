package cn.mastercom.bigdata.mapr.xdr.loc.prepare;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.xdr.prepare.deal.XdrPrepareDeal;

public class XdrPrepareReducer
{

	public static class StatReducer extends DataDealReducerV2<Text, Text, NullWritable, Text>
	{
		private ResultOutputer resultOutputer;
		public XdrPrepareDeal xdrPrepareDeal;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);		
			resultOutputer = new ResultOutputer(mos);
			xdrPrepareDeal = new XdrPrepareDeal(resultOutputer);
		}

		/**
		 * Called once at the end of the task.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{	
			xdrPrepareDeal.init(key);
			int pushDataFlag = 0;
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext())
			{
				String value = iterator.next().toString();
				pushDataFlag = xdrPrepareDeal.pushData(XdrPrepareDeal.DataType_REDUCE, value);
				if (pushDataFlag == StaticConfig.FAILURE)
				{
					return;
				}
			}
			xdrPrepareDeal.statData();
			xdrPrepareDeal.outData();
		}

	}

}

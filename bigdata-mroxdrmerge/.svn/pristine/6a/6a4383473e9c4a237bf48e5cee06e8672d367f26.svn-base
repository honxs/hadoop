package cn.mastercom.bigdata.mapr.stat.userResident.buildIndoorCell;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.stat.userResident.buildIndoorCell.BuildIndoorCellKey;

public class BuildIndoorCellMapper
{

	public static class BuildIndoorCellMap extends Mapper<Object, Text, BuildIndoorCellKey, Text>
	{
		public BuildIndoorCellKey mapkey;
		public int cityId;
		public int buildId;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split("\t", -1);
			if (strs.length < 19)
			{
				return;
			}
			cityId = Integer.parseInt(strs[0]);
			buildId = Integer.parseInt(strs[10]);
			if(buildId > 0)
			{
				mapkey = new BuildIndoorCellKey(cityId ,buildId);
				context.write(mapkey, value);
			}
		}
	}

	public static class BuildIndoorCellPartitioner extends Partitioner<BuildIndoorCellKey, Text> implements Configurable
	{
		private Configuration conf = null;

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public void setConf(Configuration conf)
		{
			this.conf = conf;
		}

		@Override
		public int getPartition(BuildIndoorCellKey key, Text text, int reduceNum)
		{
			// TODO Auto-generated method stub
			return Math.abs(("" + key.getBuildId()).hashCode()) % reduceNum;
		}

	}

	public static class BuildIndoorCellSortKeyComparator extends WritableComparator
	{
		public BuildIndoorCellSortKeyComparator()
		{
			super(BuildIndoorCellKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			BuildIndoorCellKey s1 = (BuildIndoorCellKey) a;
			BuildIndoorCellKey s2 = (BuildIndoorCellKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class BuildIndoorCellSortKeyGroupComparator extends WritableComparator
	{

		public BuildIndoorCellSortKeyGroupComparator()
		{
			super(BuildIndoorCellKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			BuildIndoorCellKey s1 = (BuildIndoorCellKey) a;
			BuildIndoorCellKey s2 = (BuildIndoorCellKey) b;

			if (s1.buildId > s2.buildId)
			{
				return 1;
			}
			else if (s1.buildId < s2.buildId)
			{
				return -1;
			}
			else
			{
				return 0;
			}
		}
	}

}

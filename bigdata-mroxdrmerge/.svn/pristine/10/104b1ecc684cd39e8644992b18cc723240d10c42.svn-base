package cn.mastercom.bigdata.mapr.evt.locall;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.mastercom.bigdata.evt.locall.stat.ImsiKey;
import cn.mastercom.bigdata.evt.locall.stat.S1apidEciKey;


public class ShuffleUtils
{
	

	public static class ImsiPartitioner extends Partitioner<ImsiKey, Text> implements Configurable
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
		public int getPartition(ImsiKey key, Text value, int numOfReducer)
		{
			return Math.abs(("" + key.getImsi()).hashCode() % numOfReducer) ;
		}

	}

	/**
	 * 
	 * @author Administrator
	 *
	 */
	public static class ImsiSortKeyComparator extends WritableComparator
	{
		public ImsiSortKeyComparator()
		{
			super(ImsiKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			ImsiKey s1 = (ImsiKey) a;
			ImsiKey s2 = (ImsiKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class ImsiSortKeyGroupComparator extends WritableComparator
	{

		public ImsiSortKeyGroupComparator()
		{
			super(ImsiKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			ImsiKey s1 = (ImsiKey) a;
			ImsiKey s2 = (ImsiKey) b;

			if (s1.getImsi() > s2.getImsi())
			{
				return 1;
			}
			else if (s1.getImsi() < s2.getImsi())
			{
				return -1;
			}
			return 0;
		}
	}
	
	public static class s1apidImsiPartitioner extends Partitioner<S1apidEciKey, Text> implements Configurable
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
		public int getPartition(S1apidEciKey key, Text value, int numOfReducer)
		{
			return Math.abs(("" + key.getS1apid() + key.getEci()).hashCode() % numOfReducer);
		}

	}

	public static class S1apidEciSortKeyComparator extends WritableComparator
	{
		public S1apidEciSortKeyComparator()
		{
			super(S1apidEciKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			S1apidEciKey s1 = (S1apidEciKey) a;
			S1apidEciKey s2 = (S1apidEciKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class S1apidEciSortKeyGroupComparator extends WritableComparator
	{

		public S1apidEciSortKeyGroupComparator()
		{
			super(S1apidEciKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{

			S1apidEciKey s1 = (S1apidEciKey) a;
			S1apidEciKey s2 = (S1apidEciKey) b;

			if (s1.getS1apid() > s2.getS1apid())
			{
				return 1;
			}
			else if (s1.getS1apid() < s2.getS1apid())
			{
				return -1;
			}
			else
			{
				if (s1.getEci() > s2.getEci())
				{
					return 1;
				}
				else if (s1.getEci() < s2.getEci())
				{
					return -1;
				}
				return 0;
			}
		}
	}
	
}

package cn.mastercom.bigdata.mapr.stat.userResident;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.loc.userResident.ResidentUserLocDeal;
import cn.mastercom.bigdata.loc.userResident.UserResidentKey;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealMapperV2;

public class UserResidentMapper
{
	public static class UserResidentMap extends DataDealMapperV2<Object, Text, UserResidentKey, Text>
	{
		public UserResidentKey mapkey;
		public long eci;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
			Configuration conf = context.getConfiguration();
			MainModel.GetInstance().setConf(conf);
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split("\t", -1);
			if (strs.length < 18)
			{
				return;
			}
			eci = Long.parseLong(strs[7]);
			mapkey = new UserResidentKey(eci, ResidentUserLocDeal.DATATYPE_RESIDENT_USER);
			context.write(mapkey, value);
		}
	}

	public static class MroLocLibMap extends DataDealMapperV2<Object, Text, UserResidentKey, Text>
	{
		public UserResidentKey mapkey;
		public long eci;
		public String label;
		public String loctp;
		public int doorType;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context, MainModel.GetInstance().getAppConfig().getExcludeIpList());
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split("\t", -1);
			if (strs.length < 22)
			{
				return;
			}
			eci = Long.parseLong(strs[18]);
			
			doorType = Integer.parseInt(strs[9]);
			loctp = strs[11];
			label = strs[12];	//位置库运动状态标志
			mapkey = new UserResidentKey(eci, ResidentUserLocDeal.DATATYPE_MRO_LOC_LIB);
			if(("static").equals(label)) //要mro运动状态为static部分数据
			{
				context.write(mapkey, value);
			}
			else if(loctp.contains("fp") && doorType == StaticConfig.ACTTYPE_IN) //mro的loctp为fp室内部分数据
			{
				context.write(mapkey, value);
			}
		}
	}
	
	public static class XdrLocLibMap extends DataDealMapperV2<Object, Text, UserResidentKey, Text>
	{
		public UserResidentKey mapkey;
		public long eci;
		public String label;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split("\t", -1);
			if (strs.length < 22)
			{
				return;
			}
			eci = Long.parseLong(strs[18]);
			label = strs[12];	//位置库运动状态标志
			if(("static").equals(label)) //只要xdr运动状态为static部分数据
			{
				mapkey = new UserResidentKey(eci, ResidentUserLocDeal.DATATYPE_XDR_LOC_LIB);
				context.write(mapkey, value);
			}
		}
	}

	public static class UserResidentPartitioner extends Partitioner<UserResidentKey, Text> implements Configurable
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
		public int getPartition(UserResidentKey key, Text text, int reduceNum)
		{
			// TODO Auto-generated method stub
			return Math.abs(("" + key.getEci()).hashCode()) % reduceNum;
		}

	}

	public static class UserResidentSortKeyComparator extends WritableComparator
	{
		public UserResidentSortKeyComparator()
		{
			super(UserResidentKey.class, true);
		}

		@Override
		public int compare(Object a, Object b)
		{
			UserResidentKey s1 = (UserResidentKey) a;
			UserResidentKey s2 = (UserResidentKey) b;
			return s1.compareTo(s2);
		}

	}

	public static class UserResidentSortKeyGroupComparator extends WritableComparator
	{

		public UserResidentSortKeyGroupComparator()
		{
			super(UserResidentKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			UserResidentKey s1 = (UserResidentKey) a;
			UserResidentKey s2 = (UserResidentKey) b;

			if (s1.getEci() > s2.getEci())
			{
				return 1;
			}
			else if (s1.getEci() < s2.getEci())
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

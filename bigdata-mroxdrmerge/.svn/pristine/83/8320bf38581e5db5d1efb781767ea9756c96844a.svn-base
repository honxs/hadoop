package cn.mastercom.bigdata.mapr.stat.freqloc;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.mastercom.bigdata.stat.freqloc.FreqLocKey;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class FreqLocMapper
{
	public static class FreqLocMap extends Mapper<Object, Text, FreqLocKey, Text>
	{
		public FreqLocKey mapKey;
		StringBuffer sb = new StringBuffer();
		public int LteScRSRP;			//主服场强范围
		
		public int LteScRSRP_DX;		//电信主服场强
		public int LteScEarfcn_DX;		//电信主服频点
		public int LteScPci_DX;			//电信主服PCI
		public int Round_LteScPci_DX;	//除3圆整后的pci
		
		public int LteScRSRP_LT;		//联通主服场强
		public int LteScEarfcn_LT;		//联通主服频点
		public int LteScPci_LT;			//联通主服PCI
		public int Round_LteScPci_LT;	//除3圆整后的pci

		public int cityId;
		public int longitude;
		public int latitude;
		public int Time;
		public int ibuildingID;
		public static final String spliter = "\t";
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] strs = value.toString().split(spliter, -1);
			if(strs.length < 91)
			{
				return;
			}			
			
			cityId = Integer.parseInt(strs[0]);
			Time = Integer.parseInt(strs[1]);
			longitude = Integer.parseInt(strs[6]);
			if(longitude <= 0)
			{
				return;
			}
			latitude = Integer.parseInt(strs[7]);		
			ibuildingID = Integer.parseInt(strs[4]);
			LteScRSRP_DX = Integer.parseInt(strs[72]);
			LteScRSRP = MainModel.GetInstance().getAppConfig().getLteScRSRP();
			
			if(LteScRSRP_DX  >= LteScRSRP && LteScRSRP_DX != 0)
			{
				LteScEarfcn_DX = Integer.parseInt(strs[74]);
				
				LteScPci_DX = Integer.parseInt(strs[75]);
				Round_LteScPci_DX = LteScPci_DX / 3;
				mapKey = new FreqLocKey(cityId, LteScEarfcn_DX, Round_LteScPci_DX);
				sb.delete(0, sb.length());
				sb.append(longitude);
				sb.append(spliter);
				sb.append(latitude);
				sb.append(spliter);
				sb.append(ibuildingID);
				sb.append(spliter);
				sb.append(Time);
				sb.append(spliter);
				sb.append(LteScRSRP_DX);
				sb.append(spliter);
				sb.append(LteScPci_DX);
				context.write(mapKey, new Text(sb.toString()));
			}			
			
			LteScRSRP_LT = Integer.parseInt(strs[87]);
			if(LteScRSRP_LT >= LteScRSRP && LteScRSRP_LT != 0)
			{
				LteScEarfcn_LT = Integer.parseInt(strs[89]);
				LteScPci_LT = Integer.parseInt(strs[90]);
				Round_LteScPci_LT = LteScPci_LT / 3;
				mapKey = new FreqLocKey(cityId, LteScEarfcn_LT, Round_LteScPci_LT);
				sb.delete(0, sb.length());
				sb.append(longitude);
				sb.append(spliter);
				sb.append(latitude);
				sb.append(spliter);
				sb.append(ibuildingID);
				sb.append(spliter);
				sb.append(Time);
				sb.append(spliter);
				sb.append(LteScRSRP_LT);
				sb.append(spliter);
				sb.append(LteScPci_LT);
				context.write(mapKey, new Text(sb.toString()));
			}
			
		}		
	}
	
	public static class FreqLocPartitioner extends Partitioner<FreqLocKey, Text> implements Configurable
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
		public int getPartition(FreqLocKey key, Text text, int reduceNum)
		{
			// TODO Auto-generated method stub
			return Math.abs(("" + key.getFreq()).hashCode()) % reduceNum;
		}

	}
}

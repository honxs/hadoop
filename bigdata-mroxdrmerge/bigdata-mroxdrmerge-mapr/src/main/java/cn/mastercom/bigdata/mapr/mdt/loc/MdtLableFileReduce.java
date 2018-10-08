package cn.mastercom.bigdata.mapr.mdt.loc;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mapr.mro.loc.MroLableFileReducers;
import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;
import cn.mastercom.bigdata.util.hbase.HbaseDBHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class MdtLableFileReduce {
	protected static Logger LOG = Logger.getLogger(MroLableFileReducers.class);
	public static class MdtDataFileReducers extends DataDealReducerV2<CellTimeKey, Text, NullWritable, Text> 
	{
		private ResultOutputer resultOutputer;
		
		private MdtMmeDeal mdtmmeDeal;
		
		private Connection conn;
		private Configuration config;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			resultOutputer = new ResultOutputer(mos);
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)) 
			{
		        config = HBaseConfiguration.create();
				config.set("hbase.zookeeper.quorum", MainModel.GetInstance().getAppConfig().getZookeeperQuorum());
				config.set("hbase.zookeeper.property.clientPort", MainModel.GetInstance().getAppConfig().getClientPort() + "");
				config.set("zookeeper.znode.parent", MainModel.GetInstance().getAppConfig().getZnodeParent());
				LOGHelper.GetLogger().writeLog(LogType.info, "zookeeperQuorum is:" + config.get("hbase.zookeeper.quorum"));
				LOGHelper.GetLogger().writeLog(LogType.info, "zookeeperClientPort:" + config.get("hbase.zookeeper.property.clientPort"));
				LOGHelper.GetLogger().writeLog(LogType.info, "znodeParent is:" + config.get("zookeeper.znode.parent"));
				HbaseDBHelper hbaseDB = HbaseDBHelper.getInstance();
				conn =hbaseDB.getConnection(config);
				MainModel.GetInstance().setHbaseConf(config);
			}
			MainModel.GetInstance().setConf(conf);
			mdtmmeDeal = new MdtMmeDeal(resultOutputer);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.GuangXi2)) {
				if (conn != null) 
				{
					conn.close();
				}
			}
			try
			{
				LOGHelper.GetLogger().writeLog(LogType.info, "begin	 cleanup:");
				LOGHelper.GetLogger().writeLog(LogType.info, "end cleanup:");
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"output data error", "output data error ", e);
			}

			super.cleanup(context);
		}

		@Override
		public void reduce(CellTimeKey key, Iterable<Text> values, Context context) {
			mdtmmeDeal.init(key);
			int pushDataFlag = 0;
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext())
			{
				String value = iterator.next().toString();
				
				try
				{
					pushDataFlag = mdtmmeDeal.pushData(key, value);
					if (pushDataFlag == StaticConfig.FAILURE)
					{
						return;
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			
//			mdtmmeDeal.statData();
//			mdtmmeDeal.outData();

		}
		
		
		
	}
}

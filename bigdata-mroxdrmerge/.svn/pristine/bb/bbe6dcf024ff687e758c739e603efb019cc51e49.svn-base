package cn.mastercom.bigdata.mapr.evt.locall;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import cn.mastercom.bigdata.evt.locall.stat.BinarySearchJoin;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.evt.locall.model.XdrDataFactory;
import cn.mastercom.bigdata.evt.locall.stat.LocItem;
import cn.mastercom.bigdata.evt.locall.stat.S1apidEciKey;
import cn.mastercom.bigdata.evt.locall.stat.XdrLocallexDeal2;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.hadoop.mapred.DataDealReducerV2;

public class LocAllReducer_S1apidEci {
	public static class StatReducer_S1apidEci extends DataDealReducerV2<S1apidEciKey, Text, NullWritable, Text> {
		private XdrLocallexDeal2 xdrLocallexDeal;
		// 用来判断上一行数据是否是位置库， TODO 换一个方式20180701
		private boolean pre_isLoc = true;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 得到Main传过来的conf,加载日志构造器，multiOutPut等设置
			super.setup(context);
			MainModel.GetInstance().setConf(conf);
			ResultOutputer resultOutputer = new ResultOutputer(mos);
			xdrLocallexDeal = new XdrLocallexDeal2(resultOutputer);
			//Procedure_Status成功的标识，默认是0
			String successValue = MainModel.GetInstance().getAppConfig().getSuccessProdoceStatusValue();
			StaticConfig.successProdoceStatusValue = Integer.parseInt(successValue);
            // 得到运行时传入的时间，在事件表输出的时候，会将这个时间输出
			String thestrTime = context.getConfiguration().get("mapreduce.job.date");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyMMdd");
			Date date = null;
			try {
				date = simpleDateFormat.parse(thestrTime.substring(3, 9));
				XdrLocallexDeal2.ROUND_dAY_TIME = (int) (date.getTime() / 1000L);
			} catch (ParseException e) {
				LOGHelper.GetLogger().writeLog(LogType.error,"evt_s1apid reduce dateFormat error","evt_s1apid reduce dateFormat error",e);
			}
            // 加载工参表，后续通过eci得到cityID会用上
			if (!CellConfig.GetInstance().loadLteCell(conf)) 
            {
				LOGHelper.GetLogger().writeLog(LogType.error,
						"ltecell init error 请检查！" + CellConfig.GetInstance().errLog);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			xdrLocallexDeal.xdrStatConsumers.flush();
			xdrLocallexDeal.dataStater.outResult();
			super.cleanup(context);
		}
		@Override
		public void reduce(S1apidEciKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			xdrLocallexDeal.init(key);
			//清空一下之前的位置库数据
			BinarySearchJoin.clear();
			xdrLocallexDeal.immLocItemMap = new HashMap<>();
			int pushDataFlag = 0;
			Iterator<Text> iterator = values.iterator();
			// 记录xdr数据的条数
			int count = 0;
			while (iterator.hasNext()) {
				String value = iterator.next().toString();
				/**
				 * 一个imsi的数据，如果超过10W条，那么当作异常数据
				 * 同理，如果某15分钟内的数据超过指定条数，在经纬度回填的时候，也当作异常数据
				 * @see LocItemMng.fillLoc
				 */
				if ((key.getDataType() != XdrDataFactory.LOCTYPE_XDRLOC) && (key.getDataType() != XdrDataFactory
						.LOCTYPE_MRLOC)) {
					count++;
				}
                // xdrData条数大于10万条，则当作是异常数据，但是内蒙需要输出原数据的结果
				if (count > 100000) {
					if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng)) {
						LocAllExUtil.neimengPushCPEData(xdrLocallexDeal, key.getDataType(), value);
						continue;
					} else {
						return;
					}
				}

				pushDataFlag = xdrLocallexDeal.pushData(key.getDataType(), value);
				if (pushDataFlag == StaticConfig.FAILURE) {
					return;
				}
			}
			xdrLocallexDeal.statData();
			xdrLocallexDeal.outData();

		}

	}
}

package cn.mastercom.bigdata.evt.locall.stat;

import java.util.*;
import java.util.Map.Entry;

import cn.mastercom.bigdata.base.function.StatConsumer;
import cn.mastercom.bigdata.evt.locall.model.*;
import cn.mastercom.bigdata.stat.impl.xdr.XdrStatConsumers;
import cn.mastercom.bigdata.util.*;
import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.evt.locall.model.StatVideoLag;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.evt.locall.model.XdrDataFactory;
import cn.mastercom.bigdata.evt.locall.model.XdrData_Mme;
import cn.mastercom.bigdata.loc.hsr.HSRConfig;
import cn.mastercom.bigdata.loc.hsr.LocFunc;
import cn.mastercom.bigdata.loc.hsr.PositionModel;
import cn.mastercom.bigdata.mroxdrmerge.AppConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.GisFunction;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.xdr.loc.ResidentUser;

public class XdrLocallexDeal2 implements IDataDeal
{

	public ResultOutputer resultOutputer;

	private HashMap<String,ResidentUser> residentUserMap; // key为 hours+"_"+eci
	public HashMap<Integer,List<ImmLocItem>> immLocItemMap; //imm的位置库,key为时间 秒，ImmLocItem为类
	private int tmpDataType = -1;
	private XdrDataBase tmpItem;
	private Object key;
	// 当前行数据的解析器
	private ParseItem curParseItem = null;
	// 当前行数据的解析器
	private DataAdapterReader curDataAdapterReader = null;
	// 用来统计和输出的controller对象
	public DataStater dataStater = null;
	// 事件详单吐出的StringBuffer
	private StringBuffer tmsb = new StringBuffer();
	public static HashMap<String, String> ImeiMobileTypeMap = new HashMap<>();//手机型号识别
	// 用户小区统计, key为eci，value为count值
	private HashMap<Long, Integer> userCellMaps;
	private long curImsi = -1;
	private long curS1apid = -1;
	private long curEci = -1;
	// 保存的所有xdr数据，key为dataType，value为xdr的list数据
	HashMap<Integer, ArrayList<XdrDataBase>> allDataMap = new HashMap<>();
	//用来保存xdr数据
	ArrayList<XdrDataBase> dataList;
    // 运行是传入的时间所转换成的时间戳
	public static int ROUND_dAY_TIME;

	// 高铁配置
	HSRConfig hsrConfig;

	//Xdr统计
	public XdrStatConsumers xdrStatConsumers;

	public ResultOutputer getResultOutputer()
	{
		return resultOutputer;
	}

	public XdrLocallexDeal2(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
		StaticConfig.putCityNameByCityId();
		dataStater = new DataStater(resultOutputer);

		// 20171129 add 加载高铁配置
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2))
		{
			Configuration conf = MainModel.GetInstance().getConf();
			
			String date = conf.get("mapreduce.job.date").replace("01_", "");
			AppConfig appConfig = MainModel.GetInstance().getAppConfig();
			String baseOutpath = appConfig.getMroXdrMergePath();
			hsrConfig = HSRConfig.GetInstance();
			if (!hsrConfig.initDailyCfg(conf, baseOutpath, date) || !hsrConfig.initGerneralCfg(conf,
					appConfig.getHsrSegmentPath(), appConfig.getHsrSectionPath(), appConfig.getHsrSectionCellPath(),
					appConfig.getHsrStationCellPath(), appConfig.getHsrStationPath()))
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "高铁车次配置init error 请检查！");
				hsrConfig = null;
			} else
			{
				// 加载小区工参， 用于判断高铁定位后距离是否过远
				CellConfig.GetInstance().loadLteCell(conf);
			}
		}

		xdrStatConsumers = new XdrStatConsumers(resultOutputer);

	}

	/**
	 * 根据key的类型不同，给 curImsi 赋值或者 curS1apid和curEci赋值
	 * @param key
	 * ImsiKey or S1apidKey
	 */
	public void init(Object key)
	{
		if (key instanceof ImsiKey)
		{
			curImsi = ((ImsiKey) key).getImsi();
			residentUserMap = new HashMap<String,ResidentUser>();
		}
		else
		{
			curS1apid = ((S1apidEciKey) key).getS1apid();
			curEci = ((S1apidEciKey) key).getEci();
		}
		this.key = key;
		tmpItem = null;
		curParseItem = null;
		curDataAdapterReader = null;
		userCellMaps = new HashMap<>();
		tmpDataType = -1;
		allDataMap = new HashMap<>();
		dataList = new ArrayList<>();//
	}

	/**
	 * 统计视频卡顿的指标 只需要吐出event，没有detail
	 * 
	 * @param xdrDataBaseList
	 *            HTTP数据的List
	 */
	private void stateVidelLag(ArrayList<XdrDataBase> xdrDataBaseList)
	{
		StatVideoLag statVideoLag = new StatVideoLag();

		ArrayList<EventData> eventDataListAll = statVideoLag.statVideoLag(xdrDataBaseList);
		if (eventDataListAll != null)
		{
			for (EventData eventData : eventDataListAll)
			{
				if (eventData.eventStat != null)
				{
					dataStater.stat(eventData);
				}
			}
		}
	}

	/**
	 * 对http的数据进行页面浏览指标的统计，并且输出
	 * 
	 * @param xdrDataBaseList
	 *            HTTP的实现类
	 */
	public void statHttpPage(ArrayList<XdrDataBase> xdrDataBaseList)
	{
		HttpPageDeal.dataStater = dataStater;
		ArrayList<EventData> eventDataListAll = HttpPageDeal.deal(xdrDataBaseList);
		if (eventDataListAll != null)
		{
			for (EventData eventData : eventDataListAll)
			{
				if (eventData.eventStat != null)
				{
					dataStater.stat(eventData);
				}
				// 吐出详单，北京用来对比数据用的
				if (eventData.eventDetial != null)
				{
					outEventData(eventData);
				}
			}
		}
	}

	public void outXdrData(int dataType, XdrDataBase item)
	{
		tmsb.delete(0, tmsb.length());
		item.toString(tmsb);

		try
		{
			if (tmsb.length() > 0)
			{
				resultOutputer.pushData(dataType, tmsb.toString());
			}
		} catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"XdrLocallexDeal2 out xdr data error1", "out xdr data error" +
					".",e);
		}

	}

	public void outXdrData(int dataType, ArrayList<XdrDataBase> xdrDataList)
	{
		for (XdrDataBase item : xdrDataList)
		{
			tmsb.delete(0, tmsb.length());
			item.toString(tmsb);

			try
			{
				if (tmsb.length() > 0)
				{
					resultOutputer.pushData(dataType, tmsb.toString());
				}
			} catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"XdrLocallexDeal2 out xdr data error", "XdrLocallexDeal2 out xdr data error.",e);
			}
		}

	}

	/**
	 * 吐出详单，根据定位到的confidentType来进行详单的吐出
	 * 
	 * @param item
	 */
	public void outEventData(EventData item)
	{
		tmsb.delete(0, tmsb.length());
		item.toString(tmsb);

		try
		{

			if (item.confidentType == StaticConfig.IH)
			{
				if (item.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.IHSAMPLE.getIndex(), tmsb.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.IHSAMPLEALL.getIndex(), tmsb.toString());

			} else if (item.confidentType == StaticConfig.IM)
			{
				if (item.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.IMSAMPLE.getIndex(), tmsb.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.IMSAMPLEALL.getIndex(), tmsb.toString());

			} else if (item.confidentType == StaticConfig.IL)
			{
				if (item.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.ILSAMPLE.getIndex(), tmsb.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.ILSAMPLEALL.getIndex(), tmsb.toString());

			} else if (item.confidentType == StaticConfig.OH)
			{
				if (item.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.OHSAMPLE.getIndex(), tmsb.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.OHSAMPLEALL.getIndex(), tmsb.toString());

			} else if (item.confidentType == StaticConfig.OM)
			{
				if (item.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.OMSAMPLE.getIndex(), tmsb.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.OMSAMPLEALL.getIndex(), tmsb.toString());

			} else
			{
				if (item.eventDetial.sampleType == StaticConfig.FAILURE_SAMPLE)
				{
					resultOutputer.pushData(TypeIoEvtEnum.OLSAMPLE.getIndex(), tmsb.toString());
				}

				resultOutputer.pushData(TypeIoEvtEnum.OLSAMPLEALL.getIndex(), tmsb.toString());

			}

		} catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"XdrLocallExDeal2.outDataError","XdrLocallExDeal2.outDataError: ",e);
		}

	}

	/**
	 * 对mme数据统计重定向指标，并且输出到详单
	 * 
	 * @param xdrDataBaseList
	 *            MME数据的List
	 */
	public void statRedirect(ArrayList<XdrDataBase> xdrDataBaseList)
	{
		boolean redirectFlag = false;
		EventData curEventData = null;
		for (XdrDataBase xdrDataBase : xdrDataBaseList)
		{
			XdrData_Mme xdrDataMme = (XdrData_Mme) xdrDataBase;

            // 有的城市是request是284也当成28
            if (xdrDataMme.Request_Cause == 284)
            {
                xdrDataMme.Request_Cause = 28;
            }

			// if (xdrdata_mme.Procedure_Type == 20 &&
			// xdrdata_mme.Request_Cause == 28)
			if (xdrDataMme.Procedure_Type == 20 && xdrDataMme.Request_Cause == 28 && !"".equals(xdrDataMme.MSISDN)
					&& !"".equals(xdrDataMme.imsi) && xdrDataMme.istime > 0)
			{

				redirectFlag = true;
				curEventData = xdrDataMme.toRedirectEventData();
			} // else if (redirectFlag && xdrdata_mme.Procedure_Status == 0
				// && curEventData != null)
			else if (redirectFlag && (xdrDataMme.Procedure_Type == 1 || xdrDataMme.Procedure_Type == 5)
					&& !"".equals(xdrDataMme.MSISDN) && curEventData != null && xdrDataMme.istime > 0)
			{
				redirectFlag = false;

				// 加一个了
				curEventData.eventDetial.fvalue[4] = xdrDataMme.Cell_ID;
				curEventData.eventDetial.fvalue[5] = xdrDataMme.istime * 1000L + xdrDataMme.istimems;
				curEventData.eventDetial.fvalue[6] = curEventData.eventDetial.fvalue[5]
						- curEventData.eventDetial.fvalue[3];

				outEventData(curEventData);

                // 吐出高铁事件
                if (curEventData.iTestType == StaticConfig.TestType_HiRail)
                {
                    try
                    {
                        tmsb.delete(0, tmsb.length());
                        int rtnCode = curEventData.toHsrString(tmsb);
                        if (rtnCode == 0)
                        {// 成功
                            resultOutputer.pushData(TypeIoEvtEnum.HSRSAMPLE.getIndex(), tmsb.toString());
                        }
                    } catch (Exception e)
                    {
						LOGHelper.GetLogger().writeLog(LogType.error,"HiRailOutSampleDataError","HiRailOutSampleDataError: ",e);
                    }
                }


				// 把这个kpiset改成2.再统计eventstat中重定向的次数！
				curEventData.iKpiSet = 2;
				dataStater.stat(curEventData);
			}
		}
	}

	/**
	 * <p>
	 * 1. 解析tb_loc,xdr_loc位置库数据并且保存
	 * 2. 解析常驻用户位置库数据并且保存
	 * 3. 解析xdr数据并且保存
	 */
	@Override
	public int pushData(int dataType, String value)
	{
		/* 保存xdrLoc,tbLoc位置库数据 */
		if (dataType == XdrDataFactory.LOCTYPE_XDRLOC || dataType == XdrDataFactory.LOCTYPE_MRLOC)
		{
			BinarySearchJoin.pushData(value);
		}
		/* 保存常驻用户位置库数据 */
		else if(dataType == XdrDataFactory.LOCTYPE_RESIDENTLOC){
			if (curImsi > 0){
				String[] strs = value.toString().split(ResidentUser.spliter, -1);
				ResidentUser residentUser = new ResidentUser();
				residentUser.fillMergeData(strs);
				residentUserMap.put(residentUser.hour+"_"+residentUser.eci, residentUser);

			}
		}
		/* 解析xdr数据并且保存*/
		else
		{
			/*当dataType变化后，根据dataType实例化相应的对象*/
			if (tmpDataType != dataType)
			{
				try
				{
					tmpItem = XdrDataFactory.GetInstance().getXdrDataObject(dataType);
					dataList = new ArrayList<XdrDataBase>();

					allDataMap.put(dataType, dataList);
					tmpDataType = dataType;
				} catch (Exception e)
				{
                    LOGHelper.GetLogger().writeLog(LogType.error,"get XdrDataBase from XdrDataFactory error","get XdrDataBase from XdrDataFactory error: " +
                            "DataType is: "+dataType+" ",e);
				}
			}
            //解析xdr数据
			XdrDataBase xdrDataItem = null;
			try
			{
				curParseItem = tmpItem.getDataParseItem();
				curDataAdapterReader = new DataAdapterReader(curParseItem);
				xdrDataItem = XdrDataFactory.GetInstance().getXdrDataObject(dataType);
				String[] strs = value.toString().split(curParseItem.getSplitMark(), -1);
				curDataAdapterReader.readData(strs);
				if (!xdrDataItem.FillData(curDataAdapterReader))
				{
					LOGHelper.GetLogger().writeLog(LogType.error, "xdrdata fill data error :" + value.toString());
				}

				// 对xdrDataItem的imsi进行加密
				xdrDataItem.noEntryImsi = xdrDataItem.imsi;
				if(MainModel.GetInstance().getCompile().Assert(CompileMark.Encrypt)){
					xdrDataItem.imsi = Func.getEncrypt(xdrDataItem.imsi);
				}

				//如果是mdt:imm1，组成一个key,time+数据
				if(dataType==TypeIoEvtEnum.ORIGIN_MDT_IMM1.getIndex()){
                    collectImmLocMap(xdrDataItem);
				}
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error, "xdrdata fill data error","xdrdata fill data error value is:" + value.toString()
                        ,e);
			}
			dataList.add(xdrDataItem);
		}
		return StaticConfig.SUCCESS;
	}

    /**
     * imm1的位置库保存到数据中
     * @param xdrDataItem
     */
	private void collectImmLocMap(XdrDataBase xdrDataItem) {
		MdtImm1 mdtImm1 = (MdtImm1) xdrDataItem;
		ImmLocItem immLocItem = new ImmLocItem(mdtImm1.istime,mdtImm1.iLongitude,
                mdtImm1.iLatitude, mdtImm1.confidentType);
		if(immLocItemMap!=null){
            int theTime = mdtImm1.istime/10*10;
		    if(immLocItemMap.containsKey(theTime)){
                immLocItemMap.get(theTime).add(immLocItem);
            }else{
                List<ImmLocItem> immLocItems = new ArrayList<>();
                immLocItems.add(immLocItem);
                immLocItemMap.put(theTime,immLocItems);
            }
        }
	}

	/**
	 * 1. xdr_loc和tb_loc定位：可选
	 * 2. 高铁定位： 可选
	 * 3. 常驻用户定位： 可选
	 * 4. IMM1给IMM4定位：可选
	 * 5. 将原始数据得到事件，然后进行统计，输出等
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void statData()
	{
		try
		{
			/* 有xdr数据的时候，位置库的数据才开始进行排序 */
			if(allDataMap.size()>0){
				BinarySearchJoin.sortLocItemLists();
			}
			/*给所有的事件原始数据和位置库关联，进行经纬度回填*/
			if (BinarySearchJoin.locItemLists.size()>0)
			{
				// key:datatype
				for (Integer key : allDataMap.keySet())
				{
					ArrayList<XdrDataBase> records = allDataMap.get(key);
					BinarySearchJoin.fillLoc(records);
				}

			}

			// 加入高铁定位
			// 内蒙山西不需要高铁定位
			if (MainModel.GetInstance().getCompile().Assert(CompileMark.HiRail2) && hsrConfig != null
					//按imsi分组的话 只需要判断一遍
					&& (this.curImsi <= 0 || (this.curImsi > 0 && hsrConfig.dicImsiData.containsKey(curImsi)))
			)
			{
				LocFunc hsrLocFunc = new LocFunc(hsrConfig);
				for (ArrayList<XdrDataBase> xdrList : allDataMap.values())
				{
					hsrLocFixed(hsrLocFunc, xdrList);
				}

			}

			/* 常驻用户回填 */
            if(residentUserMap!=null && residentUserMap.size()>0){
                residentUserFillLoc();
            }
            /* IMM1给IMM4定位 */
            if (immLocItemMap != null)
            {
                // 先给位置库排序
                for (Integer key : immLocItemMap.keySet()) {
                    Collections.sort(immLocItemMap.get(key));
                }
                //对数据IMM4数据进行定位
                for (Integer key : allDataMap.keySet())
                {
                    if(key==TypeIoEvtEnum.ORIGIN_MDT_IMM4.getIndex()
                            || key==TypeIoEvtEnum.ORIGIN_MDT_IMM5.getIndex()){
                        ArrayList<XdrDataBase> xdrDataBases = allDataMap.get(key);
                        for (XdrDataBase xdrDataBase : xdrDataBases) {
                            if(immLocItemMap.containsKey(xdrDataBase.istime/10*10)){
                                ArrayList<ImmLocItem> allItemLists = new ArrayList<>();
                                List<ImmLocItem> immLocItems0 = immLocItemMap.get((xdrDataBase.istime-10) / 10 * 10);
                                List<ImmLocItem> immLocItems1 = immLocItemMap.get(xdrDataBase.istime / 10 * 10);
                                List<ImmLocItem> immLocItems2 = immLocItemMap.get((xdrDataBase.istime+10) / 10 * 10);
                               if(immLocItems0!=null){
                                   allItemLists.addAll(immLocItems0);
                               }
                               allItemLists.addAll(immLocItems1);
                               if(immLocItems2!=null){
                                   allItemLists.addAll(immLocItems2);
                               }
                                //二分查找index
                                int index = binarySearch(allItemLists,xdrDataBase.istime,0,allItemLists.size()-1);
                                ImmLocItem immLocItem = allItemLists.get(index);
                                xdrDataBase.iLongitude = immLocItem.longitude;
                                xdrDataBase.iLatitude = immLocItem.latitude;
                                if(immLocItem.confidentType>0){
                                    xdrDataBase.confidentType = immLocItem.confidentType;
                                }
                            }
                        }
                    }
                }
            }

			// 天津去重,volte进来的肯定是同一个imsi的，所以只要时间一样，就算重复了
			if(MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)){

				if(allDataMap.containsKey(TypeIoEvtEnum.ORIGINMW.getIndex())){
					ArrayList<XdrDataBase> mwLists = allDataMap.get(TypeIoEvtEnum.ORIGINMW.getIndex());
                    duplicateRemove(mwLists);

                }else if(allDataMap.containsKey(TypeIoEvtEnum.ORIGINSV.getIndex())){
					ArrayList<XdrDataBase> svLists = allDataMap.get(TypeIoEvtEnum.ORIGINSV.getIndex());
                    duplicateRemove(svLists);
				} else if(allDataMap.containsKey(TypeIoEvtEnum.ORIGINRX.getIndex())){
					ArrayList<XdrDataBase> rxLists = allDataMap.get(TypeIoEvtEnum.ORIGINRX.getIndex());
                    duplicateRemove(rxLists);
                }

			}



			/* 页面统计和视频统计 */
			if (allDataMap.containsKey(TypeIoEvtEnum.ORIGINHTTP.getIndex())
					&& !MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
			{
				// 页面浏览指标需要对http的数据按照时间进行排序
				Collections.sort(allDataMap.get(TypeIoEvtEnum.ORIGINHTTP.getIndex()));

				statHttpPage(allDataMap.get(TypeIoEvtEnum.ORIGINHTTP.getIndex()));

				if (MainModel.GetInstance().getCompile().Assert(CompileMark.BeiJing))
				{
					stateVidelLag(allDataMap.get(TypeIoEvtEnum.ORIGINHTTP.getIndex()));
				}

			}
			// mme重定向的详单输出, 目前只有北京才吐出，所以加了个北京的判断
			if (allDataMap.containsKey(TypeIoEvtEnum.ORIGINMME.getIndex())
					&& MainModel.GetInstance().getAppConfig().get23GRedirect().equals("true"))
			{
				Collections.sort(allDataMap.get(TypeIoEvtEnum.ORIGINMME.getIndex()));

				statRedirect(allDataMap.get(TypeIoEvtEnum.ORIGINMME.getIndex()));
			}


		} catch (Exception e)
		{
			LOGHelper.GetLogger().writeLog(LogType.error,"statData Exception: ", "statData Exception: "+e.getMessage()
                    ,e);
		}

		// 内蒙对原数据加上定位信息后输出
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			for (Entry<Integer, ArrayList<XdrDataBase>> entry : allDataMap.entrySet())
			{
				// 输出源数据，加上定位信息,RX的不用输出
                if(entry.getKey()!=TypeIoEvtEnum.ORIGINRX.getIndex()){
                    outXdrData(entry.getKey(), entry.getValue());
                }
			}
//			return;
		}

		for (ArrayList<XdrDataBase> xdrDataList : allDataMap.values())
		{

			for (XdrDataBase xdrData : xdrDataList)
			{
									/* 通过与工参表的eci关联得到cityID,
					TODO zhaikaishun 20180704 天津不能写死1201，数据有问题应该去查证
					天津部分ECI有问题直接赋值1201 */
				LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(xdrData.ecgi);
				if (lteCellInfo != null)
				{
					xdrData.iCityID = lteCellInfo.cityid;
				}
				if(MainModel.GetInstance().getCompile().Assert(CompileMark.TianJin)){
					xdrData.iCityID = 1201;
				}


				//增加 xdr 统计
				xdrStatConsumers.accept(xdrData);

				// 数据运算
				ArrayList<EventData> eventDataList = xdrData.toEventData();

				if (eventDataList == null)
				{
					continue;
				}

				for (EventData eventData : eventDataList)
				{

					// 吐出高铁事件
					if (eventData.iTestType == StaticConfig.TestType_HiRail)
					{
						try
						{
							tmsb.delete(0, tmsb.length());
							int rtnCode = eventData.toHsrString(tmsb);
							if (rtnCode == 0)
							{// 成功
								resultOutputer.pushData(TypeIoEvtEnum.HSRSAMPLE.getIndex(), tmsb.toString());
							}
						} catch (Exception e)
						{
                            LOGHelper.GetLogger().writeLog(LogType.error,"HiRailOutSampleDataError:","HiRailOutSampleDataError: ",e);
						}
					}

					// 小区用户统计
					if (eventData.eventStat != null && eventData.Interface == StaticConfig.INTERFACE_S1_U
							&& eventData.iKpiSet == 4)
					{
						if (userCellMaps.containsKey(eventData.iEci))
						{
							eventData.eventStat.fvalue[3] = 0;
						} else if (eventData.eventStat.fvalue[3] == 1)
						{
							userCellMaps.put(eventData.iEci, 1);
						}
					}
					// 事件统计
					if (eventData.eventStat != null)
					{
						dataStater.stat(eventData);
					}
					// 详单的吐出
					if (eventData.eventDetial != null)
					{
						outEventData(eventData);
					}
				}
			}
		}
	}

    /**
     * 按照时间相同的进行去重
     * @param mwLists
     */
    private void duplicateRemove(ArrayList<XdrDataBase> mwLists) {
        Collections.sort(mwLists);
        int preTime = -1;
        Iterator<XdrDataBase> iterator = mwLists.iterator();
        while (iterator.hasNext()){
            XdrDataBase next = iterator.next();
            if (next.istime==preTime) {
                iterator.remove();
            }
            preTime = next.istime;
        }
    }

    /**
     *  递归二分，应该使用循环
     * @param allItemLists
     * @param istime
     * @param begin
     * @param end
     * @return
     */
    private int binarySearch(ArrayList<ImmLocItem> allItemLists, int istime,int begin,int end) {
	    if(begin==end){
	        return begin;
        }
	    if((end-begin)==1){
	        return Math.abs(istime-allItemLists.get(begin).time)>Math.abs(allItemLists.get(end).time-istime)?end:begin;
        }
	    int mid = (begin+end+1)/2;
        if(istime>allItemLists.get(mid).time){
            return binarySearch(allItemLists,istime,mid,end);
        }else if(istime<allItemLists.get(mid).time){
             return binarySearch(allItemLists,istime,begin,mid);
        }else{
            return mid;
        }

	}

    /**
     * 常驻用户回填
     */
    private void residentUserFillLoc() {
        for (Integer key : allDataMap.keySet())
        {
            ArrayList<XdrDataBase> xdrLists = allDataMap.get(key);
            for(XdrDataBase xdrdata:xdrLists){
                if(xdrdata.iLatitude<=0){
                    String residentUserKey = new Date(xdrdata.getIstime()).getHours()+"_"+xdrdata.ecgi;
                    if(residentUserMap.containsKey(residentUserKey)){

                        ResidentUser residentUser = residentUserMap.get(residentUserKey);
                        xdrdata.iLongitude = (int)residentUser.longitude;
                        xdrdata.iLatitude = (int)residentUser.latitude;
                        xdrdata.ibuildid = residentUser.buildId;
                        xdrdata.iheight = residentUser.height;
                        xdrdata.position = residentUser.position;
                        xdrdata.confidentType = Func.getSampleConfidentType(residentUser.locSource,
                                StaticConfig.ACTTYPE_IN,
                                StaticConfig.TestType_CQT);
						xdrdata.strloctp = "ru";
                    }
                }
            }
        }
    }

    @Override
	public void outData()
	{
		// TODO Auto-generated method stub

	}

	private void hsrLocFixed(LocFunc hsrLocFunc, List<XdrDataBase> xdrList)
	{
		for (XdrDataBase xdrItem : xdrList)
		{
			PositionModel position = hsrLocFunc.FindPosition(xdrItem.noEntryImsi, xdrItem.istime, xdrItem.ecgi, xdrItem.imei);
			if (position != null)
			{
				int lng = (int) (position.lng * 10000000);// double to int
				int lat = (int) (position.lat * 10000000);
				if (notTooFar(lng, lat, xdrItem))
				{
					xdrItem.testType = StaticConfig.TestType_HiRail;
					xdrItem.iLongitude = lng;
					xdrItem.iLatitude = lat;
					xdrItem.trainKey = position.trainKey;
					xdrItem.sectionId = position.sectionid;
					xdrItem.segmentId = position.segid;
				}
			}
		}
	}

	private boolean notTooFar(int lng, int lat, XdrDataBase xdrItem)
	{
		LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(xdrItem.ecgi);
		if (lteCellInfo == null)
		{
			return false;
		}

		int dist = -1;
		int maxRadius = 3000;
		if (3 * lteCellInfo.radius <= 1000)// 没有理想覆盖的数据
		{
			maxRadius = 1000;
		} else if (3 * lteCellInfo.radius >= 3000)
		{
			maxRadius = 3000;
		} else
		{
			maxRadius = 3 * lteCellInfo.radius;
		}
		if (lng > 0 && lat > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
		{
			dist = (int) GisFunction.GetDistance(lng, lat, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
		}
		boolean result = dist > 0 && dist < maxRadius;
		if (result)
		{
			xdrItem.iCityID = lteCellInfo.cityid;
		}
		return result;
	}
}

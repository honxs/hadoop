package cn.mastercom.bigdata.mapr.mdt.loc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.conf.config.CellBuildInfo;
import cn.mastercom.bigdata.conf.config.CellBuildWifi;
import cn.mastercom.bigdata.conf.config.ImeiConfig;
import cn.mastercom.bigdata.evt.locall.stat.XdrLocallexDeal2;
import cn.mastercom.bigdata.mro.loc.CellTimeKey;
import cn.mastercom.bigdata.mro.loc.MroXdrDeal;
import cn.mastercom.bigdata.mro.loc.XdrLabel;
import cn.mastercom.bigdata.mro.loc.XdrLabelMng;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.DataAdapterReader;
import cn.mastercom.bigdata.util.IDataDeal;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.xdr.loc.LocationItem;
import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;

public class MdtMmeDeal implements IDataDeal{
	public static final int DataType_MME = MroXdrDeal.DataType_XDRLOCATION;
	public static final int DataType_MDT_IMM = MroXdrDeal.DataType_MDT_IMM;
	public static final int DataType_MDT_LOG = MroXdrDeal.DataType_MDT_LOG;
	private ResultOutputer resultOutputer;
	protected static final Log LOG = LogFactory.getLog(MdtMmeDeal.class);
	public StringBuffer sb = new StringBuffer();

	
	private XdrLabelMng xdrLabelMng;
	ParseItem parseItem_IMM;
	DataAdapterReader dataAdapterReader_IMM;
	DataAdapterReader dataAdapterReader_LOG;
	List<SIGNAL_MR_All> allMdtItemList = null;
	ParseItem parseItem_LOG;   
	Configuration conf;
	//是否被初始化过
	boolean hasInit;
	
	
	public MdtMmeDeal(ResultOutputer resultOutputer) {
		this.resultOutputer = resultOutputer;
		
		try{
			parseItem_IMM = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-IMM");
			parseItem_LOG = MainModel.GetInstance().getDataAdapterConfig().getParseItem("MDT-SRC-LOG");

			if (parseItem_IMM == null || parseItem_LOG == null)
			{
				throw new IOException("parse item do not get.");
			}
			dataAdapterReader_IMM = new DataAdapterReader(parseItem_IMM);
			dataAdapterReader_LOG = new DataAdapterReader(parseItem_LOG);
			
		}catch (Exception e)
		{
			LOGHelper.GetLogger().writeDetailLog(LogType.error,"MroXdrDeal init error", "MroXdrDeal init error: ", e);
			e.printStackTrace();
		}
	}
	
	public int pushData(CellTimeKey key, String value)
	{
		int dataType = key.getDataType();
//		current5Sec = key.getSuTime();
		return pushData(dataType, value);
	}
	
	@Override
	public int pushData(int dataType, String value) {
		// TODO Auto-generated method stub
		
		if (dataType != DataType_MME && hasInit == false)
		{
			xdrLabelMng.init();
			hasInit = true;
		}
		if (dataType == DataType_MME)
		{
			String[] strs = value.toString().split("_", -1);
			
			XdrLabel mme = new XdrLabel();
			try
			{
				mme.imsi = Long.parseLong(strs[0]);
				mme.itime = Integer.parseInt(strs[1]);
				mme.eci = Long.parseLong(strs[2]);
				mme.s1apid = Long.parseLong(strs[3]);
				
				
				
				xdrLabelMng.addXdrLocItem(mme);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeDetailLog(LogType.error,"XdrLable.FillData error", "XdrLable.FillData error ", e);
				for(int i = 0;i < strs.length;i++){
					System.out.println("mdt mme error~  Str : "+strs[i]);
				}
				return StaticConfig.EXCEPTION;
			}
		}
		else if (dataType == DataType_MDT_IMM)
		{
			SIGNAL_MR_All mdtItem = null;
			String[] strs = null;
			try
			{
				mdtItem = new SIGNAL_MR_All();
				strs = value.toString().split(parseItem_IMM.getSplitMark(), -1);
				dataAdapterReader_IMM.readData(strs);
				mdtItem.FillIMMData(dataAdapterReader_IMM);
				if (mdtItem == null || mdtItem.tsc == null || mdtItem.tsc.MmeUeS1apId <= 0 || mdtItem.tsc.Eci <= 0 || mdtItem.tsc.beginTime <= 0)
					return StaticConfig.EXCEPTION;
				/*
				 * mme根据S1apid为MDT回填用户
				 */
				XdrLabel mmeItem = xdrLabelMng.findXdrByS1apidTime(mdtItem.tsc.MmeUeS1apId, mdtItem.tsc.beginTime);
				if (mmeItem != null && mdtItem.tsc.longitude > 0 && mdtItem.tsc.latitude > 0)
				{
					mdtItem.tsc.IMSI = mmeItem.imsi;
					mdtItem.tsc.Msisdn = mmeItem.msisdn;
					
					resultOutputer.pushData(XdrPrepareTablesEnum.xdrLocation.getIndex(),outStrtoFs(mdtItem));

				}

//				allMdtItemList.add(mdtItem);

//				resultOutputer.pushData(XdrPrepareTablesEnum.xdrLocation.getIndex(),  outStrtoFs(mdtItem));

			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MDT_IMM.FillData error", "MDT_IMM.FillData error ", e);
				return StaticConfig.EXCEPTION;
			}
		}
		else if (dataType == DataType_MDT_LOG)
		{
			SIGNAL_MR_All mdtItem = null;
			String[] strs = null;
			try
			{
				mdtItem = new SIGNAL_MR_All();
				strs = value.toString().split(parseItem_LOG.getSplitMark(), -1);
				dataAdapterReader_LOG.readData(strs);
				mdtItem.FillLOGData(dataAdapterReader_LOG);
				if (mdtItem == null || mdtItem.tsc == null || mdtItem.tsc.MmeUeS1apId <= 0 || mdtItem.tsc.Eci <= 0 || mdtItem.tsc.beginTime <= 0)
					return StaticConfig.EXCEPTION;
				/*
				 * 回填用户
				 */
				XdrLabel mmeItem = xdrLabelMng.findXdrByS1apidTime(mdtItem.tsc.MmeUeS1apId, mdtItem.tsc.beginTime);
				if (mmeItem != null && mdtItem.tsc.longitude > 0 && mdtItem.tsc.latitude > 0)
				{
					mdtItem.tsc.IMSI = mmeItem.imsi;
					mdtItem.tsc.Msisdn = mmeItem.msisdn;

					resultOutputer.pushData(XdrPrepareTablesEnum.xdrLocation.getIndex(),  outStrtoFs(mdtItem));

				}
//				allMdtItemList.add(mdtItem);
//				resultOutputer.pushData(XdrPrepareTablesEnum.xdrLocation.getIndex(),  outStrtoFs(mdtItem));
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeLog(LogType.error,"MDT_LOG.FillData error", "MDT_LOG.FillData error ", e);
				return StaticConfig.EXCEPTION;
			}
		}
		return 0;
	}
	
	@Override
	public void statData() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void outData() {
		// TODO Auto-generated method stub
		
	}
	
	public String outStrtoFs(SIGNAL_MR_All mdtItem)
	{
		
		sb.delete(0, sb.length());
		sb.append(mdtItem.tsc.IMSI);  //imsi
		sb.append("|");
		sb.append(((long)mdtItem.tsc.beginTime)*1000+(long)mdtItem.tsc.beginTimems);//itime + itimeMS
		sb.append("|");
		sb.append(((long)mdtItem.tsc.beginTime)*1000+(long)mdtItem.tsc.beginTimems);//locTime + locTimeMS
		sb.append("|");
		sb.append(mdtItem.tsc.Eci);   //eci
		sb.append("|");
		sb.append("");  //serIP
		sb.append("|");
		sb.append(0);   //port
		sb.append("|");
		sb.append("");   //serverIP
		sb.append("|");
		sb.append(9);   //location
		sb.append("|");
		sb.append("ll");   //loctp
		sb.append("|");
		sb.append(mdtItem.radius > 0 ? mdtItem.radius : 20);   //radius
		sb.append("|");
		sb.append((mdtItem.tsc.longitude + 0.0d)/10000000);
		sb.append("|");
		sb.append((mdtItem.tsc.latitude + 0.0d)/10000000);
		sb.append("|");
		sb.append(9);   //location2
		sb.append("|");
		sb.append("");   //wifiName
		
		return sb.toString();
		
		
	}
	
	public void init(CellTimeKey key)
	{
		
		xdrLabelMng = new XdrLabelMng();
		hasInit = false;
		//LOGHelper.GetLogger().writeLog(LogType.info, "Datakey :  "+ key);
		
	}

	
	


	
	

}

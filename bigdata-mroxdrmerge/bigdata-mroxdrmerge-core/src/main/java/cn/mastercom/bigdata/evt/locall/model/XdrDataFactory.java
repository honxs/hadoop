package cn.mastercom.bigdata.evt.locall.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class XdrDataFactory implements Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static int LOCTYPE_XDRLOC = 1;
	public static int LOCTYPE_MRLOC = 2;
	public static int LOCTYPE_RESIDENTLOC = 3;

	private Map<Integer, XdrDataBase> dataTypeMap;

	private static XdrDataFactory instance;

	public static XdrDataFactory GetInstance()
	{
		if (instance == null)
		{
			instance = new XdrDataFactory();
		}
		return instance;
	}

	private XdrDataFactory()
	{
		init();
	}

	public boolean init()
	{
		dataTypeMap = new HashMap<Integer, XdrDataBase>();

		// 内蒙数据
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{

			XdrData_Mme itemmme = new XdrData_Mme();
			itemmme.setDataType(TypeIoEvtEnum.ORIGINMME.getIndex());
			dataTypeMap.put(itemmme.getDataType(), itemmme);

			XdrData_Http item_Http = new XdrData_Http();
			item_Http.setDataType(TypeIoEvtEnum.ORIGINHTTP.getIndex());
			dataTypeMap.put(item_Http.getDataType(), item_Http);

			XdrData_Mg itemMg = new XdrData_Mg();
			itemMg.setDataType(TypeIoEvtEnum.ORIGINMG.getIndex());
			dataTypeMap.put(itemMg.getDataType(), itemMg);

			XdrData_Sv itemNeimengSv = new XdrData_Sv();
			itemNeimengSv.setDataType(TypeIoEvtEnum.ORIGINSV.getIndex());
			dataTypeMap.put(itemNeimengSv.getDataType(), itemNeimengSv);

			XdrData_Rtp itemRtp = new XdrData_Rtp();
			itemRtp.setDataType(TypeIoEvtEnum.ORIGINRTP.getIndex());
			dataTypeMap.put(itemRtp.getDataType(), itemRtp);
			
			
			MroData_ul_lostRackRateL mroData_ul_lostRackRateL = new MroData_ul_lostRackRateL();
			mroData_ul_lostRackRateL.setDataType(TypeIoEvtEnum.ORIGIN_MRO.getIndex());
			dataTypeMap.put(mroData_ul_lostRackRateL.getDataType(), mroData_ul_lostRackRateL);
			
			XdrData_Mw itemMw = new XdrData_Mw();
			itemMw.setDataType(TypeIoEvtEnum.ORIGINMW.getIndex());
			dataTypeMap.put(itemMw.getDataType(), itemMw);

			XdrData_Rx itemRx = new XdrData_Rx();
			itemRx.setDataType(TypeIoEvtEnum.ORIGINRX.getIndex());
			dataTypeMap.put(itemRx.getDataType(), itemRx);

		}
		else if (MainModel.GetInstance().getCompile().Assert(CompileMark.NingXia)
				|| MainModel.GetInstance().getCompile().Assert(CompileMark.YunNan))
		{

			XdrData_Mme itemmme = new XdrData_Mme();
			itemmme.setDataType(TypeIoEvtEnum.ORIGINMME.getIndex());
			dataTypeMap.put(itemmme.getDataType(), itemmme);

			XdrData_Http item_Http = new XdrData_Http();
			item_Http.setDataType(TypeIoEvtEnum.ORIGINHTTP.getIndex());
			dataTypeMap.put(item_Http.getDataType(), item_Http);

			XdrData_Uu itemUu = new XdrData_Uu();
			itemUu.setDataType(TypeIoEvtEnum.ORIGIN_Uu.getIndex());
			dataTypeMap.put(itemUu.getDataType(), itemUu);
		}

		else
		{
			{
				XdrData_Http item = new XdrData_Http();
				item.setDataType(TypeIoEvtEnum.ORIGINHTTP.getIndex());
				dataTypeMap.put(item.getDataType(), item);

				XdrData_Mw itemMw = new XdrData_Mw();
				itemMw.setDataType(TypeIoEvtEnum.ORIGINMW.getIndex());
				dataTypeMap.put(itemMw.getDataType(), itemMw);

				XdrData_Sv itemSv = new XdrData_Sv();
				itemSv.setDataType(TypeIoEvtEnum.ORIGINSV.getIndex());
				dataTypeMap.put(itemSv.getDataType(), itemSv);

				XdrData_Rx itemRx = new XdrData_Rx();
				itemRx.setDataType(TypeIoEvtEnum.ORIGINRX.getIndex());
				dataTypeMap.put(itemRx.getDataType(), itemRx);

				XdrData_Mme itemmme = new XdrData_Mme();
				itemmme.setDataType(TypeIoEvtEnum.ORIGINMME.getIndex());
				dataTypeMap.put(itemmme.getDataType(), itemmme);
			}

			{
				XdrData_MOS_BeiJing item = new XdrData_MOS_BeiJing();
				item.setDataType(TypeIoEvtEnum.ORIGINMOS_BEIJING.getIndex());
				dataTypeMap.put(item.getDataType(), item);
			}

			{
				XdrData_WJTDH_BeiJing item = new XdrData_WJTDH_BeiJing();
				item.setDataType(TypeIoEvtEnum.ORIGINWJTDH_BEIJING.getIndex());
				dataTypeMap.put(item.getDataType(), item);
			}

			{
				XdrData_Ims_Mo itemMo = new XdrData_Ims_Mo();
				itemMo.setDataType(TypeIoEvtEnum.ORIGINIMS_MO.getIndex());
				dataTypeMap.put(itemMo.getDataType(), itemMo);

				XdrData_Ims_Mt itemMt = new XdrData_Ims_Mt();
				itemMt.setDataType(TypeIoEvtEnum.ORIGINIMS_MT.getIndex());
				dataTypeMap.put(itemMt.getDataType(), itemMt);

				// XdrData_Cdr_Sv itemCdrSv = new XdrData_Cdr_Sv();
				// itemCdrSv.setDataType(TypeIoEvt.ORIGIN.dataType);
				// dataTypeMap.put(itemCdrSv.getDataType(), itemCdrSv);

				XdrData_Cdr_Quality itemCdrQuality = new XdrData_Cdr_Quality();
				itemCdrQuality.setDataType(TypeIoEvtEnum.ORIGINCDR_QUALITY.getIndex());
				dataTypeMap.put(itemCdrQuality.getDataType(), itemCdrQuality);
			}
			{
				XdrData_Uu itemUu = new XdrData_Uu();
				itemUu.setDataType(TypeIoEvtEnum.ORIGIN_Uu.getIndex());
				dataTypeMap.put(itemUu.getDataType(), itemUu);
			}
			{
				MdtData_HOFRLF mdtData_HOFRLF = new MdtData_HOFRLF();
				mdtData_HOFRLF.setDataType(TypeIoEvtEnum.ORIGIN_MDT_RLFHOF.getIndex());
				dataTypeMap.put(mdtData_HOFRLF.getDataType(), mdtData_HOFRLF);
			}
			
			{
				MdtData_HOFRLF_OtherPath mdtData_HOFRLF_Other = new MdtData_HOFRLF_OtherPath();
				mdtData_HOFRLF_Other.setDataType(TypeIoEvtEnum.ORIGIN_MDT_RLFHOF_OTHERPATH.getIndex());
				dataTypeMap.put(mdtData_HOFRLF_Other.getDataType(), mdtData_HOFRLF_Other);
			}

			{
				MdtData_RCEF mdtData_Rcef = new MdtData_RCEF();
				mdtData_Rcef.setDataType(TypeIoEvtEnum.ORIGIN_MDT_RCEF.getIndex());
				dataTypeMap.put(mdtData_Rcef.getDataType(), mdtData_Rcef);
			}

			
			{
				MosSharding mosSharding = new MosSharding();
				mosSharding.setDataType(TypeIoEvtEnum.ORIGIN_MOS_SHARDING.getIndex());
				dataTypeMap.put(mosSharding.getDataType(), mosSharding);
			}
			// mdt1,mdt4,mdt5
            {
                MdtImm1 mdtImm1 = new MdtImm1();
                mdtImm1.setDataType(TypeIoEvtEnum.ORIGIN_MDT_IMM1.getIndex());
                dataTypeMap.put(mdtImm1.getDataType(),mdtImm1);
            }
            {
                MdtImm4 mdtImm4 = new MdtImm4();
                mdtImm4.setDataType(TypeIoEvtEnum.ORIGIN_MDT_IMM4.getIndex());
                dataTypeMap.put(mdtImm4.getDataType(),mdtImm4);
            }
            {
                MdtImm5 mdtImm5 = new MdtImm5();
                mdtImm5.setDataType(TypeIoEvtEnum.ORIGIN_MDT_IMM5.getIndex());
                dataTypeMap.put(mdtImm5.getDataType(),mdtImm5);
            }
			{
				MdtDataHOLRLFRCEF mdtDataHOLRLFRCEF = new MdtDataHOLRLFRCEF();
				mdtDataHOLRLFRCEF.setDataType(TypeIoEvtEnum.ORIGIN_MDT_RLFHOFRCEF.getIndex());
				dataTypeMap.put(mdtDataHOLRLFRCEF.getDataType(),mdtDataHOLRLFRCEF);
			}

		}

		return true;
	}

	public XdrDataBase getXdrDataObject(int dataType) throws InstantiationException, IllegalAccessException
	{
		XdrDataBase xdrData = dataTypeMap.get(dataType);
		XdrDataBase newItem = xdrData.getClass().newInstance();
		newItem.setDataType(xdrData.getDataType());
		return newItem;
	}

}

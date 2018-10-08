package cn.mastercom.bigdata.xdr.loc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import cn.mastercom.bigdata.StructData.DT_Event;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.RowKeyMaker;
import cn.mastercom.bigdata.StructData.SIGNAL_XDR_4G;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_23G;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_4G;
import cn.mastercom.bigdata.StructData.Stat_CellGrid_Freq_4G;
import cn.mastercom.bigdata.StructData.Stat_Cell_2G;
import cn.mastercom.bigdata.StructData.Stat_Cell_3G;
import cn.mastercom.bigdata.StructData.Stat_Cell_4G;
import cn.mastercom.bigdata.StructData.Stat_Cell_Freq;
import cn.mastercom.bigdata.StructData.Stat_Grid_23G;
import cn.mastercom.bigdata.StructData.Stat_Grid_2G;
import cn.mastercom.bigdata.StructData.Stat_Grid_3G;
import cn.mastercom.bigdata.StructData.Stat_Grid_4G;
import cn.mastercom.bigdata.StructData.Stat_Grid_Freq_4G;
import cn.mastercom.bigdata.StructData.Stat_UserAct_Cell;
import cn.mastercom.bigdata.StructData.Stat_UserGrid_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.Func;
import cn.mastercom.bigdata.xdr.loc.UserActStat.UserAct;
import cn.mastercom.bigdata.xdr.loc.UserActStat.UserActTime;

public class ResultHelper
{
	public static final String TabMark = "\t";

	public static String getPutLteEvent(DT_Event item)
	{
		// String key = RowKeyMaker.DtLteEventKey(0, 100, item.imsi, item.itime,
		// item.wtimems, item.ilongitude, item.ilatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(key);sb.append(TabMark);
		sb.append(Func.getEncrypt(item.imsi));
		sb.append(TabMark);
		sb.append(item.cityID);
		sb.append(TabMark);
		sb.append(item.fileID);
		sb.append(TabMark);
		sb.append(item.projectID);
		sb.append(TabMark);
		sb.append(item.SampleID);
		sb.append(TabMark);
		sb.append(item.itime);
		sb.append(TabMark);
		sb.append(item.wtimems);
		sb.append(TabMark);
		sb.append(item.bms);
		sb.append(TabMark);
		sb.append(item.eventID);
		sb.append(TabMark);
		sb.append(item.ilongitude);
		sb.append(TabMark);
		sb.append(item.ilatitude);
		sb.append(TabMark);
		sb.append(item.cqtposid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.wRAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.iTargetLAC);
		sb.append(TabMark);
		sb.append(item.wTargetRAC);
		sb.append(TabMark);
		sb.append(item.iTargetCI);
		sb.append(TabMark);
		sb.append(item.ivalue1);
		sb.append(TabMark);
		sb.append(item.ivalue2);
		sb.append(TabMark);
		sb.append(item.ivalue3);
		sb.append(TabMark);
		sb.append(item.ivalue4);
		sb.append(TabMark);
		sb.append(item.ivalue5);
		sb.append(TabMark);
		sb.append(item.ivalue6);
		sb.append(TabMark);
		sb.append(item.ivalue7);
		sb.append(TabMark);
		sb.append(item.ivalue8);
		sb.append(TabMark);
		sb.append(item.ivalue9);
		sb.append(TabMark);
		sb.append(item.ivalue10);
		sb.append(TabMark);
		sb.append(item.LocFillType);
		sb.append(TabMark);

		sb.append(item.testType);
		sb.append(TabMark);
		sb.append(item.location);
		sb.append(TabMark);
		sb.append(item.dist);
		sb.append(TabMark);
		sb.append(item.radius);
		sb.append(TabMark);
		sb.append(item.loctp);
		sb.append(TabMark);
		sb.append(item.indoor);
		sb.append(TabMark);
		sb.append(item.networktype);
		sb.append(TabMark);
		sb.append(item.label);
		sb.append(TabMark);
		sb.append(item.moveDirect);

		return sb.toString();
	}

	public static String getPutLteSample(DT_Sample_4G item)
	{
		//cs采样点 从gsm第二组开始放异频数据，td第三组开始放异频数据，降序排列，优先放gsm
		ArrayList<NC_LTE> freq = new ArrayList<>();
		freq.addAll(Arrays.asList(item.lt_freq));
		freq.addAll(Arrays.asList(item.dx_freq));
		Collections.sort(freq, new Comparator<NC_LTE>() {
			@Override
			public int compare(NC_LTE o1, NC_LTE o2) {
				// TODO Auto-generated method stub
				return o2.LteNcRSRP - o1.LteNcRSRP;
			}
		});
		
		StringBuffer sb = new StringBuffer();
		sb.append(item.cityID);
		sb.append(TabMark);
		sb.append(item.imeiTac);
		sb.append(TabMark);
		sb.append(item.itime);
		sb.append(TabMark);
		sb.append(item.wtimems);
		sb.append(TabMark);
		sb.append(item.bms);
		sb.append(TabMark);
		sb.append(item.ilongitude);
		sb.append(TabMark);
		sb.append(item.ilatitude);
		sb.append(TabMark);
		sb.append(item.ibuildingID);
		sb.append(TabMark);
		sb.append(item.iheight);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.Eci);
		sb.append(TabMark);
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			sb.append(item.IMSI);
			sb.append(TabMark);
			sb.append(item.MSISDN);
		}
		else
		{
			sb.append(Func.getEncrypt(item.IMSI));
			sb.append(TabMark);
			sb.append(Func.getEncrypt(item.MSISDN));
		}
		sb.append(TabMark);
		sb.append(item.UETac);
		sb.append(TabMark);
		sb.append(item.UEBrand);
		sb.append(TabMark);
		sb.append(item.UEType);
		sb.append(TabMark);
		sb.append(item.serviceType);
		sb.append(TabMark);
		sb.append(item.serviceSubType);
		sb.append(TabMark);
		sb.append(item.urlDomain);
		sb.append(TabMark);
		sb.append(item.IPDataUL);
		sb.append(TabMark);
		sb.append(item.IPDataDL);
		sb.append(TabMark);
		sb.append(item.duration);
		sb.append(TabMark);
		sb.append(item.IPThroughputUL);
		sb.append(TabMark);
		sb.append(item.IPThroughputDL);
		sb.append(TabMark);
		sb.append(item.IPPacketUL);
		sb.append(TabMark);
		sb.append(item.IPPacketDL);
		sb.append(TabMark);
		sb.append(item.TCPReTranPacketUL);
		sb.append(TabMark);
		sb.append(item.TCPReTranPacketDL);
		sb.append(TabMark);
		sb.append(item.sessionRequest);
		sb.append(TabMark);
		sb.append(item.sessionResult);
		sb.append(TabMark);
		sb.append(item.eventType);
		sb.append(TabMark);
		sb.append(item.userType);
		sb.append(TabMark);
		sb.append(item.eNBName);
		sb.append(TabMark);
		sb.append(item.eNBLongitude);
		sb.append(TabMark);
		sb.append(item.eNBLatitude);
		sb.append(TabMark);
		sb.append(item.eNBDistance);
		sb.append(TabMark);
		sb.append(item.flag);
		sb.append(TabMark);
		sb.append(item.ENBId);
		sb.append(TabMark);
		sb.append(item.UserLabel);
		sb.append(TabMark);
		sb.append(item.CellId);
		sb.append(TabMark);
		sb.append(item.Earfcn);
		sb.append(TabMark);
		sb.append(item.SubFrameNbr);
		sb.append(TabMark);
		sb.append(item.MmeCode);
		sb.append(TabMark);
		sb.append(item.MmeGroupId);
		sb.append(TabMark);
		sb.append(item.MmeUeS1apId);
		sb.append(TabMark);
		sb.append(item.Weight);
		sb.append(TabMark);
		sb.append(item.LteScRSRP);
		sb.append(TabMark);
		sb.append(item.LteScRSRQ);
		sb.append(TabMark);
		sb.append(item.LteScEarfcn);
		sb.append(TabMark);
		sb.append(item.LteScPci);
		sb.append(TabMark);
		sb.append(item.LteScBSR);
		sb.append(TabMark);
		sb.append(item.LteScRTTD);
		sb.append(TabMark);
		sb.append(item.LteScTadv);
		sb.append(TabMark);
		sb.append(item.LteScAOA);
		sb.append(TabMark);
		sb.append(item.LteScPHR);
		sb.append(TabMark);
		sb.append(item.LteScRIP);
		sb.append(TabMark);
		sb.append(item.LteScSinrUL);
		sb.append(TabMark);
		sb.append(item.nccount[0]);
		sb.append(TabMark);
		sb.append(item.nccount[1]);
		sb.append(TabMark);
//		sb.append(item.nccount[2]);
		sb.append(0);
		sb.append(TabMark);
//		sb.append(item.nccount[3]);
		sb.append(0);
		sb.append(TabMark);

		for (int i = 0; i < item.tlte.length; ++i)
		{
			sb.append(item.tlte[i].LteNcRSRP);
			sb.append(TabMark);
			sb.append(item.tlte[i].LteNcRSRQ);
			sb.append(TabMark);
			sb.append(item.tlte[i].LteNcEarfcn);
			sb.append(TabMark);
			sb.append(item.tlte[i].LteNcPci);
			sb.append(TabMark);
		}

		//没有td
		for (int i = 0; i < 6; ++i)
		{
			sb.append(0);
			sb.append(TabMark);
			sb.append(0);
			sb.append(TabMark);
			sb.append(0);
			sb.append(TabMark);
		}
		
		sb.append(item.tgsm[0].GsmNcellCarrierRSSI);
		sb.append(TabMark);
		sb.append(item.tgsm[0].GsmNcellBcch);
		sb.append(TabMark);
		sb.append(item.tgsm[0].GsmNcellBsic);
		sb.append(TabMark);
		for (int i = 0; i < 5; ++i)
		{
			if (freq.get(i).LteNcRSRP < 0 && freq.get(i).LteNcRSRP > -200)
			{
				sb.append((freq.get(i).LteNcRSRP + 200) * 1000 + (freq.get(i).LteNcRSRQ + 200));
				sb.append(TabMark);
				sb.append(freq.get(i).LteNcEarfcn);
				sb.append(TabMark);
				sb.append(freq.get(i).LteNcPci);
				sb.append(TabMark);
			}else {
				sb.append(0);
				sb.append(TabMark);
				sb.append(0);
				sb.append(TabMark);
				sb.append(0);
				sb.append(TabMark);
			}
		}
		freq.clear();
		//原本trip
		for (int i = 0; i < 10; ++i)
		{
			sb.append(0);
			sb.append(TabMark);
			sb.append(0);
			sb.append(TabMark);
			sb.append(0);
			sb.append(TabMark);
		}
		sb.append(item.LocFillType);
		sb.append(TabMark);

		sb.append(item.testType);
		sb.append(TabMark);
		sb.append(item.location);
		sb.append(TabMark);
		sb.append(item.dist);
		sb.append(TabMark);
		sb.append(item.radius);
		sb.append(TabMark);
		sb.append(item.locType);
		sb.append(TabMark);
		sb.append(item.indoor);
		sb.append(TabMark);
		sb.append(item.networktype);
		sb.append(TabMark);
		sb.append(item.label);
		sb.append(TabMark);
		sb.append(item.simuLongitude);
		sb.append(TabMark);
		sb.append(item.simuLatitude);
		sb.append(TabMark);

		sb.append(item.moveDirect);
		sb.append(TabMark);
		sb.append(item.mrType);
		sb.append(TabMark);

		sb.append(item.dfcnJamCellCount);
		sb.append(TabMark);
		sb.append(item.sfcnJamCellCount);
		
		if (MainModel.GetInstance().getCompile().Assert(CompileMark.NeiMeng))
		{
			for (int i = 0; i < item.tlte.length; ++i)
			{
				sb.append(TabMark);
				sb.append(item.tlte[i].LteNcEnodeb);
				sb.append(TabMark);
				sb.append(item.tlte[i].LteNcCid);
			}
			for (int i = 0; i < item.tgsm.length; ++i)
			{
				sb.append(TabMark);
				sb.append(item.tgsm[i].GsmNcellCarrierRSSI);
				sb.append(TabMark);
				sb.append(item.tgsm[i].GsmNcellBcch);
				sb.append(TabMark);
				sb.append(item.tgsm[i].GsmNcellBsic);
				sb.append(TabMark);
				sb.append(item.tgsm[i].GsmNcellLac);
				sb.append(TabMark);
				sb.append(item.tgsm[i].GsmNcellCi);
			}
			for (int i = 0; i < item.tgsm.length; ++i)
			{
				sb.append(TabMark);
				sb.append(StaticConfig.Int_Abnormal);
				sb.append(TabMark);
				sb.append(StaticConfig.Int_Abnormal);
				sb.append(TabMark);
				sb.append(StaticConfig.Int_Abnormal);
				sb.append(TabMark);
				sb.append(StaticConfig.Int_Abnormal);
				sb.append(TabMark);
				sb.append(StaticConfig.Int_Abnormal);
			}
		}

		return sb.toString().replace("-1000000.0", "").replace("-1000000", "");

	}

	public static String getPutLteSampleSmp(DT_Sample_4G item)
	{	
		//String samKey = RowKeyMaker.DtLteSampleKey(0, 100, item.IMSI, item.itime, item.wtimems, item.ilongitude, item.ilatitude);         

		StringBuffer sb = new StringBuffer();
		//sb.append(samKey);sb.append(TabMark);
		
		sb.append(item.cityID);sb.append(TabMark);            
		sb.append(item.imeiTac);sb.append(TabMark);          
		sb.append(item.itime);sb.append(TabMark);             
		sb.append(item.wtimems);sb.append(TabMark);           
		sb.append(item.bms);sb.append(TabMark);               
		sb.append(item.ilongitude);sb.append(TabMark);        
		sb.append(item.ilatitude);sb.append(TabMark);         
		sb.append(item.ibuildingID);sb.append(TabMark);            
		sb.append(item.iheight);sb.append(TabMark);             
		sb.append(item.iLAC);sb.append(TabMark);              
		sb.append(item.iCI);sb.append(TabMark);      
		sb.append(item.Eci);sb.append(TabMark); 
		sb.append(item.IMSI);sb.append(TabMark);              
		sb.append(item.MSISDN);sb.append(TabMark);            
		//sb.append(item.IMSI+"");sb.append(TabMark);              
		//sb.append(item.MSISDN+"");sb.append(TabMark);            
		/*sb.append(item.UETac);sb.append(TabMark);             
		sb.append(item.UEBrand);sb.append(TabMark);           
		sb.append(item.UEType);sb.append(TabMark);            
		sb.append(item.serviceType);sb.append(TabMark);       
		sb.append(item.serviceSubType);sb.append(TabMark);    
		sb.append(item.urlDomain);sb.append(TabMark);         
		sb.append(item.IPDataUL);sb.append(TabMark);          
		sb.append(item.IPDataDL);sb.append(TabMark);          
		sb.append(item.duration);sb.append(TabMark);          
		sb.append(item.IPThroughputUL);sb.append(TabMark);    
		sb.append(item.IPThroughputDL);sb.append(TabMark);    
		sb.append(item.IPPacketUL);sb.append(TabMark);        
		sb.append(item.IPPacketDL);sb.append(TabMark);        
		sb.append(item.TCPReTranPacketUL);sb.append(TabMark); 
		sb.append(item.TCPReTranPacketDL);sb.append(TabMark); 
		sb.append(item.sessionRequest);sb.append(TabMark);    
		sb.append(item.sessionResult);sb.append(TabMark);     
		sb.append(item.eventType);sb.append(TabMark);         
		sb.append(item.userType);sb.append(TabMark);          
		sb.append(item.eNBName);sb.append(TabMark);           
		sb.append(item.eNBLongitude);sb.append(TabMark);      
		sb.append(item.eNBLatitude);sb.append(TabMark);       
		sb.append(item.eNBDistance);sb.append(TabMark);       
		sb.append(item.flag);sb.append(TabMark); */             
		sb.append(item.ENBId);sb.append(TabMark);             
		sb.append(item.UserLabel);sb.append(TabMark);         
		sb.append(item.CellId);sb.append(TabMark);            
		sb.append(item.Earfcn);sb.append(TabMark);            
		//sb.append(item.SubFrameNbr);sb.append(TabMark);       
		//sb.append(item.MmeCode);sb.append(TabMark);           
		//sb.append(item.MmeGroupId);sb.append(TabMark);        
		sb.append(item.MmeUeS1apId);sb.append(TabMark);       
		//sb.append(item.Weight);sb.append(TabMark);            
		sb.append(item.LteScRSRP);sb.append(TabMark);         
		sb.append(item.LteScRSRQ);sb.append(TabMark);         
		sb.append(item.LteScEarfcn);sb.append(TabMark);       
		sb.append(item.LteScPci);sb.append(TabMark);          
		sb.append(item.LteScBSR);sb.append(TabMark);          
		//sb.append(item.LteScRTTD);sb.append(TabMark);         
		sb.append(item.LteScTadv);sb.append(TabMark);         
		sb.append(item.LteScAOA);sb.append(TabMark);          
		//sb.append(item.LteScPHR);sb.append(TabMark);          
		//sb.append(item.LteScRIP);sb.append(TabMark);          
		sb.append(item.LteScSinrUL);sb.append(TabMark);       
		sb.append(item.nccount[0]);sb.append(TabMark);          
		//sb.append(item.nccount[1]);sb.append(TabMark);          
		//sb.append(item.nccount[2]);sb.append(TabMark);          
		//sb.append(item.nccount[3]);sb.append(TabMark);          

        
        for (int i=0; i< item.tlte.length; ++i)
		{
        	sb.append(item.tlte[i].LteNcRSRP);sb.append(TabMark);    
        	sb.append(item.tlte[i].LteNcRSRQ);sb.append(TabMark);  
        	sb.append(item.tlte[i].LteNcEarfcn);sb.append(TabMark);  
        	sb.append(item.tlte[i].LteNcPci);sb.append(TabMark);  
		}       
        
        /*for (int i=0; i< item.ttds.length; ++i)
		{
        	sb.append(item.ttds[i].TdsPccpchRSCP);sb.append(TabMark);  
        	sb.append(item.ttds[i].TdsNcellUarfcn);sb.append(TabMark); 
        	sb.append(item.ttds[i].TdsCellParameterId);sb.append(TabMark); 
		}*/
        
        for (int i=1; i< 3; ++i)
		{//dx freq
        	sb.append(item.tgsm[i].GsmNcellCarrierRSSI);sb.append(TabMark); 
        	sb.append(item.tgsm[i].GsmNcellBcch);sb.append(TabMark); 
        	sb.append(item.tgsm[i].GsmNcellBsic);sb.append(TabMark); 
		}  

        /*for (int i=0; i< item.trip.length; ++i)
		{
        	sb.append(item.trip[i].Earfcn);sb.append(TabMark); 
        	sb.append(item.trip[i].SubFrame);sb.append(TabMark); 
        	sb.append(item.trip[i].LteScRIP);sb.append(TabMark);  
		}*/
        sb.append(item.LocFillType);sb.append(TabMark);  
        
        sb.append(item.testType); sb.append(TabMark);
        sb.append(item.location); sb.append(TabMark);
        sb.append(item.dist);     sb.append(TabMark);
        sb.append(item.radius);   sb.append(TabMark);
        sb.append(item.locType);    sb.append(TabMark); 
        sb.append(item.indoor);   sb.append(TabMark);
        sb.append(item.networktype);sb.append(TabMark);
        sb.append(item.label);sb.append(TabMark);
        sb.append(item.simuLongitude);sb.append(TabMark);
        sb.append(item.simuLatitude);sb.append(TabMark);
        
        sb.append(item.moveDirect);sb.append(TabMark);
        sb.append(item.mrType);sb.append(TabMark);
        
        sb.append(item.dfcnJamCellCount);sb.append(TabMark);
        sb.append(item.sfcnJamCellCount);

        return sb.toString();      
	}

	public static String getPutCellGrid_4G(Stat_CellGrid_4G item)
	{
		// String samKey = RowKeyMaker.DtLteCellGridKey(0, 100, item.iLac,
		// item.iCi, item.startTime, item.itllongitude, item.itllatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLac);
		sb.append(TabMark);
		sb.append(item.iCi);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.UpLen);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed_1M);
		sb.append(TabMark);

		sb.append(item.UserCount_4G);
		sb.append(TabMark);
		sb.append(item.UserCount_3G);
		sb.append(TabMark);
		sb.append(item.UserCount_2G);
		sb.append(TabMark);
		sb.append(item.UserCount_4GFall);
		sb.append(TabMark);
		sb.append(item.XdrCount);
		sb.append(TabMark);
		sb.append(item.MrCount);
		sb.append(TabMark);

		sb.append(item.tStat.RSRQ_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nSum);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[0]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[1]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[2]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[3]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[4]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[5]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nCount7);
		sb.append(TabMark);
		sb.append(item.overlapnum);
		sb.append(TabMark);
		sb.append(item.overlapden);
		return sb.toString();
	}

	public static String getPutCellGrid_23G(Stat_CellGrid_23G item)
	{
		// String samKey = RowKeyMaker.DtLteCellGridKey(0, 100, item.iLac,
		// item.iCi, item.startTime, item.itllongitude, item.itllatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLac);
		sb.append(TabMark);
		sb.append(item.iCi);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.UserCount);
		sb.append(TabMark);
		sb.append(item.UserCount_2G);
		sb.append(TabMark);
		sb.append(item.UserCount_3G);
		sb.append(TabMark);
		sb.append(item.XdrCount);

		return sb.toString();
	}

	public static String getPutGrid_4G(Stat_Grid_4G item)
	{
		// String samKey = RowKeyMaker.DtLteGridKey(0, 100, item.startTime,
		// item.itllongitude, item.itllatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed_1M);
		sb.append(TabMark);

		sb.append(item.UserCount_4G);
		sb.append(TabMark);
		sb.append(item.UserCount_3G);
		sb.append(TabMark);
		sb.append(item.UserCount_2G);
		sb.append(TabMark);
		sb.append(item.UserCount_4GFall);
		sb.append(TabMark);
		sb.append(item.XdrCount);
		sb.append(TabMark);
		sb.append(item.MrCount);
		sb.append(TabMark);

		sb.append(item.tStat.RSRQ_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nSum);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[0]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[1]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[2]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[3]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[4]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[5]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nCount7);

		return sb.toString();
	}
	
	public static String getPutCellGrid_4G_FREQ(Stat_CellGrid_Freq_4G item)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(item.eci);
		sb.append(TabMark);
		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.freq);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nCount7);

		return sb.toString();

	}

	public static String getPutGrid_4G_FREQ(Stat_Grid_Freq_4G item)
	{
		StringBuffer sb = new StringBuffer();

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.freq);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nCount7);
		return sb.toString();
	}

	public static String getPutGrid_2G(Stat_Grid_2G item)
	{
		// String samKey = RowKeyMaker.DtLteGridKey(0, 100, item.startTime,
		// item.itllongitude, item.itllatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.UserCount);
		sb.append(TabMark);
		sb.append(item.XdrCount);

		return sb.toString();
	}

	public static String getPutGrid_3G(Stat_Grid_3G item)
	{
		// String samKey = RowKeyMaker.DtLteGridKey(0, 100, item.startTime,
		// item.itllongitude, item.itllatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.UserCount);
		sb.append(TabMark);
		sb.append(item.XdrCount);

		return sb.toString();
	}

	public static String getPutGrid_23G(Stat_Grid_23G item)
	{
		// String samKey = RowKeyMaker.DtLteGridKey(0, 100, item.startTime,
		// item.itllongitude, item.itllatitude);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);

		sb.append(item.UserCount);
		sb.append(TabMark);
		sb.append(item.UserCount_2G);
		sb.append(TabMark);
		sb.append(item.UserCount_3G);
		sb.append(TabMark);
		sb.append(item.XdrCount);

		return sb.toString();
	}

	public static String getPutCell_4G(Stat_Cell_4G item)
	{
		// String samKey = RowKeyMaker.DtLteCellKey(0, 100, item.iCI,
		// item.startTime);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.xdrCount);
		sb.append(TabMark);
		sb.append(item.mroCount);
		sb.append(TabMark);
		sb.append(item.mroxdrCount);
		sb.append(TabMark);
		sb.append(item.mreCount);
		sb.append(TabMark);
		sb.append(item.mrexdrCount);
		sb.append(TabMark);

		sb.append(item.origLocXdrCount);
		sb.append(TabMark);
		sb.append(item.totalLocXdrCount);
		sb.append(TabMark);
		sb.append(item.validLocXdrCount);
		sb.append(TabMark);
		sb.append(item.dtXdrCount);
		sb.append(TabMark);
		sb.append(item.cqtXdrCount);
		sb.append(TabMark);
		sb.append(item.dtexXdrCount);
		sb.append(TabMark);

		sb.append(item.tStat.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.RSRP_nCount.length; i++)
		{
			sb.append(item.tStat.RSRP_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.SINR_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.SINR_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.tStat.SINR_nCount.length; i++)
		{
			sb.append(item.tStat.SINR_nCount[i]);
			sb.append(TabMark);
		}
		sb.append(item.tStat.RSRP100_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP105_SINR0);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR3);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP110_SINR0);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed);
		sb.append(TabMark);

		sb.append(item.tStat.UpLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DwLen_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationU_1M);
		sb.append(TabMark);
		sb.append(item.tStat.DurationD_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxUpSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.AvgDwSpeed_1M);
		sb.append(TabMark);
		sb.append(item.tStat.MaxDwSpeed_1M);
		sb.append(TabMark);

		sb.append(item.sfcnJamSamCount);
		sb.append(TabMark);
		sb.append(item.sdfcnJamSamCount);
		sb.append(TabMark);

		sb.append(item.tStat.RSRQ_nTotal);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nSum);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[0]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[1]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[2]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[3]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[4]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRQ_nCount[5]);
		sb.append(TabMark);
		sb.append(item.tStat.RSRP_nCount7);
		return sb.toString();
	}

	public static String getPutCell_Freq(Stat_Cell_Freq item)
	{
		// String samKey = RowKeyMaker.DtLteCellKey(0, 100, item.iCI,
		// item.startTime);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.freq);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);

		sb.append(item.RSRP_nTotal);
		sb.append(TabMark);
		sb.append(item.RSRP_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.RSRP_nCount.length; i++)
		{
			sb.append(item.RSRP_nCount[i]);
			sb.append(TabMark);
		}

		sb.append(item.RSRQ_nTotal);
		sb.append(TabMark);
		sb.append(item.RSRQ_nSum);
		sb.append(TabMark);

		for (int i = 0; i < item.RSRQ_nCount.length; i++)
		{
			if (i == item.RSRQ_nCount.length - 1)
			{
				sb.append(item.RSRQ_nCount[i]);
			}
			else
			{
				sb.append(item.RSRQ_nCount[i]);
				sb.append(TabMark);
			}
		}
		sb.append(TabMark);
		sb.append(item.RSRP_nCount7);
		sb.append(TabMark);
		sb.append(item.pci);
		return sb.toString();
	}

	public static String getPutCell_2G(Stat_Cell_2G item)
	{
		// String samKey = RowKeyMaker.DtLteCellKey(0, 100, item.iCI,
		// item.startTime);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.xdrCount);

		return sb.toString();
	}

	public static String getPutCell_3G(Stat_Cell_3G item)
	{
		// String samKey = RowKeyMaker.DtLteCellKey(0, 100, item.iCI,
		// item.startTime);

		StringBuffer sb = new StringBuffer();
		// sb.append(samKey);sb.append(TabMark);

		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.iLAC);
		sb.append(TabMark);
		sb.append(item.iCI);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.iduration);
		sb.append(TabMark);
		sb.append(item.idistance);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.xdrCount);

		return sb.toString();
	}

	public static String getPutUserGridInfo(Stat_UserGrid_4G item)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(item.startTime);
		sb.append(TabMark);
		sb.append(item.endTime);
		sb.append(TabMark);
		sb.append(item.itllongitude);
		sb.append(TabMark);
		sb.append(item.itllatitude);
		sb.append(TabMark);
		sb.append(item.ibrlongitude);
		sb.append(TabMark);
		sb.append(item.ibrlatitude);
		sb.append(TabMark);
		sb.append(item.isamplenum);
		sb.append(TabMark);
		sb.append(item.userCount_4G);

		return sb.toString();
	}

	public static String getPutUserActCell(Stat_UserAct_Cell item)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(item.icityid);
		sb.append(TabMark);
		sb.append(Func.getEncrypt(item.imsi));
		sb.append(TabMark);
		sb.append(Func.getEncrypt(item.msisdn));
		sb.append(TabMark);
		sb.append(item.stime);
		sb.append(TabMark);
		sb.append(item.etime);
		sb.append(TabMark);
		sb.append(item.eci);
		sb.append(TabMark);
		sb.append(item.sn);
		sb.append(TabMark);
		sb.append(item.nbeci);
		sb.append(TabMark);
		sb.append(item.rsrpSum);
		sb.append(TabMark);
		sb.append(item.rsrpTotal);
		sb.append(TabMark);
		sb.append(item.rsrpMaxMark);
		sb.append(TabMark);
		sb.append(item.rsrpMinMark);

		return sb.toString();
	}

	public static String getPutUerInfo(UserInfoMng.UserInfo item)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(0);
		sb.append(TabMark); // cityid
		sb.append(Func.getEncrypt(item.imsi));
		sb.append(TabMark);
		sb.append(item.xdrCount);
		sb.append(TabMark);
		sb.append(item.brand);
		sb.append(TabMark);
		sb.append(item.type);
		sb.append(TabMark);
		sb.append(Func.getEncrypt(item.msisdn));
		sb.append(TabMark);
		sb.append(item.imei);
		sb.append(TabMark);
		sb.append(item.IPDataUL);
		sb.append(TabMark);
		sb.append(item.IPDataDurationUL);
		sb.append(TabMark);
		sb.append(item.IPDataDL);
		sb.append(TabMark);
		sb.append(item.IPDataDurationDL);
		sb.append(TabMark);
		sb.append(item.ServiceTypeList);
		sb.append(TabMark);
		sb.append(item.ServiceTypeCountList);
		sb.append(TabMark);
		sb.append(item.NetType);
		sb.append(TabMark);
		sb.append(item.EciList);
		sb.append(TabMark);
		sb.append(item.EciCount);

		return sb.toString();
	}

	public static String getPutUerAct(UserActStat item)
	{
		StringBuffer sb = new StringBuffer();

		for (UserActTime userActTime : item.userActTimeMap.values())
		{
			for (UserAct userAct : userActTime.userActMap.values())
			{
				sb.delete(0, sb.length());

				sb.append(0);
				sb.append(TabMark); // cityid
				sb.append(item.imsi);
				sb.append(TabMark);
				sb.append(item.msisdn);
				sb.append(TabMark);
				sb.append(userActTime.stime);
				sb.append(TabMark);
				sb.append(userActTime.etime);
				sb.append(TabMark);
				sb.append(userAct.eci);
				sb.append(TabMark);
				sb.append(userAct.sn);
				sb.append(TabMark);
				sb.append(userActTime.cellcount);
				sb.append(TabMark);
				sb.append(userActTime.xdrcountTotal);
				sb.append(TabMark);
				sb.append(userActTime.duration);
				sb.append(TabMark);
				sb.append(userAct.xdrcount);
				sb.append(TabMark);
				sb.append(userAct.cellduration);
				sb.append(TabMark);
				sb.append(userAct.durationrate);
				sb.append(TabMark);

				sb.append(userAct.xdrcount);

				return sb.toString();
			}
		}
		return "";
	}

	public static String getPutImsiSampleIndex(long imsi, int time, String blockid)
	{
		String samKey = RowKeyMaker.DtLteSampleIndexKey(0, 100, imsi, time / 3600 * 3600, blockid);

		StringBuffer sb = new StringBuffer();
		sb.append(samKey);
		sb.append(TabMark);
		sb.append(0);
		return sb.toString();
	}

	public static String getPutImsiEventIndex(long imsi, int time, String blockid)
	{
		String samKey = RowKeyMaker.DtLteEventIndexKey(0, 100, imsi, time / 3600 * 3600, blockid);

		StringBuffer sb = new StringBuffer();
		sb.append(samKey);
		sb.append(TabMark);
		sb.append(0);
		return sb.toString();
	}

	public static String getPutHttpXdr(SIGNAL_XDR_4G item)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(item.stime);
		sb.append(TabMark);
		sb.append(item.etime);
		sb.append(TabMark);
		sb.append(item.IMSI);
		sb.append(TabMark);
		sb.append(item.IMEI);
		sb.append(TabMark);
		sb.append(item.MSISDN);
		sb.append(TabMark);
		sb.append(item.TAC);
		sb.append(TabMark);
		sb.append(item.Eci);
		sb.append(TabMark);
		sb.append(item.Service_Type);
		sb.append(TabMark);
		sb.append(item.Sub_Service_Type);
		sb.append(TabMark);
		sb.append(item.host);
		sb.append(TabMark);
		sb.append(item.longitude);
		sb.append(TabMark);
		sb.append(item.latitude);
		sb.append(TabMark);
		sb.append(item.Procedure_Status);
		sb.append(TabMark);
		sb.append(item.HTTP_WAP_status);
		sb.append(TabMark);
		sb.append(item.IP_LEN_UL);
		sb.append(TabMark);
		sb.append(item.IP_LEN_DL);

		return sb.toString();
	}
}

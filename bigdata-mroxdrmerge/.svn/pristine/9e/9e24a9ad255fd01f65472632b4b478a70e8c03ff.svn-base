package cn.mastercom.bigdata.mro.stat;

import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.struct.CSStat_GridFreq;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;

public class CSTenGridFreqStatdo_4G  extends AMapStatDo_4G<CSStat_GridFreq>{
	int i,sourceType;
	public CSTenGridFreqStatdo_4G(int i,ResultOutputer resultOutputer, int sourceType1,int sourceType, int dataType) {
		super(resultOutputer, sourceType1, dataType);
		this.i=i;
		this.sourceType=sourceType;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		List<NC_LTE> itemList = sample.getNclte_Freq();
		int tllongitude = (int) ((long) sample.ilongitude / 1000 * 1000);
		int tllatitude = (int) ((long) sample.ilatitude / 900 * 900 + 900);
		int brlongitude = tllongitude + 1000;
		int brlatitude = tllatitude - 900;
		return new Object[] {itemList.get(i).LteNcEarfcn,sample.cityID,tllongitude,tllatitude,brlongitude,brlatitude,itemList.get(i).LteNcPci,TimeHelper.getRoundDayTime(sample.itime)};
	}

	@Override
	protected CSStat_GridFreq createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		int longitude = (int) ((long) sample.ilongitude /1000 * 1000);
		int latitude = (int) ((long) sample.ilatitude / 900 * 900 + 900);
		int brlongitude = longitude + 1000;
		int brlatitude = latitude - 900;
		List<NC_LTE> itemList = sample.getNclte_Freq();
		CSStat_GridFreq CSGridFreq=new CSStat_GridFreq();
		CSGridFreq.doFirstSample(sample, itemList.get(i), longitude, latitude,brlongitude,brlatitude);
		return CSGridFreq;
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		boolean istrue;
		List<NC_LTE> itemList = sample.getNclte_Freq();
		istrue=(sample.flag.equals("MRO")||sample.flag.equals("MRE"))&&sample.ilongitude>0
				&&sample.ilatitude>0&&i<itemList.size()&&sample.itime>0;
		if(sourceType==StaticConfig.TestType_DT)
				istrue=istrue&&(sample.testType == StaticConfig.TestType_DT);
		if(sourceType==StaticConfig.TestType_CQT)
				istrue=istrue&&(sample.testType == StaticConfig.TestType_CQT);
		return istrue;
	}

}
 

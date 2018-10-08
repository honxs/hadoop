package cn.mastercom.bigdata.mro.stat;

import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.AMapStatDo_4G;
import cn.mastercom.bigdata.mro.stat.struct.CSStat_CellGrid;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.TimeHelper;
/**
 * Created by tanpeng on 2018/8/14.
 */
public class CSTenCellGridStatdo_4G extends AMapStatDo_4G<CSStat_CellGrid>{
	int sourceType;
	public CSTenCellGridStatdo_4G(ResultOutputer resultOutputer, int sourceType1,int sourceType,int dataType) {
		
		super(resultOutputer, sourceType1, dataType);
		this.sourceType=sourceType;
	}

	@Override
	protected Object[] getPartitionKeys(DT_Sample_4G sample) {
		int itllongitude= (int) ((long) sample.ilongitude/1000 * 1000);
		int itllatitude = (int) ((long) sample.ilatitude/ 900 * 900 + 900);
		int ibrlongitude= itllongitude + 1000;
		int ibrlatitude = itllatitude - 900;
	    return new Object[] {sample.Eci,sample.cityID,itllongitude,itllatitude,ibrlongitude,ibrlatitude,TimeHelper.getRoundDayTime(sample.itime)};
	}

	@Override
	protected CSStat_CellGrid createFirstStatItem(DT_Sample_4G sample, Object[] keys) {
		CSStat_CellGrid CS_CellGrid = new CSStat_CellGrid();
		CS_CellGrid.doFirstSample(sample,true,false);//doFirstSample(sample,isten,isGrid); 
		return CS_CellGrid;							//isten：是否统计10*10栅格   	isGrid：是否统计的仅仅是栅格
	}

	@Override
	protected boolean statOrNot(DT_Sample_4G sample) {
		boolean istrue;
		istrue=sample.itime > 0&&sample.ilongitude>0&&sample.ilatitude>0&&
				(sample.flag.equals("EVT")||sample.flag.equals("MRO")||sample.flag.equals("MRE"))
				&&sample.Eci > 0&&sample.Eci < Integer.MAX_VALUE;
			if(sourceType==StaticConfig.TestType_DT)
				istrue=istrue&&(sample.testType == StaticConfig.TestType_DT);
			if(sourceType==StaticConfig.TestType_CQT)
			istrue=istrue&&(sample.testType == StaticConfig.TestType_CQT);
		return istrue;
	}
	

}
package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;
import cn.mastercom.bigdata.util.FormatTime;

public class ResidentMergeDo implements IMergeDataDo,Serializable
{

	public int dataType;
	public Stat_Resident stat_resident = new Stat_Resident();
	private StringBuffer sbTemp = new StringBuffer();

	public String getMapKey() {
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(stat_resident.cityID);
		sbTemp.append("_");
		sbTemp.append(stat_resident.IMSI);
		sbTemp.append("_");
		sbTemp.append(stat_resident.hour);
		sbTemp.append("_");
		sbTemp.append(stat_resident.eci);
		
		return sbTemp.toString();
		

	}

	@Override
	public int getDataType() {
		return dataType;
	}

	@Override
	public int setDataType(int dataType) {
		this.dataType = dataType;
		return 0;
	}
	

	@Override
	public boolean mergeData(Object o)
	{
		ResidentMergeDo temp = (ResidentMergeDo) o;
		
		stat_resident.RSRPValue += temp.stat_resident.RSRPValue;
		stat_resident.MRCnt += temp.stat_resident.MRCnt;
		stat_resident.LteNcRSRP1Value += temp.stat_resident.LteNcRSRP1Value;
		stat_resident.LteNcRSRP1Cnt += temp.stat_resident.LteNcRSRP1Cnt;
		stat_resident.LteNcRSRP2Value += temp.stat_resident.LteNcRSRP2Value;
		stat_resident.LteNcRSRP2Cnt += temp.stat_resident.LteNcRSRP2Cnt;
		stat_resident.LteNcRSRP3Value += temp.stat_resident.LteNcRSRP3Value;
		stat_resident.LteNcRSRP3Cnt += temp.stat_resident.LteNcRSRP3Cnt;
		stat_resident.LteNcRSRP4Value += temp.stat_resident.LteNcRSRP4Value;
		stat_resident.LteNcRSRP4Cnt += temp.stat_resident.LteNcRSRP4Cnt;
		stat_resident.LteNcRSRP5Value += temp.stat_resident.LteNcRSRP5Value;
		stat_resident.LteNcRSRP5Cnt += temp.stat_resident.LteNcRSRP5Cnt;
		stat_resident.LteNcRSRP6Value += temp.stat_resident.LteNcRSRP6Value;
		stat_resident.LteNcRSRP6Cnt += temp.stat_resident.LteNcRSRP6Cnt;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try 
		{
			
			
			stat_resident = Stat_Resident.FillData(vals, 0);
			
		}    
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData()
	{ 
	
		String res=stat_resident.cityID+"\t"+stat_resident.IMSI+"\t"+stat_resident.hour+"\t"+stat_resident.eci;
		res+="\t"+stat_resident.RSRPValue+"\t"+stat_resident.MRCnt;
		//if(stat_resident.LteNcEci1!=0||stat_resident.LteNcRSRP1Value!=0){
			res+="\t"+stat_resident.LteNcEci1+"\t"+stat_resident.LteNcRSRP1Value+"\t"+stat_resident.LteNcRSRP1Cnt;
		//}
		
		//if(stat_resident.LteNcEci2!=0||stat_resident.LteNcRSRP2Value!=0){
			res+="\t"+stat_resident.LteNcEci2+"\t"+stat_resident.LteNcRSRP2Value+"\t"+stat_resident.LteNcRSRP2Cnt;
		//}
		 
		//if(stat_resident.LteNcEci3!=0||stat_resident.LteNcRSRP3Value!=0){
			res+="\t"+stat_resident.LteNcEci3+"\t"+stat_resident.LteNcRSRP3Value+"\t"+stat_resident.LteNcRSRP3Cnt;
		//}
		
		//if(stat_resident.LteNcEci4!=0||stat_resident.LteNcRSRP4Value!=0){
			res+="\t"+stat_resident.LteNcEci4+"\t"+stat_resident.LteNcRSRP4Value+"\t"+stat_resident.LteNcRSRP4Cnt;
		//}
		//if(stat_resident.LteNcEci5!=0||stat_resident.LteNcRSRP5Value!=0){
			res+="\t"+stat_resident.LteNcEci5+"\t"+stat_resident.LteNcRSRP5Value+"\t"+stat_resident.LteNcRSRP5Cnt;
		//}
		
		//if(stat_resident.LteNcEci6!=0||stat_resident.LteNcRSRP6Value!=0){
			res+="\t"+stat_resident.LteNcEci6+"\t"+stat_resident.LteNcRSRP6Value+"\t"+stat_resident.LteNcRSRP6Cnt;
		//}
		   
		return res;
	}

}
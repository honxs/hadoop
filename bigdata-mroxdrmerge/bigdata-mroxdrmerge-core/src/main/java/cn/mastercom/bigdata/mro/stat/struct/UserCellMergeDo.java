package cn.mastercom.bigdata.mro.stat.struct;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;


public class UserCellMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public UserCell usercell = new UserCell();
	private StringBuffer sbTemp = new StringBuffer();

	@Override
	public String getMapKey()
	{
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(usercell.CityID);
		sbTemp.append("_");
		sbTemp.append(usercell.ECI);
		sbTemp.append("_");
		sbTemp.append(usercell.Time);
		sbTemp.append("_");
		sbTemp.append(usercell.Imsi);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		UserCellMergeDo temp = (UserCellMergeDo) o;
		usercell.MRCnt += temp.usercell.MRCnt;
		usercell.MRRSRQCnt += temp.usercell.MRRSRQCnt;
		usercell.MRSINRCnt += temp.usercell.MRSINRCnt;
		usercell.RSRPValue += temp.usercell.RSRPValue;
		usercell.RSRQValue += temp.usercell.RSRQValue;
		usercell.SINRValue += temp.usercell.SINRValue;
		usercell.MRCnt_95 += temp.usercell.MRCnt_95;
		usercell.MRCnt_100 += temp.usercell.MRCnt_100;
		usercell.MRCnt_103 += temp.usercell.MRCnt_103;
		usercell.MRCnt_105 += temp.usercell.MRCnt_105;
		usercell.MRCnt_110 += temp.usercell.MRCnt_110;
		usercell.MRCnt_113 += temp.usercell.MRCnt_113;
		usercell.MRCnt_128 += temp.usercell.MRCnt_128;
		usercell.RSRP100_SINR0 += temp.usercell.RSRP100_SINR0;
		usercell.RSRP105_SINR0 += temp.usercell.RSRP105_SINR0;
		usercell.RSRP110_SINR3 += temp.usercell.RSRP110_SINR3;
		usercell.RSRP110_SINR0 += temp.usercell.RSRP110_SINR0;
		usercell.SINR_0 += temp.usercell.SINR_0;
		usercell.RSRQ_14 += temp.usercell.RSRQ_14;
		usercell.FGCnt += temp.usercell.FGCnt;
		usercell.OTTCnt += temp.usercell.OTTCnt;
		usercell.RUCnt += temp.usercell.RUCnt;
		usercell.WLANCnt += temp.usercell.WLANCnt;
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		try
		{
			usercell = UserCell.FillData(vals, sPos);
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
		return usercell.mergeStatToLine();
	}

}

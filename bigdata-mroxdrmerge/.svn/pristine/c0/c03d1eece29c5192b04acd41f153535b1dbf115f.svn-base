package cn.mastercom.bigdata.mdt.stat;

import java.io.Serializable;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

public class MdtImeiMergeDo implements IMergeDataDo, Serializable
{
	private int dataType = 0;
	public Stat_mdt_imei mdtImeiStruct = new Stat_mdt_imei();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey() {
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(mdtImeiStruct.iCityID);
		sbTemp.append("_");
		sbTemp.append(mdtImeiStruct.IMEI_TAC);
		sbTemp.append("_");
		sbTemp.append(mdtImeiStruct.iTime);
		return sbTemp.toString();
	}

	@Override
	public int getDataType() {
		// TODO Auto-generated method stub
		return dataType;
	}

	@Override
	public int setDataType(int dataType) {
		// TODO Auto-generated method stub
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o) {
		// TODO Auto-generated method stub
		MdtImeiMergeDo temp = (MdtImeiMergeDo) o;
		mdtImeiStruct.im_mdt_total += temp.mdtImeiStruct.im_mdt_total;
		mdtImeiStruct.im_mdt_loc_80 += temp.mdtImeiStruct.im_mdt_loc_80;
		mdtImeiStruct.im_mdt_loc_60 += temp.mdtImeiStruct.im_mdt_loc_60;
		mdtImeiStruct.im_mdt_loc_40 += temp.mdtImeiStruct.im_mdt_loc_40;
		mdtImeiStruct.im_mdt_loc_20 += temp.mdtImeiStruct.im_mdt_loc_20;
		mdtImeiStruct.im_mdt_loc_0 += temp.mdtImeiStruct.im_mdt_loc_0;
		mdtImeiStruct.logged_mdt_total += temp.mdtImeiStruct.logged_mdt_total;
		mdtImeiStruct.logged_mdt_loc_80 += temp.mdtImeiStruct.logged_mdt_loc_80;
		mdtImeiStruct.logged_mdt_loc_60 += temp.mdtImeiStruct.logged_mdt_loc_60;
		mdtImeiStruct.logged_mdt_loc_40 += temp.mdtImeiStruct.logged_mdt_loc_40;
		mdtImeiStruct.logged_mdt_loc_20 += temp.mdtImeiStruct.logged_mdt_loc_20;
		mdtImeiStruct.logged_mdt_loc_0 += temp.mdtImeiStruct.logged_mdt_loc_0;
		mdtImeiStruct.rlf_mdt_total += temp.mdtImeiStruct.rlf_mdt_total;
		mdtImeiStruct.rlf_mdt_loc_80 += temp.mdtImeiStruct.rlf_mdt_loc_80;
		mdtImeiStruct.rlf_mdt_loc_60 += temp.mdtImeiStruct.rlf_mdt_loc_60;
		mdtImeiStruct.rlf_mdt_loc_40 += temp.mdtImeiStruct.rlf_mdt_loc_40;
		mdtImeiStruct.rlf_mdt_loc_20 += temp.mdtImeiStruct.rlf_mdt_loc_20;
		mdtImeiStruct.rlf_mdt_loc_0 += temp.mdtImeiStruct.rlf_mdt_loc_0;
		mdtImeiStruct.rcef_mdt_total += temp.mdtImeiStruct.rcef_mdt_total;
		mdtImeiStruct.rcef_mdt_loc_80 += temp.mdtImeiStruct.rcef_mdt_loc_80;
		mdtImeiStruct.rcef_mdt_loc_60 += temp.mdtImeiStruct.rcef_mdt_loc_60;
		mdtImeiStruct.rcef_mdt_loc_40 += temp.mdtImeiStruct.rcef_mdt_loc_40;
		mdtImeiStruct.rcef_mdt_loc_20 += temp.mdtImeiStruct.rcef_mdt_loc_20;
		mdtImeiStruct.rcef_mdt_loc_0 += temp.mdtImeiStruct.rcef_mdt_loc_0;
		
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos) {
		// TODO Auto-generated method stub
		try
		{
			mdtImeiStruct = Stat_mdt_imei.FillData(vals, 0);
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	@Override
	public String getData() {
		// TODO Auto-generated method stub
		return mdtImeiStruct.toLine();
	}

}

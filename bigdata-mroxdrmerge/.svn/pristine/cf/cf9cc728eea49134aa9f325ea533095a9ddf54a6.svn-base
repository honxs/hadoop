package cn.mastercom.bigdata.stat.userResident.buildIndoorCell;

import cn.mastercom.bigdata.stat.userResident.homeBroadbandLoc.NewResidentUser;
import cn.mastercom.bigdata.util.DataGeter;

public class BuildIndoorCellUser
{
	public int cityID;
	public int buildId;
	public int height;
	public int position;
	public long eci;
	public int userCnt;
	public int indoorCellUserCnt;
	
	public static final String spliter = "\t";

	public BuildIndoorCellUser()
	{
		clean();
	}

	public void clean()
	{
		cityID = -1;
		buildId = -1;
		height = -1;
		position = 0;
		eci = -1000000;
		userCnt = 0;
		indoorCellUserCnt = 0;
	}
	
	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(userCnt);
		sb.append(spliter);
		sb.append(indoorCellUserCnt);
		return sb.toString();
	}
	
	public String toLine()
	{
		StringBuffer sb = new StringBuffer();
		sb.delete(0, sb.length());
		sb.append(cityID);
		sb.append(spliter);
		sb.append(buildId);
		sb.append(spliter);
		sb.append(height);
		sb.append(spliter);
		sb.append(position);
		sb.append(spliter);
		sb.append(userCnt);
		sb.append(spliter);
		sb.append(indoorCellUserCnt);
		return sb.toString();
	}
	
	public void fillData(NewResidentUser residentUser)
	{
		cityID = residentUser.cityID;
		buildId = residentUser.buildId;
		height = residentUser.height;
		position = residentUser.position;
		eci = residentUser.eci;
		
	}
	
	public static BuildIndoorCellUser fillData2(String args[])
	{
		BuildIndoorCellUser item = new BuildIndoorCellUser();
		int i = 0;
		item.cityID = DataGeter.GetInt(args[i++]);
		item.buildId = DataGeter.GetInt(args[i++]);
		item.height = DataGeter.GetInt(args[i++]);
		item.eci = DataGeter.GetLong(args[i++]);
		item.userCnt = DataGeter.GetInt(args[i++]);
		item.indoorCellUserCnt = DataGeter.GetInt(args[i++]);
		return item;
	}
	
	public void updateNum(BuildIndoorCellUser buildIndoorCellUser)
	{
		indoorCellUserCnt++;
	}
}

package cn.mastercom.bigdata.locuser_v3;

import java.util.HashMap;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;

public class BuildAna
{
	public HashMap<Integer, IndoorErrResult> AnaBuild(DataUnit dataUnit, ReportProgress rptProgress)
	{
		HashMap<Integer, IndoorErrResult> tempResult = new HashMap<Integer, IndoorErrResult>();
		LteCellInfo cellInfo;
		try
		{
			for (EciUnit eunit : dataUnit.eciUnits.values())
			{
				for (MrUser mu : eunit.muser.values())
				{
					for (MrSec msc : mu.sections)
					{
						for (MrSplice sl : msc.splices)
						{
							if (sl.scell == null)
							{
								sl.scell = new MrPoint();
							}
							if (sl.nicell == null)
							{
								sl.nicell = new MrPoint();
							}
							if (sl.nocell == null)
							{
								sl.nocell = new MrPoint();
							}
							if (sl.scell.cell.isindoor == 1)
							{
								IndoorErrResult indoortemp = tempResult.get(sl.eci);
								if (indoortemp == null)
								{
									indoortemp = new IndoorErrResult();
									indoortemp.eci = sl.eci;
									cellInfo = CellConfig.GetInstance().getLteCell(sl.eci);
		
									if (cellInfo != null)
									{
										indoortemp.eciName = cellInfo.cellName;
										indoortemp.coverType = cellInfo.indoor == 1 ? "室内" : "室外";
										indoortemp.cityid = cellInfo.cityid;
									}
									else
									{
										continue;
									}
									tempResult.put(sl.eci, indoortemp);
								}
								indoortemp.dealSpliter(sl);
							}
						}
					}
				}
			}
			for (IndoorErrResult temp : tempResult.values())
			{
				temp.setAvg_rsrp();
				temp.statRate();
			}
		}
		catch (Exception e)
		{
			rptProgress.writeLog(0, "BuildAna:" + e.getMessage());
		}
		return tempResult;
	}
}

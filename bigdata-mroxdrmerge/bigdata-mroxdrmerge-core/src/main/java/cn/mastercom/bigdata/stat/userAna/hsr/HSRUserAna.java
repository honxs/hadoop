package cn.mastercom.bigdata.stat.userAna.hsr;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import cn.mastercom.bigdata.stat.userAna.model.StationImsi;

public class HSRUserAna
{
	public int init(Configuration conf, String hsrStationPath, String hsrLinePath, String hsrStationCellPath, String hsrLineCellPath, String hsrSectionRruPath, String hsrSegPath, String hsrIndoorPath)
	{
		try
		{
			if(!HSRConfigHelper.getInstance().checkInited())
				HSRConfigHelper.getInstance().readConfig(conf, hsrStationPath, hsrLinePath, hsrStationCellPath, hsrLineCellPath, hsrSectionRruPath, hsrSegPath, hsrIndoorPath);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
		return 0;
	}

	public int statData(RailSecImsi railSegImsi, HSRData hsrData)
	{
		Worker w = new Worker();
		w.DoWork(railSegImsi, hsrData);
		return 0;
	}

	public List<RailSecImsi> statData(List<StationImsi> stationImsiList)
	{
		Worker w = new Worker();
		return w.DoWork(stationImsiList);
	}
	
}

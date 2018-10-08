package cn.mastercom.bigdata.locuser_v2;

import java.util.List;

import cn.mastercom.bigdata.StructData.NC_LTE;
import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;


public class SamFiles
{
	public SamList sList = new SamList();

    public SamFiles(ReportProgress rptP)
    {
    }
	
    public boolean GetNext(DataUnit dataUnit, List<SIGNAL_MR_All> lsams)
    {
    	if (lsams.size() == 0)
    	{
    		return false;
    	}
    	
    	for (int ii = 0; ii < lsams.size(); ii++)
    	{
    		SIGNAL_MR_All mall = lsams.get(ii);
    		
    		MrSam sam = new MrSam();
    	
			sam.mall = mall;
			sam.itime = mall.tsc.beginTime;
			sam.cityid = mall.tsc.cityID;
			sam.s1apid = mall.tsc.MmeUeS1apId;
			sam.mrotype = mall.tsc.EventType;
			sam.ta = mall.tsc.LteScTadv;
			sam.aoa = mall.tsc.LteScAOA;

			sam.scell.btime = mall.tsc.beginTime;
			sam.scell.etime = mall.tsc.beginTime;
			sam.scell.eci = (int)(mall.tsc.Eci);
			sam.scell.pciarfcn = mall.tsc.LteScPci * 65536 + mall.tsc.LteScEarfcn;		
			sam.scell.rsrp_avg = mall.tsc.LteScRSRP;
			sam.scell.rsrq_avg = mall.tsc.LteScRSRQ;			
			if (sam.scell.rsrp_avg != -1000000)
			{
				sam.scell.rsrp_cnt = 1;
			}
			if (sam.scell.rsrq_avg != -1000000)
			{
				sam.scell.rsrq_cnt = 1;
			}

			for (int kk = 0; kk < mall.nccount[0]; kk++)
			{
				NC_LTE SamField = mall.tlte[kk];

				Mrcell ncell = new Mrcell();
				
				ncell.rsrp_avg = SamField.LteNcRSRP;
				ncell.rsrq_avg = SamField.LteNcRSRQ;
				int earfcn = SamField.LteNcEarfcn;
				int pci = SamField.LteNcPci;

				if (earfcn != -1000000 && pci != -1000000 && pci != 0 && earfcn != 0)
				{
					if (ncell.rsrp_avg != -1000000)
					{
						ncell.btime = sam.itime;
						ncell.etime = sam.itime;
						ncell.pciarfcn = pci *65536 + earfcn;
						ncell.rsrp_cnt = 1;
						if (ncell.rsrq_avg != -1000000)
						{
							ncell.rsrq_cnt = 1;
						}
						
						sam.ncells.add(ncell);
					}
				}
			}
    	
    		sList.SetSams(sam, dataUnit);
    	}
    	
    	return true;
    }
}

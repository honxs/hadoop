package cn.mastercom.bigdata.locuser_v3;

import java.util.HashMap;
import java.util.Map;

public class EciUnit
{
	public AOATAPrint aoataprint = new AOATAPrint();
	
    public int cityid = 0;
    public int eci = 0;
    // 采样点
    public Map<Long, MrUser> muser = new HashMap<Long, MrUser>();
    // 指纹库
    public FingerPrint finger = new FingerPrint();
    // 当前场强二维矩阵
    public RsrpTable rtable = new RsrpTable();
    // ta峰值
    public static int TACNT = 200;
    public static int RPCNT = 81;
    public int[][] ta_peak = new int[TACNT][];// inrsrp,otrsrp,-1000000
    
    public void Clear()
    {
        eci = 0;
        muser.clear();
        finger.Clear();
        rtable.Clear();
    	for(int i = 0 ; i < ta_peak.length; i++ )
    	{
    		ta_peak[i] = null;
    	}
    }
    
    public void SetPeak()
    {
        int[][] ta_rsrp = new int[TACNT][];
    	for(int i = 0 ; i < ta_rsrp.length; i++ )
    	{
    		ta_rsrp[i] = null;
    	}
    	for(int i = 0 ; i < ta_peak.length; i++ )
    	{
    		ta_peak[i] = null;
    	}

        for (MrUser mm : muser.values())
        {
            for (MrSam ms : mm.samples)
            {
                if (ms.ta >= 0 && ms.ta < EciUnit.TACNT && ms.scell.rsrp_avg != -1000000 && ms.scell.rsrp_cnt > 0)
                {
                    int rsrp = (int)(ms.scell.rsrp_avg / ms.scell.rsrp_cnt);
                    if (rsrp >= -130 && rsrp <= -50)
                    {
                        if (ta_rsrp[ms.ta] == null)
                        {
                            ta_rsrp[ms.ta] = new int[RPCNT];
                        	for(int i = 0 ; i < ta_rsrp[ms.ta].length; i++ )
                        	{
                        		ta_rsrp[ms.ta][i] = 0;
                        	}
                            ta_peak[ms.ta] = new int[] { -1000000, -1000000 };
                        }
                        ta_rsrp[ms.ta][rsrp + 130]++;
                    }
                }
            }
        }

        for (int ii = 0; ii < TACNT; ii++)
        {
            if (ta_rsrp[ii] != null)
            {
                PeakValue.GetSeak(ta_rsrp[ii], 10, ta_peak[ii]);
            }
        }

        ta_rsrp = null;
    }
    // s1apid数量
    public int siCount()
    {
        return muser.size();
    }
    // splice数量
    public long spCount()
    {
    	long nCount = 0;
        for (MrUser mu : muser.values())
        {
        	for (MrSec ms : mu.sections)
            {
                nCount += ms.splices.size();
            }
        }
        return nCount;
    }
    // section数量
    public long scCount()
    {
    	long nCount = 0;
    	for (MrUser mu : muser.values())
        {
            nCount += mu.sections.size();
        }
        return nCount;
    }
}

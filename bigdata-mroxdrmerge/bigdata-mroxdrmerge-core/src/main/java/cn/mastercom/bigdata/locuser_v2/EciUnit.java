package cn.mastercom.bigdata.locuser_v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EciUnit
{
    public int eci = 0;
    // 采样点
    public Map<Long, ArrayList<MrSam>> samples = new HashMap<Long, ArrayList<MrSam>>();
    // 切片
    public List<MrSplice> splices = new ArrayList<MrSplice>();
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
        samples.clear();
        splices.clear();
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

        for (ArrayList<MrSam> mm : samples.values())
        {
            for (MrSam ms : mm)
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
}

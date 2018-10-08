package cn.mastercom.bigdata.locuser_v2;

import java.util.ArrayList;
import java.util.List;

public class PeakValue
{
    public static void GetSeak(int[] values, int wsize, int[] peak)
    {
        List<Integer> lo_listsn = new ArrayList<Integer>();
        int lo_ileft = 0;
        int lo_iright = 0;
        int lo_maxcount = 0;
        long lo_havedatanum = 0;//有数据的个数
        long lo_havedatasample = 0;//有数据的采样点数
        for (int ii = 3; ii < values.length - 3; ii++)
        {
            if (values[ii] > 0)
            {
                lo_havedatanum++;
                lo_havedatasample += values[ii];
                if ((values[ii] >= values[ii - 1]) && (values[ii] >= values[ii - 2]) && (values[ii] >= values[ii - 3]) &&
                    (values[ii] >= values[ii + 1]) && (values[ii] >= values[ii + 2]) && (values[ii] >= values[ii + 3]))
                {
                    lo_listsn.add(ii);
                    if (values[ii] > lo_maxcount)
                    {
                        lo_maxcount = values[ii];
                    }
                }
            }
        }
        
        if ((lo_havedatasample < 20) || (lo_havedatanum < 10))
        {
            return;
        }
        
        for (int ii = 0; ii < lo_listsn.size(); ii++)
        {
            if (values[lo_listsn.get(ii)] * 10 < lo_maxcount)
            {
                lo_listsn.remove(ii);
                ii--;
            }
        }

        if (lo_listsn.size() <= 0)
        {
            //没有明显峰值，则用RSRP的平均值左右5dB作为分界线；
        	long lo_totalvalue = 0;
        	long lo_totalcnt = 0;
            int lo_avgvalue = 0;
            for (int ii = 0; ii < values.length - 3; ii++)
            {
                lo_totalvalue += values[ii] * ii;
                lo_totalcnt += values[ii];
            }
            if (lo_totalcnt <= 0)
            {
                return;
            }
            lo_avgvalue = (int)(lo_totalvalue / lo_totalcnt);
            lo_ileft = lo_avgvalue - 5;
            lo_iright = lo_avgvalue + 5;
        }
        else if (lo_listsn.size() == 1)
        {
            lo_ileft = lo_listsn.get(0) - 5;
            lo_iright = lo_listsn.get(0) + 5;
        }
        else
        {
            lo_ileft = lo_listsn.get(0);
            lo_iright = lo_listsn.get(lo_listsn.size() - 1);
            if (lo_iright - lo_ileft > 10)
            {
                int lo_avg = (lo_iright + lo_ileft) / 2;
                lo_ileft = lo_avg - 5;
                lo_iright = lo_avg + 5;
            }
        }

        peak[0] = lo_ileft - 130;
        peak[1] = lo_iright - 130;    	
    }
    
    public static void GetSeak1(int[] values, int wsize, int[] peak)
    {
        List<Peaker> wData = new ArrayList<Peaker>();

        for (int ii = 0; ii <= values.length - wsize; ii++)
        {
            SumWin(values, wsize, ii, wData);
        }

        int nPeak = 0;            
        boolean bAsc = false; // 是否上升过
        List<Integer> lPeak = new ArrayList<Integer>();
        int nDesc = 0;
        for (int jj = 1; jj < wData.size(); jj++)
        {
            if (wData.get(nPeak).nMax > wData.get(jj).nMax)
            {
                nDesc++;
                if (bAsc)
                {
                    if (nDesc >= 1)
                    {                    	
                        if (!lPeak.contains(nPeak))
                        {
                            lPeak.add(nPeak);
                        }
                        nPeak = jj;
                        bAsc = false;
                        nDesc = 0;
                    }
                }
                else
                {
                    nDesc = 0;
                    nPeak = jj;
                }                    
            }
            else if (wData.get(nPeak).nMax == wData.get(jj).nMax)
            {                    
                if (wData.get(nPeak).nAvg < wData.get(jj).nAvg)
                {
                    nPeak = jj;
                }
            }
            else
            {
                bAsc = true;
                nPeak = jj;
            }
        }

        if (lPeak.size() != 2)
        {
            return;
        }

        peak[0] = wData.get(lPeak.get(0)).nPos - 130;
        peak[1] = wData.get(lPeak.get(1)).nPos - 130;

        //for (int kk = 0; kk < lPeak.Count; kk++)
        //{
        //    Trace.WriteLine(wData[lPeak[kk]].nPos.ToString() + "-" + values[wData[lPeak[kk]].nPos].ToString());
        //}            
    }

    private static void SumWin(int[] values, int wsize, int ii, List<Peaker> wData)
    {
        int nSum = 0;
        int nCnt = 0;
        Peaker pk = new Peaker();
        pk.nPos = ii;
        for (int jj = 0; jj < wsize; jj++)
        {
            if (values[jj + ii] == 0)
            {
                continue;
            }

            if (pk.nMax < values[jj + ii])
            {
                pk.nPos = jj + ii;
                pk.nMax = values[jj + ii];
            }

            nSum += values[jj + ii];
            nCnt += 1;
        }

        if (nCnt > 0)
        {
            pk.nAvg = nSum / nCnt;
        }

        wData.add(pk);
    }
}

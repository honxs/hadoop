package cn.mastercom.bigdata.locuser_v2;

public class CellOI
{
    private double[] K_m_n = new double[61];

    public double GetValue(double rsrp)
    {
        if (rsrp > -60)
        {
            return K_m_n[0];
        }
        else if (rsrp > -119)
        {
            return K_m_n[(int)(rsrp * (-1) - 60) + 1];
        }
        else
        {
            return K_m_n[60];
        }
    }

    public void Init(CellX cx)
    {
        double minK = 0;
        for (int ii = 0; ii < 61; ii++)
        {
            K_m_n[ii] = cx.GetRate(-59 - ii);

            if (minK == 0)
            {
                minK = K_m_n[ii];
            }
            else if (minK > K_m_n[ii])
            {
                minK = K_m_n[ii];
            }
        }

        for (int jj = 0; jj < 61; jj++)
        {
            if (K_m_n[jj] > 0)
            {
                K_m_n[jj] = minK / K_m_n[jj];
            }
        }
    }
}

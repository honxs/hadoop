package cn.mastercom.bigdata.locuser_v3;

public class CellX
{
    public int Kcount; // 小区覆盖室外总栅格数
    // Kin_0_60:小区覆盖室内-60dB~0dB的栅格总数
    // 小区覆盖室内-61~-60dB的栅格总数,-60~-61到-118~-119dB的栅格总数
	// Kin_119：小区覆盖室内-119~-140dB的栅格总数
    public int[] K_m_n = new int[61];

    private int GetValue(double rsrp)
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

    public double GetRate(double k)
    {
        double Kout2 = 0.2 * GetValue(k - 9) + 0.4 * GetValue(k - 8) + 0.6 * GetValue(k - 7)
            + 0.8 * GetValue(k - 6) + GetValue(k - 5) + GetValue(k - 4) + GetValue(k - 3)
            + GetValue(k - 2) + GetValue(k - 1) + GetValue(k) + GetValue(k + 1)
            + GetValue(k + 2) + GetValue(k + 3) + GetValue(k + 4) + GetValue(k + 5)
            + 0.9 * GetValue(k + 6) + 0.8 * GetValue(k + 7) + 0.7 * GetValue(k + 8) + 0.6 * GetValue(k + 9)
            + 0.5 * GetValue(k + 10) + 0.4 * GetValue(k + 11) + 0.3 * GetValue(k + 12) + 0.2 * GetValue(k + 13)
            + 0.1 * GetValue(k + 14);

        return Kout2;
    }
}

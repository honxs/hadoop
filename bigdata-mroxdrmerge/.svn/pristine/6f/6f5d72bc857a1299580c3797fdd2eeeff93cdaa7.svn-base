package cn.mastercom.bigdata.locuser_v3;

import java.util.ArrayList;

public class EGTable
{
    private static int ncount = 31;

    private EGMap[][] rpvector = new EGMap[ncount][ncount];

    public EGTable()
    {
    	for(int i = 0 ; i < rpvector.length; i++ )
    	{
    		for(int j = 0 ; j < rpvector[i].length; j++)
    		{
    			rpvector[i][j] = null;
    		}
    	}
    }

    public ArrayList<FingerGrid> GetGrid(double srsrp, double nrsrp)
    {
        int ii = GetPosi(srsrp);
        int jj = GetPosi(nrsrp);
        if (rpvector[ii][jj] == null)
        {
            return null;
        }
        return new ArrayList<FingerGrid>((rpvector[ii][jj]).egmap.values());
    }

    public void SetGrid(FingerGrid fg, double srsrp, double nrsrp)
    {
        int ii = GetPosi(srsrp);
        int jj = GetPosi(nrsrp);

        if (rpvector[ii][jj] == null)
        {
            rpvector[ii][jj] = new EGMap();
        }

        if (!(rpvector[ii][jj]).egmap.containsKey(fg.fkey))
        {
            rpvector[ii][jj].egmap.put(fg.fkey, fg);
        }
    }

    public int GetPosi(double rsrp)
    {
        int isrsrp = ((int)((rsrp) * (-1)) - 60) / 2;
        if (isrsrp < 0) isrsrp = 0;
        if (isrsrp > 30) isrsrp = 30;
        return isrsrp;
    }
}

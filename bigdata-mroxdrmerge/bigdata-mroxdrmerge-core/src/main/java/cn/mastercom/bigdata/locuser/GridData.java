package cn.mastercom.bigdata.locuser;

import java.util.HashMap;
import java.util.Map;

public class GridData
{
    public int bid = -1;
    public int level = 0;
    public int ilongitude = 0;
    public int ilatitude = 0;
    public int radius = 0;
    public int hasindoor = 0;
    public SimuData scell = null;
    public Map<Integer, SimuData> cells = new HashMap<Integer, SimuData>();
}

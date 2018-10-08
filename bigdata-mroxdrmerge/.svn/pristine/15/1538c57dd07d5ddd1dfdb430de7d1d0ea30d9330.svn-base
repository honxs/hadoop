package cn.mastercom.bigdata.loc.hsr;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.util.BitConverter;

public class LineSegModel
{

    public int lineID;
    public int regionID;
    public byte[] PointShape;
    public double distToStart;
    public double distToEnd;

    private List<PositionModel> _vertex;
    public List<PositionModel> GetVertex()
    {
        if (this._vertex == null && this.PointShape != null && this.PointShape.length > 0)
        {
            this._vertex = GetPointsFromBytes(PointShape);
        }
        return _vertex;
    }

    private List<PositionModel> GetPointsFromBytes(byte[] borderBytes)
    {
        List<PositionModel> lstPoints = new ArrayList<PositionModel>();
        if (borderBytes == null)
        {
            return lstPoints;
        }
        int pos = 0;
        while (pos < borderBytes.length)
        {
            int iLng = BitConverter.toInt(borderBytes, pos);
            double lng = 1d * iLng / 10000000;
            pos += 4;

            int iLat = BitConverter.toInt(borderBytes, pos);
            double lat = 1d * iLat / 1000000;
            pos += 4;

            lstPoints.add(new PositionModel(lng, lat));
        }
        return lstPoints;
    }
	
	
	
	
}

package cn.mastercom.bigdata.StructData;

import cn.mastercom.bigdata.conf.cellconfig.CellConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;
import cn.mastercom.bigdata.util.GisFunction;

/**
 * Created by Administrator on 2017/5/22.
 */
public class NoTypeSignal extends SIGNAL_LOC{
    public DT_Sample_4G dt_sample_4G;
    public String IMSI;

    public NoTypeSignal(){
        dt_sample_4G=new DT_Sample_4G();
    }
    
    public void fillShortData(String[] str){
    	IMSI=str[13];
    	stime = Integer.parseInt(str[3]);
    }
    

    public void fillData(String[] str){

        dt_sample_4G.fillSamData(str);

        IMSI=dt_sample_4G.IMSI+"";
        cityID=dt_sample_4G.cityID;
        stime=dt_sample_4G.itime;
        longitude=dt_sample_4G.ilongitude;
        latitude=dt_sample_4G.ilatitude;
        location=dt_sample_4G.location;
        loctp=dt_sample_4G.locType;
        radius=dt_sample_4G.radius;

    }
    public void fadbackFill(){

        dt_sample_4G.testType=testTypeGL;
        dt_sample_4G.ilongitude=longitudeGL;
        dt_sample_4G.ilatitude=latitudeGL;
        dt_sample_4G.dist=distGL;
        dt_sample_4G.radius=radiusGL;
        dt_sample_4G.locType=loctpGL;
        dt_sample_4G.indoor=indoorGL;
        dt_sample_4G.label=lableGL;
        dt_sample_4G.itime=locationGL; //TODO ?
        //TODO 
        dt_sample_4G.flag = "mro";

    }

    public String getImsi(){
        return IMSI;
    }

    @Override
    public String GetCellKey() {
        return dt_sample_4G.Eci+"";
    }

    @Override
    public int GetSampleDistance(int ilongitude, int ilatitude) {
        LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(dt_sample_4G.Eci);
        if(lteCellInfo != null)
        {
            if(longitude > 0 && latitude > 0 && lteCellInfo.ilongitude > 0 && lteCellInfo.ilatitude > 0)
            {
                return (int) GisFunction.GetDistance(ilongitude, ilatitude, lteCellInfo.ilongitude, lteCellInfo.ilatitude);
            }
        }

        return StaticConfig.Int_Abnormal;
    }

    @Override
    public int GetMaxCellRadius() {
        int maxRadius = 6000;
        LteCellInfo lteCellInfo = CellConfig.GetInstance().getLteCell(dt_sample_4G.Eci);
        if(lteCellInfo != null)
        {
            maxRadius = Math.min(maxRadius, 5*lteCellInfo.radius);
            maxRadius = Math.max(maxRadius, 1500);
        }
        return maxRadius;
    }


}

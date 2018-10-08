package cn.mastercom.bigdata.mro.loc;

import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.conf.cellconfig.LteCellInfo;

import java.util.List;

public class FillBackLocByCellBuild {
    public static void fillBackByCellBuild(List<SIGNAL_MR_All> mrList, LteCellInfo cellInfo,int buildID) {
        // buildID为0说明不是室分或者没找到buildID
        if(buildID==0){
            return;
        }
        if(cellInfo==null || cellInfo.ilongitude<=0){
            return;
        }
        for (SIGNAL_MR_All signal_mr_all : mrList) {
            if(signal_mr_all.tsc.longitude<=0){
                signal_mr_all.tsc.longitude = cellInfo.ilongitude;
                signal_mr_all.tsc.latitude = cellInfo.ilatitude;
                signal_mr_all.ibuildingID = buildID;
                //testType 是低速
                signal_mr_all.samState = StaticConfig.ACTTYPE_IN;
                signal_mr_all.locSource = StaticConfig.LOCTYPE_MID;
                signal_mr_all.testType = StaticConfig.TestType_CQT;
            }
        }
    }
}

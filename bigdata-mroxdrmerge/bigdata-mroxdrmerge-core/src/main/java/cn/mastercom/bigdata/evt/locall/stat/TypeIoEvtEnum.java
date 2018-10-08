package cn.mastercom.bigdata.evt.locall.stat;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/13.
 */
public enum  TypeIoEvtEnum implements IOutPutPathEnum {
	
	
	ORIGINMME("tbEventOriginMme","/TB_EVENT_ORIGIN_MME_"),
	ORIGINMW("tbEventOriginMw","/TB_EVENT_ORIGIN_MW_"),
	ORIGINSV("tbEventOriginSv","/TB_EVENT_ORIGIN_SV_"),
	ORIGINRX( "tbEventOriginRx","/TB_EVENT_ORIGIN_Rx_"),
	ORIGINHTTP( "tbEventOriginS1uHttp","/TB_EVENT_ORIGIN_S1U_HTTP_"),
	ORIGINMG("tbEventOriginMg","/TB_EVENT_ORIGIN_MG_"),
	ORIGINRTP( "tbEventOriginRtp","/TB_EVENT_ORIGIN_RTP_"),
	ORIGINMOS_BEIJING("tbEventOriginMosBeiJing","/TB_EVENT_ORIGIN_MOS_BEIJING_"),
	ORIGINWJTDH_BEIJING("tbEventOriginMjtdhBeiJing","/TB_EVENT_ORIGIN_MJTDH_BEIJING_"),
	ORIGINIMS_MO("tbEventOriginImsMo","/TB_EVENT_ORIGIN_Ims_Mo_"),
	ORIGINIMS_MT("tbEventOriginImsMt","/TB_EVENT_ORIGIN_Ims_Mt_"),
	ORIGINCDR_QUALITY("tbEventOriginCdrQuality","/TB_EVENT_ORIGIN_Cdr_Quality_"),
	ORIGIN_Uu("tbEventOriginUu","/TB_EVENT_ORIGIN_UU_"),
	ORIGIN_MRO("tbEventOriginMro","/TB_EVENT_ORIGIN_MRO_LTE_SCPLR_"),
	ORIGIN_MDT_RLFHOF("tbEventOriginMdtRlfHof","/TB_EVENT_ORIGIN_Mdt_RlfHof_"),
	ORIGIN_MDT_RLFHOF_OTHERPATH("tbEventOriginMdtRlfHofOtherPath","/TB_EVENT_ORIGIN_Mdt_RlfHof_"),
    ORIGIN_MDT_RCEF("tbEventOriginMdtRcef","/TB_EVENT_ORIGIN_Mdt_RlfHof_"),
	ORIGIN_MOS_SHARDING("tbEventOriginMosSharding","/TB_EVENT_ORIGIN_MOS_SHRDING"),
    ORIGIN_MDT_IMM1("tbEventOriginMdtImm1","/TB_EVENT_ORIGIN_MDT_IMM1"),
    ORIGIN_MDT_IMM4("tbEventOriginMdtImm4","/TB_EVENT_ORIGIN_MDT_IMM4"),
    ORIGIN_MDT_IMM5("tbEventOriginMdtImm5","/TB_EVENT_ORIGIN_MDT_IMM5"),
    ORIGIN_MDT_RLFHOFRCEF("tbEventOriginMdrRlfHofRcef","/TB_EVENT_ORIGIN_Mdt_RlfHofRcef_"),

     IMSAMPLE("tbEventMidInSample","/tb_evt_insample_abnormal_mid_dd_")
    ,OMSAMPLE("tbEventMidOutSample","/tb_evt_outsample_abnormal_mid_dd_")
    ,ILSAMPLE("tbEventLowInSample","/tb_evt_insample_abnormal_low_dd_")
    ,OLSAMPLE("tbEventLowOutSample","/tb_evt_outsample_abnormal_low_dd_")
    ,IHSAMPLE("tbEventHighInSample","/tb_evt_insample_abnormal_high_dd_")
    ,OHSAMPLE("tbEventHighOutSample","/tb_evt_outsample_abnormal_high_dd_")
    
    ,IMSAMPLEALL("tbEventMidInSampleALL","/tb_evt_insample_mid_dd_")
    ,OMSAMPLEALL("tbEventMidOutSampleALL","/tb_evt_outsample_mid_dd_")
    ,ILSAMPLEALL("tbEventLowInSampleALL","/tb_evt_insample_low_dd_")
    ,OLSAMPLEALL("tbEventLowOutSampleALL","/tb_evt_outsample_low_dd_")
    ,IHSAMPLEALL("tbEventHighInSampleALL","/tb_evt_insample_high_dd_")
    ,OHSAMPLEALL("tbEventHighOutSampleALL","/tb_evt_outsample_high_dd_")
    
    ,OHGRIDEVT("tbEventHighOutGrid","/tb_evt_outgrid_high_dd_")
    ,OMGRIDEVT("tbEventMidOutGrid","/tb_evt_outgrid_mid_dd_")
    ,OLGRIDEVT("tbEventLowOutGrid","/tb_evt_outgrid_low_dd_")
    ,OHCELLGRIDEVT("tbEventHighOutCellGrid","/tb_evt_outgrid_cell_high_dd_")
    ,OMCELLGRIDEVT("tbEventMidOutCellGrid","/tb_evt_outgrid_cell_mid_dd_")
    ,OLCELLGRIDEVT("tbEventLowOutCellGrid","/tb_evt_outgrid_cell_low_dd_")
    ,IHGRIDEVT("tbEventHighInGrid","/tb_evt_ingrid_high_dd_")
    ,IMGRIDEVT("tbEventMidInGrid","/tb_evt_ingrid_mid_dd_")
    ,ILGRIDEVT("tbEventLowInGrid","/tb_evt_ingrid_low_dd_")
    ,IHCELLGRIDEVT("tbEventHighInCellGrid","/tb_evt_ingrid_cell_high_dd_")
    ,IMCELLGRIDEVT("tbEventMidInCellGrid","/tb_evt_ingrid_cell_mid_dd_")
    ,ILCELLGRIDEVT("tbEventLowInCellGrid","/tb_evt_ingrid_cell_low_dd_")

    ,BHGRIDEVT("tbEventHighBuildGrid","/tb_evt_building_high_dd_")
    ,BMGRIDEVT("tbEventMidBuildGrid","/tb_evt_building_mid_dd_")
    ,BLGRIDEVT("tbEventLowBuildGrid","/tb_evt_building_low_dd_")
    
    ,BHPOSEVT("tbEventHighBuildPos","/tb_evt_building_pos_high_yd_dd_")
    ,BMPOSEVT("tbEventMidBuildPos","/tb_evt_building_pos_mid_yd_dd_")
    ,BLPOSEVT("tbEventLowBuildPos","/tb_evt_building_pos_low_yd_dd_")
    
    ,BHCELLGRIDEVT("tbEventHighBuildCellGrid","/tb_evt_building_cell_high_dd_")
    ,BMCELLGRIDEVT("tbEventMidBuildCellGrid","/tb_evt_building_cell_mid_dd_")
    ,BLCELLGRIDEVT("tbEventLowBuildCellGrid","/tb_evt_building_cell_low_dd_")
    
    ,BHCELLPOSEVT("tbEventHighBuildCellPos","/tb_evt_building_cell_pos_high_yd_dd_")
    ,BMCELLPOSEVT("tbEventMidBuildCellPos","/tb_evt_building_cell_pos_mid_yd_dd_")
    ,BLCELLPOSEVT("tbEventLowBuildCellPos","/tb_evt_building_cell_pos_low_yd_dd_")
    
    ,CELLEVT("tbEventCell","/tb_evt_cell_dd_")
    ,IMSIEVT("tbEventImsi","/tb_evt_imsi_dd_")

    ,AREAEVT("tbArea","/tb_evt_area_dd_")
    ,AREACELLEVT("tbAreaCell","/tb_evt_area_cell_dd_")
    ,AREACELLGRIDEVT("tbAreaGridCell","/tb_evt_area_outgrid_cell_dd_")
    ,AREAGRIDEVT("tbAreaGrid","/tb_evt_area_outgrid_dd_")
	// ,HPSAMPLE("hpEvtSample", "/tb_hp_evt_samplingpoint_yd_dd_")//++
    ,HSRSAMPLE("hsrEvtSample", "/tb_hsr_evt_samplingpoint_yd_dd_")
    ,HSRGRIDEVT("hsrEvtGrid", "/tb_hsr_evt_grid_yd_dd_")
    ,HSRCELLGRIDEVT("hsrEvtCellGrid", "/tb_hsr_evt_cellgrid_yd_dd_")
    ,HSRCELLEVT("hsrEvtCell", "/tb_hsr_evt_cell_yd_dd_")
    ,HSRTRAINSEGEVT("hsrEvtTrainseg", "/tb_hsr_evt_trainsegment_yd_dd_")
    ,HSRTRAINSEGCELLEVT("hsrEvtTrainsegCell", "/tb_hsr_evt_trainsegment_cell_yd_dd_")

    ,XDR_CELL("xdrCell", "/tb_xdr_cell_dd_")
    ;
 
    private String aliasName;
    private String outPath;
    public int index;

    private static Map<Integer, TypeIoEvtEnum> indexToEnum;

    TypeIoEvtEnum(String aliasName, String outPath) 
    {
        this.aliasName = aliasName;
        this.outPath = outPath;
        this.index= Math.abs(TypeIoEvtEnum.class.getName().hashCode()+ordinal());
    }


    @Override
    public String toString() {
        return "TypeSampleEvtIO{" +
                "aliasName='" + aliasName + '\'' +
                ", outPath='" + outPath + '\'' +
                '}';
    }

    
    
    @Override
    public String getPath(String oriPath, String startTime) 
    {
        return oriPath + this.outPath + startTime.substring(3);
    }
    

    public String getOutPath(){
        return this.outPath;
    }

	@Override
	public String getFileName() {
		// TODO Auto-generated method stub
		return aliasName;
	}


	@Override
	public String getDirName() {
		// TODO Auto-generated method stub
		return outPath;
	}


	@Override
	public int getIndex() {
		return index;
	}


	@Override
	public String getHourPath(String hPath, String outData, String hour) {
		if (hour == null)
			return getPath(hPath, outData);
		return hPath + this.outPath + outData.substring(3);
	}

	public static TypeIoEvtEnum getEvtEnumByIndex(Integer index){
        if (indexToEnum == null){
            indexToEnum = new HashMap<>();

            for (TypeIoEvtEnum evtEnum : values()){
                indexToEnum.put(evtEnum.getIndex(), evtEnum);
            }
        }
        return indexToEnum.get(index);
    }
}

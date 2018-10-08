package cn.mastercom.bigdata.mro.stat.struct;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

import java.io.Serializable;

public class ResidentUserCellMergeDo implements IMergeDataDo, Serializable {

    private int dataType;
    private StatResidentCellUser statResidentCellUser = new StatResidentCellUser();

    @Override
    public String getMapKey() {
        return this.statResidentCellUser.mapKey();
    }

    @Override
    public int getDataType() {
        return this.dataType;
    }

    @Override
    public int setDataType(int dataType) {
        this.dataType = dataType;
        return 0;
    }

    @Override
    public boolean mergeData(Object o) {
        if (o instanceof ResidentUserCellMergeDo) {
            ResidentUserCellMergeDo another = (ResidentUserCellMergeDo) o;
            if (null != another.statResidentCellUser) {
                this.statResidentCellUser.merge(another.statResidentCellUser);
            }
        }
        return true;
    }

    @Override
    public boolean fillData(String[] values, int sPos) {
        this.statResidentCellUser.fill(values);
        return true;
    }

    @Override
    public String getData() {
        return this.statResidentCellUser.toLine();
    }
}

package cn.mastercom.bigdata.stat.dimension;

import cn.mastercom.bigdata.base.constant.DataConstant;
import cn.mastercom.bigdata.base.model.impl.AbstractStat;

/**
 * Created by Kwong on 2018/7/16.
 */
public class TimeDimension extends AbstractStat.AbstractDimension{

    public int hourTime;

    public TimeDimension(){}

    public TimeDimension(int time){
        /*hourTime = FormatTime.RoundTimeForHour(time);*/
        //传进来的时间必须是格式化好的
        //如：需要按小时维度统计的 入参应是FormatTime.RoundTimeForHour(time)； 需要按天维度统计的 入参应是FormatTime.getRoundDayTime(time)
        hourTime = time;
    }

    @Override
    public void fromFormatedString(String value) throws Exception {
        if (value.contains(DataConstant.DEFAULT_COLUMN_SEPARATOR)){
            value = value.split(DataConstant.DEFAULT_COLUMN_SEPARATOR)[0];
        }
        hourTime = Integer.parseInt(value);
    }

    @Override
    public String toFormatedString() {
        return String.valueOf(hourTime);
    }
}

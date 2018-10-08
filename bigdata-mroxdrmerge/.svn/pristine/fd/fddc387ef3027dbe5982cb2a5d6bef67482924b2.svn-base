package cn.mastercom.bigdata.util;

import cn.mastercom.bigdata.StructData.SIGNAL_LOC;
import cn.mastercom.bigdata.StructData.SIGNAL_XDR_4G;
import cn.mastercom.bigdata.xdr.loc.CellSwitInfo;

import java.util.*;

/**
 * Created by Kwong on 2018/9/28.
 */
public final class BA5MinCellHelper {

    public static final String CELL_SEPARATOR = ";";

    private BA5MinCellHelper(){
        throw new UnsupportedOperationException();
    }

    /**
     * 确定同一个用户前后5分钟的小区切换
     *
     * @param xdrList
     *            该用户全天的xdr数据
     * @param cellSwitList
     *            该用户小区切换列表
     *
     */
    @Deprecated //由于比较慢 和 内存溢出，已弃用
    public static void get5MinEciList(ArrayList<CellSwitInfo> cellSwitList, List<SIGNAL_LOC> xdrList)
    {
        // 确定 xdr前后五分钟的小区切换信息
        int xdrpos = 0;
        for (CellSwitInfo tempSwitCell : cellSwitList)// eciSwitch
        {
            for (int i = xdrpos; i < xdrList.size(); i++)
            {
                SIGNAL_XDR_4G xdrItem = (SIGNAL_XDR_4G) xdrList.get(i);
                int xdrStime = xdrItem.stime - 300;
                int xdrEtime = xdrItem.stime + 300;
                if (tempSwitCell.etime < xdrStime)
                {
                    break;
                }
                if (tempSwitCell.stime > xdrEtime)
                {
                    xdrpos++;
                }
                if (xdrStime <= tempSwitCell.etime && xdrEtime >= tempSwitCell.stime)// 时间有交集
                {
                    if (xdrItem.eciSwitchList.length() < 1)
                    {
                        xdrItem.eciSwitchList.append(tempSwitCell.toString());
                    }
                    else
                    {
                        xdrItem.eciSwitchList.append(CELL_SEPARATOR + tempSwitCell.toString());
                    }
                }
            }
        }
    }

    @Deprecated
    public static void get5MinEciListNew(ArrayList<CellSwitInfo> cellSwitList, List<SIGNAL_LOC> xdrList)
    {
        if (cellSwitList == null || cellSwitList.isEmpty()) return;

        TreeMap<Double, CellSwitInfo> startTimeToEci = new TreeMap<>();
        TreeMap<Double, CellSwitInfo> endTimeToEci = new TreeMap<>();
        for (CellSwitInfo cellInfo : cellSwitList){
            try {
                if (cellInfo.stime > 0 && cellInfo.etime > 0){
                    computeIfContains(cellInfo.stime, cellInfo, startTimeToEci);
                    computeIfContains(cellInfo.etime, cellInfo, endTimeToEci);
                }
            }catch (Exception e){
                LOGHelper.GetLogger().writeLog(IWriteLogCallBack.LogType.error,"computeIfContains error", "computeIfContains error: " + cellInfo.stime +
                        "|"+cellInfo.eci, e);
                continue;
            }
        }
        Double lastXdrTime = Double.valueOf(xdrList.get(xdrList.size() - 1).stime);
        for (SIGNAL_LOC xdr : xdrList){
            //结束时间 大于 数据时间 - 300
            SortedMap<Double, CellSwitInfo> sortedMap1 = endTimeToEci.subMap(Double.valueOf(xdr.stime - 300), lastXdrTime + 0.01D);
            //开始时间 小于 数据时间 + 300
            SortedMap<Double, CellSwitInfo> sortedMap2 = startTimeToEci.subMap(0D, Double.valueOf(xdr.stime + 300));

            if (sortedMap1 != null && sortedMap2 != null && !sortedMap1.isEmpty() && !sortedMap2.isEmpty()){

                Set<CellSwitInfo> eciSet =  new HashSet<>(sortedMap1.values());
                eciSet.retainAll(new HashSet<>(sortedMap2.values()));

                if (!eciSet.isEmpty()){
                    ((SIGNAL_XDR_4G)xdr).eciSwitchList.append(StringUtil.join(eciSet, CELL_SEPARATOR));
                }
            }
        }
    }

    public static void getBA5MinEciList(ArrayList<CellSwitInfo> cellSwitList, List<SIGNAL_LOC> xdrList)
    {
        if (cellSwitList == null || cellSwitList.isEmpty()) return;

        //按300秒 切分多个多个时间段，为 24 * 12 = 288 个时间段，计算每个时间段包含哪些 切换小区
        Map<Integer, List<CellSwitInfo>> fixedTimeToCellList = new HashMap<>();

        for (CellSwitInfo cellInfo : cellSwitList){
            //开始时间 与 结束时间 之间
            int stimeFixed = cellInfo.stime / 300 * 300;
            int etimeFixed = cellInfo.etime / 300 * 300;
            for (int i = stimeFixed; i <= etimeFixed; i += 300){
                List<CellSwitInfo> cellSwitchInfoList = fixedTimeToCellList.get(i);
                if (cellSwitchInfoList == null){
                    cellSwitchInfoList = new ArrayList<>();
                    fixedTimeToCellList.put(i, cellSwitchInfoList);
                }
                cellSwitchInfoList.add(cellInfo);
            }
        }

        for (SIGNAL_LOC xdr : xdrList){
            Set<Long> eciSet = new HashSet<>();
            int current = xdr.stime / 300 * 300; //当前所在的分区肯定都是 前后5分钟以内的
            int last = current - 300; //上一个时间段
            int next = current + 300; //下一个时间段

            addLastSegEciList(xdr, fixedTimeToCellList.get(last), eciSet);
            addCurrSegEciList(xdr, fixedTimeToCellList.get(current), eciSet);
            addNextSegEciList(xdr, fixedTimeToCellList.get(next), eciSet);

            if (!eciSet.isEmpty()){
                ((SIGNAL_XDR_4G)xdr).eciSwitchList.append(StringUtil.join(eciSet, CELL_SEPARATOR));
            }
        }
    }

    private static void addLastSegEciList(SIGNAL_LOC xdr, List<CellSwitInfo> cellList, Set<Long> eciSet){
        if (cellList == null || cellList.isEmpty()) return;

        for (CellSwitInfo cell : cellList){
            if (cell.etime > xdr.stime - 300){
                eciSet.add(cell.eci);
            }
        }
    }

    private static void addCurrSegEciList(SIGNAL_LOC xdr, List<CellSwitInfo> cellList, Set<Long> eciSet){
        if (cellList == null || cellList.isEmpty()) return;

        for (CellSwitInfo cell : cellList){
            eciSet.add(cell.eci);
        }
    }

    private static void addNextSegEciList(SIGNAL_LOC xdr, List<CellSwitInfo> cellList, Set<Long> eciSet){
        if (cellList == null || cellList.isEmpty()) return;


        for (CellSwitInfo cell : cellList){
            if (cell.stime < xdr.stime + 300){
                eciSet.add(cell.eci);
            }
        }
    }

    private static void computeIfContains(double time, CellSwitInfo cellSwitInfo, TreeMap<Double, CellSwitInfo> timeToEci){
        if (!timeToEci.containsKey(time)){
            timeToEci.put(time, cellSwitInfo);
        }else {
            computeIfContains(time + 0.01D, cellSwitInfo, timeToEci);
        }
    }
}

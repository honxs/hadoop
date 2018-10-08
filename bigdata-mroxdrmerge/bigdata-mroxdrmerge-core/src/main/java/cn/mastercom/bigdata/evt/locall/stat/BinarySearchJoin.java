package cn.mastercom.bigdata.evt.locall.stat;

import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.evt.locall.model.XdrDataBase;
import cn.mastercom.bigdata.util.Func;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class BinarySearchJoin {
    public static HashMap<Integer, LocItem> locItemMap = new HashMap<>();

    public static ArrayList<LocItem> locItemLists = new ArrayList<>();

    public static int pushData(String value) {
        // 为了去重某一秒钟有室内室外的情况
        LocItem item = null;
        String[] vals = value.toString().split("\t", -1);
        item = LocItem.fillLocLib(vals);
        if (locItemMap.containsKey(item.itime)) {
            LocItem itemOld = locItemMap.get(item.itime);
            if (item.isOut() && !itemOld.isOut()) {
                itemOld.doorType = item.doorType;
                itemOld.confidentType = item.confidentType;
                itemOld.ibuildid = item.ibuildid;
                itemOld.iheight = item.iheight;
            } else if (item.isIn() && !itemOld.isOut()) {
                itemOld.doorType = item.doorType;
                itemOld.confidentType = item.confidentType;
                itemOld.ibuildid = item.ibuildid;
                itemOld.iheight = item.iheight;
            }
            if (item.isRSRP() && !itemOld.isRSRP()) {
                itemOld.lteScRSRP = item.lteScRSRP;
            }
            if (item.ilongitude > 0 && itemOld.ilongitude <= 0) {
                itemOld.ilongitude = item.ilongitude;
                itemOld.ilatitude = item.ilatitude;
            }
        } else {
            locItemMap.put(item.itime, item);
        }
        return 0;
    }

    public static void clear(){
        locItemMap.clear();
        locItemLists.clear();
    }

    /**
     * 位置库按照时间从小到大进行排序
     */
    public static void sortLocItemLists() {

        for (LocItem locItem : locItemMap.values()) {
            locItemLists.add(locItem);
        }
        // 排序
        Collections.sort(locItemLists, new Comparator<LocItem>() {
            @Override
            public int compare(LocItem o1, LocItem o2) {
                if (o1.itime > o2.itime) {
                    return 1;
                } else if (o1.itime < o2.itime) {
                    return -1;
                }
                return 0;
            }
        });
    }

    /**
     * 给xdr数据回填经纬度
     * @param itemList 需要定位的itemList
     */
    public static void fillLoc(ArrayList<XdrDataBase> itemList) {

        for (XdrDataBase item : itemList) {
            // 二分查找到对应的下标
            int biaryIndex = binarySearchLocItem(item.istime, locItemLists, 0, locItemLists.size());
            /*  通过二分得到的下标找到左边和右边的locItem */
            LocItem locItemLeft = null;
            LocItem locItemRight = null;
            if(biaryIndex!=-1){
                if(item.istime<locItemLists.get(biaryIndex).itime){
                    locItemRight = locItemLists.get(biaryIndex);
                    if((biaryIndex-1)>=0){
                        locItemLeft = locItemLists.get(biaryIndex-1);
                    }
                }
                else if(item.istime>locItemLists.get(biaryIndex).itime){
                    locItemLeft = locItemLists.get(biaryIndex);
                    if((biaryIndex+1)<locItemLists.size()){
                        locItemRight = locItemLists.get(biaryIndex+1);
                    }
                } else if(item.istime ==locItemLists.get(biaryIndex).itime){
                    locItemLeft = locItemRight = locItemLists.get(biaryIndex);
                }
            }
            //得到最合适那个LocItem
            LocItem nearEastLocItem = getNearEastLocItem(locItemLeft,locItemRight,item);
            //回填位置信息
            if (nearEastLocItem != null)
            {
                if (nearEastLocItem.isOut())
                {
                    item.iCityID = nearEastLocItem.cityID;
                    item.iLongitude = nearEastLocItem.ilongitude;
                    item.iLatitude = nearEastLocItem.ilatitude;
                    item.iDoorType = StaticConfig.ACTTYPE_OUT;
                    item.iAreaType = nearEastLocItem.iAreaType;
                    item.iAreaID = nearEastLocItem.iAreaID;
                    item.strloctp = nearEastLocItem.loctp;
                    item.iRadius = nearEastLocItem.radius;
                    item.ibuildid = nearEastLocItem.ibuildid;
                    item.iheight = nearEastLocItem.iheight;
                    item.testType = nearEastLocItem.testType;
                    item.label = nearEastLocItem.label;
                    item.locSource = Func.getLocSource(nearEastLocItem.loctp);
                    item.LteScRSRP = nearEastLocItem.lteScRSRP;
                    item.LteScSinrUL = nearEastLocItem.lteScSinrUL;
                    item.confidentType = nearEastLocItem.confidentType;
                    if (item.ecgi <= 0)
                    {
                        item.ecgi = nearEastLocItem.eci;
                    }

                    item.noEntryImsi = nearEastLocItem.imsi;
                    item.imsi = Func.getEncrypt(nearEastLocItem.imsi);

                    if ("".equals(item.msisdn_neimeng))
                    {
                        item.msisdn_neimeng = nearEastLocItem.msisdn;
                    }
                    item.position = nearEastLocItem.position;

                }

                else if (nearEastLocItem.isIn())
                {
                    item.iCityID = nearEastLocItem.cityID;
                    item.iLongitude = nearEastLocItem.ilongitude;
                    item.iLatitude = nearEastLocItem.ilatitude;
                    item.iDoorType = StaticConfig.ACTTYPE_IN;
                    item.iAreaType = nearEastLocItem.iAreaType;
                    item.iAreaID = nearEastLocItem.iAreaID;
                    item.strloctp = nearEastLocItem.loctp;
                    item.iRadius = nearEastLocItem.radius;
                    item.ibuildid = nearEastLocItem.ibuildid;
                    item.iheight = nearEastLocItem.iheight;
                    item.testType = nearEastLocItem.testType;
                    item.label = nearEastLocItem.label;

                    item.locSource = Func.getLocSource(nearEastLocItem.loctp);
                    item.LteScRSRP = nearEastLocItem.lteScRSRP;
                    item.LteScSinrUL = nearEastLocItem.lteScSinrUL;

                    item.confidentType = nearEastLocItem.confidentType;
                    if (item.ecgi <= 0)
                    {
                        item.ecgi = nearEastLocItem.eci;
                    }

                    item.noEntryImsi = nearEastLocItem.imsi;
                    item.imsi = Func.getEncrypt(nearEastLocItem.imsi);

                    if ("".equals(item.msisdn_neimeng))
                    {
                        item.msisdn_neimeng = nearEastLocItem.msisdn;
                    }
                    item.position = nearEastLocItem.position;
                }

            }

        }
    }

    /**
     * 找到最合适的那个Loc数据，进行回填
     * @param locItemLeft  当前xdr左边的位置
     * @param locItemRight 当前xdr右边的位置
     * @param curXdr  当前的xdr
     * @return 通过一定的要求，返回左边或者右边，也可能不满足条件返回一个空
     */
    private static LocItem getNearEastLocItem(LocItem locItemLeft, LocItem locItemRight, XdrDataBase curXdr) {
        // 两个都为null
        if (locItemLeft == null && locItemRight == null)
        {
            return null;
        }

        // 一个为null，一个不为null
        // 左边不为null,右边为null
        if (locItemLeft != null && locItemRight == null)
        {
            // 若为室外，必须在20秒内
            if (locItemLeft.isOut() && curXdr.istime - locItemLeft.itime <= 20)
            {
                return locItemLeft;
            }

            // 若为室内：
            // 1. 若小区相同则10分钟内均可回填
            if (locItemLeft.isIn() && locItemLeft.eci == curXdr.ecgi &&
                    curXdr.istime - locItemLeft.itime <= 600)
            {
                return locItemLeft;
            }
            // 2. 小区不同则限制在20秒
            if (locItemLeft.isIn() && locItemLeft.eci != curXdr.ecgi && curXdr.istime - locItemLeft.itime <= 20)
            {
                return locItemLeft;
            }
        }
        // 右边不为null,左边为null
        else if (locItemLeft == null && locItemRight != null)
        {
            // 如果是室外，则要在20秒内
            if (locItemRight.isOut() && locItemRight.itime - curXdr.istime <= 20)
            {
                return locItemRight;
            }
            // 若为室内：
            // 1. 若小区相同则10分钟内均可回填
            if (locItemRight.isIn() && locItemRight.eci == curXdr.ecgi && locItemRight.itime - curXdr.istime <= 600)
            {
                return locItemRight;
            }
            // 2. 小区不同则限制在20秒
            if (locItemRight.isIn() && locItemRight.eci != curXdr.ecgi && locItemRight.itime - curXdr.istime <= 20)
            {
                return locItemRight;
            }
        }
        //若两边有不为null得且不满足上面的条件
        if(locItemLeft==null||locItemRight==null){
            return null;
        }
        // 两个都是室内 in
        if (locItemLeft.isIn() && locItemRight.isIn())
        {
            // 若两个小区都和本小区相同，取最近的，时间限制在20分钟
            if (locItemLeft.eci == curXdr.ecgi && locItemRight.eci == curXdr.ecgi)
            {
                // 若左边最近
                if (curXdr.istime - locItemLeft.itime <= 1200 && locItemRight.itime - curXdr.istime > curXdr.istime - locItemLeft.itime)
                {
                    return locItemLeft;
                }
                // 若右边最近
                if (locItemRight.itime - curXdr.istime <= 1200 && curXdr.istime - locItemLeft.itime > locItemRight.itime - curXdr.istime)
                {
                    return locItemRight;
                }
            }

            // 若只有一个小区和本小区相同，回填相同小区的位置，时间在10分钟内
            // 若只有左边小区与本小区相同
            if (locItemLeft.eci == curXdr.ecgi && curXdr.istime - locItemLeft.itime <= 600)
            {
                return locItemLeft;
            }
            // 若只有右边小区与本小区相同
            else if (locItemRight.eci == curXdr.ecgi && locItemRight.itime - curXdr.istime <= 600)
            {
                return locItemRight;
            }

            // 若都不相同，回填最近的，时间在1分钟内
            if (locItemLeft.eci != curXdr.ecgi && locItemRight.eci != curXdr.ecgi)
            {
                // 若左边最近
                if (curXdr.istime - locItemLeft.itime <= 60 && locItemRight.itime - curXdr.istime >= curXdr.istime - locItemLeft.itime)
                {
                    return locItemLeft;
                }
                // 若右边最近
                if (locItemRight.itime - curXdr.istime <= 60 && curXdr.istime - locItemLeft.itime >= locItemRight.itime - curXdr.istime)
                {
                    return locItemRight;
                }
            }
        }

        // 一个out一个in
        // 若左边out右边in
        if (locItemLeft.isOut() && locItemRight.isIn())
        {
            // out若在20秒内，回填out
            if (curXdr.istime - locItemLeft.itime <= 20)
            {
                return locItemLeft;
            }
            // 否则，回填in， 小区限制在同一个，时间限制10分钟
            else if (locItemRight.eci == curXdr.ecgi && locItemRight.itime - curXdr.istime <= 600)
            {
                return locItemRight;
            }
            // 如果eci不同，在20秒内，也可以回填in
            else if (locItemRight.eci != curXdr.ecgi && locItemRight.itime - curXdr.istime <= 20)
            {
                return locItemRight;
            }
        }
        // 若右边out左边in
        if (locItemRight.isOut() && locItemLeft.isIn())
        {
            // out若在20秒内，回填out
            if (locItemRight.itime - curXdr.istime <= 20)
            {
                return locItemRight;
            }
            // 否则，回填in， 小区限制在同一个，时间限制10分钟
            else if (locItemLeft.eci == curXdr.ecgi && curXdr.istime - locItemLeft.itime <= 600)
            {
                return locItemLeft;
            }
            // 如果eci不同，在20秒内，也可以回填in
            else if (locItemLeft.eci != curXdr.ecgi && curXdr.istime - locItemLeft.itime <= 20)
            {
                return locItemLeft;
            }
        }

        // 两个都是out
        if (locItemLeft.isOut() && locItemRight.isOut())
        {
            // 两个eci都和本eci相等，回填最近的，时间限制20秒
            if (locItemLeft.eci == curXdr.ecgi && locItemRight.eci == curXdr.ecgi)
            {
                // 若左边最近
                if (curXdr.istime - locItemLeft.itime <= 20 && locItemRight.itime - curXdr.istime >= curXdr.istime - locItemLeft.itime)
                {
                    return locItemLeft;
                }
                // 若右边最近
                if (locItemRight.itime - curXdr.istime <= 20 && curXdr.istime - locItemLeft.itime >= locItemRight.itime - curXdr.istime)
                {
                    return locItemRight;
                }
            }
            // 若只有一个eci相等，回填相等的，时间限制20秒。
            // 若只有左边eci相等
            else if (locItemLeft.eci == curXdr.ecgi && curXdr.istime - locItemLeft.itime <= 20)
            {
                return locItemLeft;
            }
            // 若只有右边eci相等
            else if (locItemRight.eci == curXdr.ecgi && locItemRight.itime - curXdr.istime <= 20)
            {
                return locItemRight;
            }
            // 若eci都不相等，回填最近的，时间限制20秒
            else
            {
                // 若左边最近
                if (curXdr.istime - locItemLeft.itime <= 20 && locItemRight.itime - curXdr.istime >= curXdr.istime - locItemLeft.itime)
                {
                    return locItemLeft;
                }
                // 若右边最近
                if (locItemRight.itime - curXdr.istime <= 20 && curXdr.istime - locItemLeft.itime >= locItemRight.itime - curXdr.istime)
                {
                    return locItemRight;
                }
            }
        }
        return null;
    }


    /**
     *
     * @param istime 需要查找的时间
     * @param locItemLists 查找的list集合
     * @param beginIndex 开始的下标
     * @param endIndex 结束的下标
     * @return 返回二分所得到的index数组
     */
    private static int  binarySearchLocItem(int istime,
                                            ArrayList<LocItem> locItemLists,
                                            int beginIndex,int endIndex) {
        /* 当istime小于第一个值时返回0，大于最后一个值时返回length-1 */
        if(endIndex-beginIndex>0){
            if(istime<=locItemLists.get(beginIndex).itime){
                return 0;
            }else if(istime>=locItemLists.get(endIndex-1).itime){
                return endIndex-1;
            }

        }

        int mid = -1;
        while (endIndex-beginIndex>1){
            mid = (beginIndex+endIndex)/2;
            if(istime>locItemLists.get(mid).itime){
                beginIndex = mid;
            }else if(istime<locItemLists.get(mid).itime) {
                endIndex = mid;
            } else{
                return mid;
            }
        }
        return mid;
    }


    public static void main(String[] args) {
        ArrayList<LocItem> locItems = new ArrayList<>();
        LocItem locItem1 = new LocItem();locItem1.itime = 1;
        LocItem locItem2 = new LocItem();locItem2.itime = 2;
        LocItem locItem3 = new LocItem();locItem3.itime = 4;
        LocItem locItem4 = new LocItem();locItem4.itime = 6;
        LocItem locItem5 = new LocItem();locItem5.itime = 9;
        LocItem locItem6 = new LocItem();locItem6.itime = 11;
        locItems.add(locItem1);locItems.add(locItem2);locItems.add(locItem3);
        locItems.add(locItem4);locItems.add(locItem5);locItems.add(locItem6);
        LocItem locItemLeft = null;
        LocItem locItemRight = null;
        int istime = 7;
        int index = binarySearchLocItem(istime, locItems, 0, locItems.size());

        if(index!=-1){
            if(istime<locItems.get(index).itime ){
                if((index-1)>=0){
                    locItemLeft = locItems.get(index-1);
                }
                locItemRight = locItems.get(index);
            }
            else if(istime>locItems.get(index).itime){
                locItemLeft = locItems.get(index);
                if((index+1)<locItems.size()){
                    locItemRight = locItems.get(index+1);
                }
            } else if(istime ==locItems.get(index).itime){
                locItemLeft = locItemRight = locItems.get(index);
            }
        }
        if(locItemLeft!=null){
            System.out.println("left: "+locItemLeft.itime);
        }
        if(locItemRight!=null){
            System.out.println("right: "+locItemRight.itime);
        }
    }

}

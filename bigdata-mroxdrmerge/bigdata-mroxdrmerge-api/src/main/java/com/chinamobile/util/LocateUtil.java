package com.chinamobile.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import com.chinamobile.geo.GeoHash;
import com.chinamobile.xdr.BaiduRawInfo;

/**
 * Copyright © Zhou Xingwei. All Rights Reserved
 * Email: zhouxingwei@139.com
 * Function:
 * Usage:
 */
//@NotProguard
public class LocateUtil
{




    public static List<BaiduRawInfo> parsePOST(String line)
    {
        if (line == null)
        {
            return null;
        }

        List<BaiduRawInfo> result = new ArrayList<BaiduRawInfo>();


        if (line.contains("req="))
        {
            //离线数据
            String[] parts = line.split("&");
            for (String part : parts)
            {
                Pattern p = Pattern.compile("\\[(.*?)\\]");
                Matcher m = p.matcher(part);
                while (m.find())
                {
                    try
                    {
                        JSONArray array = new JSONArray(m.group());
                        for (int i = 0; i < array.length(); i++)
                        {

                            String decryptedPart = parsePOSTPart("offline", (String) array.get(i));
                            BaiduRawInfo partResult = getBaiduRawInfo(decryptedPart);
                            System.out.println(decryptedPart);
                            //时间戳丢掉了？？
                            if (partResult != null)
                            {
                                result.add(partResult);
                            }
                        }
                    } catch (JSONException e)
                    {
                        e.printStackTrace();
                    }
                }

            }


        } else
        {
            //在线数据
            String[] parts = line.split("&");
            for (String part : parts)
            {
                if (!part.contains("="))
                {
                    continue;
                }

                if (part.startsWith("bloc="))
                {
                    String decryptedPart = parsePOSTPart("bloc", part.substring(5, part.length()));
                    BaiduRawInfo partResult = getBaiduRawInfo(decryptedPart);
                    //时间戳丢掉了？？
                    if (partResult != null)
                    {
                        result.add(partResult);
                    }
                } else if (part.startsWith("up="))
                {
                    String decryptedPart = parsePOSTPart("up", part.substring(3, part.length()));
                    BaiduRawInfo partResult = getBaiduRawInfo(decryptedPart);
                    //时间戳丢掉了？？
                    if (partResult != null)
                    {
                        result.add(partResult);
                    }
                }
            }

        }


        return result;
    }


    public static String parsePOSTPart(String key, String part)
    {
        return null;
    }

    //提取解密后的数据
    public static BaiduRawInfo getBaiduRawInfo(String line)
    {
        try
        {

            if (line == null || !line.contains("&"))
            {
                return null;
            }

            String[] parts = line.split("&");

            Class<?> clazz = null;
            clazz = Class.forName("com.chinamobile.xdr.BaiduRawInfo");
            Object result = clazz.newInstance();
            Field[] fields = clazz.getDeclaredFields();

            List<String> fieldNameList = new ArrayList<String>();
            for (Field field : fields)
            {
                fieldNameList.add(field.getName());
            }

            for (String part : parts)
            {
                if (part != null && part.contains("="))
                {
                    String[] keyvalue = part.split("=");
                    if (keyvalue.length == 2 && fieldNameList.contains(keyvalue[0]))
                    {
                        //clazz.getMethod(keyvalue[0]).setAccessible(true);
                        clazz.getDeclaredField(keyvalue[0]).set(result, keyvalue[1]);
                    } 
                    else
                    {
                        System.out.println("cannot find:" + keyvalue[0]);
                    }

                }

            }

            //System.out.println("SSS"+((com.chinamobile.locate.BaiduRawInfo) result).cl);
//            System.out.println(((com.chinamobile.locate.BaiduRawInfo) result).ll);
//            System.out.println(((com.chinamobile.locate.BaiduRawInfo) result).wf);
            //System.out.println(result.timeStamp);

//            if (((BaiduRawInfo) result).cl == null)
//            {
//                System.out.println("SDF" + line);
//            }


            return (BaiduRawInfo) result;

        } catch (Exception e)
        {
            return null;

        }

    }



/**************

    //最好带个标示过来，标上上一条request的host
    public static List<LocationInfo> parseHTTPBytes(TcpData tcpData)
    {
        if (tcpData == null)
        {
            return null;
        }

        long frameIndex = tcpData.frameIndex;
        String sourceIP = tcpData.sourceIP;
        String sourcePort = tcpData.sourcePort;
        String destinationIP = tcpData.destinationIP;
        String destinationPort = tcpData.destinationPort;
        String TEID = tcpData.TEID;
        byte[] httpBytes = tcpData.payload;

        List<LocationInfo> finalResult = new ArrayList<LocationInfo>();
        String requestType = null;
        String host = null;
        String url = null;

        List<Integer> returnIndexList = new ArrayList<Integer>();
        for (int i = 0; i < httpBytes.length - 1; i++)
        {
            if (httpBytes[i] == 0x0d && httpBytes[i + 1] == 0x0a)
            {
                returnIndexList.add(i);
            }
        }

        if (returnIndexList.size() >= 2)
        {
            String expertInfo = new String(Arrays.copyOfRange(httpBytes, 0, returnIndexList.get(0)));
            String httpHeader = new String(Arrays.copyOfRange(httpBytes, returnIndexList.get(0) + 2, returnIndexList.get(returnIndexList.size() - 1)));

            String[] request_path_httpversion = expertInfo.split(" ");
            if (request_path_httpversion.length == 3)
            {
                requestType = request_path_httpversion[0];
                url = request_path_httpversion[1];
            }

            if (requestType == null)
            {
                return null;
            }
            if (requestType.equals("POST"))
            {
                int hostStartIndex = httpHeader.indexOf("Host: ");
                int hostEndIndex = httpHeader.indexOf("\r\n", hostStartIndex);
                //System.out.println(httpHeader);
                if (hostStartIndex >= 0 && hostEndIndex >= 0)
                {
                    host = httpHeader.substring(hostStartIndex + 6, hostEndIndex);
                }

                if (url != null && host != null)
                {
                    if (host.contains("loc.map.baidu.com") && url.contains("sdk.php"))
                    {
                        List<BaiduRawInfo> baiduLocResult = null;

                        int totalLength = httpBytes.length;
                        String httpPayload = new String(Arrays.copyOfRange(httpBytes, returnIndexList.get(returnIndexList.size() - 1) + 2, totalLength));
                        baiduLocResult = LocateUtil.parsePOST(httpPayload);

                        if (baiduLocResult != null)
                        {
                            for (BaiduRawInfo tmpBaiduLoc : baiduLocResult)
                            {
                                if (tmpBaiduLoc.ll != null)
                                {
                                    LocationInfo tmpLoc = new LocationInfo();
                                    tmpLoc.frameIndex = frameIndex;
                                    tmpLoc.coorType = "wgs";
                                    tmpLoc.destinationIP = destinationIP;
                                    tmpLoc.destinationPort = destinationPort;
                                    tmpLoc.host = host;
                                    tmpLoc.locationType = "gps";
                                    tmpLoc.requestType = requestType;
                                    tmpLoc.sourceIP = sourceIP;
                                    tmpLoc.sourcePort = sourcePort;
                                    tmpLoc.TEID = TEID;
                                    tmpLoc.url = url;

                                    String[] lngLat = tmpBaiduLoc.ll.split("\\|");
                                    if (lngLat != null && lngLat.length == 2
                                            && StringUtil.isNum(lngLat[0]) && StringUtil.isNum(lngLat[1])
                                            && GisUtil.isValid(Double.parseDouble(lngLat[1]),Double.parseDouble(lngLat[0])))
                                    {
                                        tmpLoc.longitude = Double.parseDouble(lngLat[0]);
                                        tmpLoc.latitude = Double.parseDouble(lngLat[1]);
                                    }

                                    if (tmpBaiduLoc.ll_t != null)
                                    {
                                        tmpLoc.timeStamp = TimeUtil.getTimeStringFromMillis(Long.parseLong(tmpBaiduLoc.ll_t) * 1000L, "yyyy-MM-dd HH:mm:ss");
                                    }

                                    finalResult.add(tmpLoc);

                                    //System.out.println(tmpLoc);

                                }
                            }

                        }

                    } else if (host.contains("itsdata.map.baidu.com") && url.contains("/long-conn-gps/sdk.php"))
                    {
                        byte[] longGPSBytes = Arrays.copyOfRange(httpBytes, returnIndexList.get(returnIndexList.size() - 3) + 2, returnIndexList.get(returnIndexList.size() - 2));
                        LocationInfo baiduNaviInfo = DecryptUtil.Navi(longGPSBytes);

                        if (baiduNaviInfo != null)
                        {
                            baiduNaviInfo.frameIndex = frameIndex;
                            baiduNaviInfo.coorType = "wgs";
                            baiduNaviInfo.destinationIP = destinationIP;
                            baiduNaviInfo.destinationPort = destinationPort;
                            baiduNaviInfo.host = host;
                            baiduNaviInfo.locationType = "gps";
                            baiduNaviInfo.requestType = requestType;
                            baiduNaviInfo.sourceIP = sourceIP;
                            baiduNaviInfo.sourcePort = sourcePort;
                            baiduNaviInfo.TEID = TEID;
                            baiduNaviInfo.url = url;

                            finalResult.add(baiduNaviInfo);
                        }


                       //  System.out.println(TimeUtil.getTimeStringFromMillis(Long.parseLong(naviRawInfo.ll_t), "yyyy-MM-dd HH:mm:ss")+"@@"
                       //  +GisUtil.GPS2Baidu(new LatLng(Double.parseDouble(items[1]),Double.parseDouble(items[0]))));

                        //System.out.println(baiduNaviInfo);
                    }
                }

            } else if (requestType.equals("GET"))
            {

            } else if (requestType.startsWith("HTTP") && url.equals("200"))
            {
                boolean isBloc200OK = false;
                if (returnIndexList != null || returnIndexList.size() > 2)
                {
                    for (int i = 0; i < returnIndexList.size() - 2; i++)
                    {
                        byte[] tmpLineBytes = Arrays.copyOfRange(httpBytes, returnIndexList.get(i), returnIndexList.get(i + 1));
                        if (tmpLineBytes != null && tmpLineBytes.length > 0)
                        {
                            String tmpeLine = new String(tmpLineBytes);
                            if (tmpeLine.contains("Http_x_bd_logid"))
                            {
                                isBloc200OK = true;
                                break;
                            }
                        }
                    }
                }

                if (isBloc200OK)
                {
                    byte[] jsonBodyBytes = Arrays.copyOfRange(httpBytes, returnIndexList.get(returnIndexList.size() - 1) + 2, httpBytes.length);

                    if (jsonBodyBytes != null && jsonBodyBytes.length > 2)
                    {
                        if (jsonBodyBytes[0] == (byte) 0x1f && jsonBodyBytes[1] == (byte) 0x8b)
                        {
                            jsonBodyBytes = StringUtil.gunzip(jsonBodyBytes);

                        }

                        if (jsonBodyBytes != null)
                        {
                            try
                            {
                                JSONObject obj = new JSONObject(new String(jsonBodyBytes));
                                JSONObject objContent = (JSONObject) obj.get("content");
                                String loctp = objContent.getString("loctp");
                                JSONObject objPoint = (JSONObject) objContent.get("point");
                                String x = objPoint.getString("x");
                                String y = objPoint.getString("y");
                                Double radius = objContent.getDouble("radius");

                                JSONObject objResult = (JSONObject) obj.get("result");
                                String timeStamp = objResult.getString("time");

                                //System.out.println(loctp + "|" + x + "|" + y);

                                LocationInfo responseBaiduInfo = new LocationInfo();
                                responseBaiduInfo.frameIndex = frameIndex;
                                responseBaiduInfo.coorType = "wgs";
                                responseBaiduInfo.destinationIP = destinationIP;
                                responseBaiduInfo.destinationPort = destinationPort;
                                responseBaiduInfo.host = host;
                                responseBaiduInfo.locationType = loctp;
                                responseBaiduInfo.requestType = requestType;
                                responseBaiduInfo.sourceIP = sourceIP;
                                responseBaiduInfo.sourcePort = sourcePort;
                                responseBaiduInfo.TEID = TEID;
                                responseBaiduInfo.url = url;
                                if(StringUtil.isNum(x) && StringUtil.isNum(y))
                                {
                                    LatLng gpsLoc = GisUtil.Baidu2GPS(new LatLng(Double.parseDouble(y),Double.parseDouble(x)));
                                    if(GisUtil.isValid(gpsLoc))
                                    {
                                        responseBaiduInfo.longitude = gpsLoc.longitude;
                                        responseBaiduInfo.latitude = gpsLoc.latitude;
                                    }

                                }

                                responseBaiduInfo.radius = radius;

                                responseBaiduInfo.timeStamp = timeStamp;

                                finalResult.add(responseBaiduInfo);

                                //System.out.println(responseBaiduInfo);

                            } catch (Exception e)
                            {
                                //e.printStackTrace();
                            }


                            ///System.out.println(sourceIP + "%" + sourcePort + "%" + destinationIP + "%" + destinationPort + "%" + new String(unzippedBytes));

                        }

                    }

                }


            }


        }

        return finalResult;
    }
************/

    //由定位结果建立指纹库
    public static void fingerprint(List<BaiduRawInfo> baiduRawInfoList)
    {
        //获取wifi列表
        List<String> wifiList = new ArrayList<String>();
        //获取GPS经纬度
        double lat = 0.0D;
        double lng = 0.0D;
        //获取GSM小区ID
        String cellID = null;
        int i = 0;
        for (BaiduRawInfo baiduRawInfo : baiduRawInfoList)
        {
            if (baiduRawInfo.wf != null)
            {
                String[] macSignalList = divideString(baiduRawInfo.wf, "\\|");
                if (macSignalList != null)
                {
                    for (String macSignal : macSignalList)
                    {
                        //format : 24df6a613096;42;
                        wifiList.add(macSignal);
                    }
                }
            }
            //获取GPS坐标
            if (baiduRawInfo.ll != null)
            {
                String[] lnglat = divideString(baiduRawInfo.ll, "\\|");
                if (lnglat != null && lnglat.length == 2
                        && StringUtil.isNum(lnglat[0]) && StringUtil.isNum(lnglat[1]))
                {
                    if (GisUtil.isValid(Double.parseDouble(lnglat[1]), Double.parseDouble(lnglat[0])))
                    {
                        ++i;
                        lng += Double.parseDouble(lnglat[0]);
                        lat += Double.parseDouble(lnglat[1]);
                    }
                }
            }

            //获取GSM小区CellID
            if (baiduRawInfo.nw != null && baiduRawInfo.cl != null)
            {
                cellID = baiduRawInfo.nw + "|" + baiduRawInfo.cl;
            }
        }
        if (i > 0)
        {
            lat = lat / i;
            lng = lng / i;
        }

        System.out.println("average lat:" + lat + "   average lng:" + lng);

        if (wifiList.size() > 0)
        {


            for (String aa : wifiList)
            {
                System.out.print(aa + "$");
            }
            System.out.println("\n");

            HashMap<String, Integer> wifiHash = averageWifi(wifiList);

            if (wifiList.size() > 1)
            {
                Iterator iterator = wifiHash.entrySet().iterator();
                while (iterator.hasNext())
                {
                    Map.Entry ent = (Map.Entry) iterator.next();
                    String mac = (String) ent.getKey();
                    Integer signal = (Integer) ent.getValue();
                    System.out.print(mac + ":" + signal + "*");
                }
                System.out.println("\n");
            }


            //汇总wifi数据
            //sadd geo wifi
            //sadd cell wifi
            //hmset geo:wifi sum
            //hmset geo:wifi num
            //hmset cell:wifi sum
            //hmset cell:wifi num
            //定期刷新zrevrange库（平均电平排序且确保数量达到一定值）


            if (GisUtil.isValid(lat, lng))
            {
                //以geo为key
                GeoHash geo = GeoHash.withBitPrecision(lat, lng, 45);

            }

            if (cellID != null)
            {
                //以cellID为key

            }
        }


    }


    public static String[] divideString(String partToDivide, String divider)
    {
        //ll=116.35456|39.90952

        if (partToDivide == null)
        {
            return null;
        }

        return partToDivide.split(divider);

    }


    /**
     * format : 24df6a613096;42; List,有重复
     * 计算平均电平
     *
     * @param wifiList
     */
    public static HashMap<String, Integer> averageWifi(List<String> wifiList)
    {
        if (wifiList == null)
        {
            return null;
        }

        HashMap<String, Integer> result = new HashMap<String, Integer>();

        HashMap<String, List<Integer>> tmpResult = new HashMap<String, List<Integer>>();

        for (String wifi : wifiList)
        {
            String[] macSig = wifi.split(";");
            if (macSig != null && macSig.length == 2 && StringUtil.isNum(macSig[1]))
            {
                String mac = macSig[0];
                int sig = Integer.parseInt(macSig[1]);

                if (tmpResult.containsKey(mac))
                {
                    tmpResult.get(mac).add(sig);
                } else
                {
                    List<Integer> tmpList = new ArrayList<Integer>();
                    tmpList.add(sig);
                    tmpResult.put(mac, tmpList);
                }
            }
        }

        Iterator iterator = tmpResult.entrySet().iterator();
        while (iterator.hasNext())
        {
            Map.Entry ent = (Map.Entry) iterator.next();
            String mac = (String) ent.getKey();
            List<Integer> sigList = (List<Integer>) ent.getValue();
            int averageSig = averageList(sigList);

            result.put(mac, averageSig);
        }

        return result;
    }


    public static Integer averageList(List<Integer> sigList)
    {
        if (sigList == null || sigList.size() == 0)
        {
            return null;
        }

        int sum = 0;
        for (Integer tmp : sigList)
        {
            sum = sum + tmp;
        }

        return sum / sigList.size();
    }


}

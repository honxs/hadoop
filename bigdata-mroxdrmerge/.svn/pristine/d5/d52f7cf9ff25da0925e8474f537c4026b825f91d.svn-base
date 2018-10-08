package com.chinamobile.xdr;

import com.chinamobile.model.XDRLocationMapping;
import com.chinamobile.util.*;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@NotProguard
public class LocationParseService
{
    private LocationLoggerCallBack locationLoggerCallBack = null;

    private TcpData tcpData = null;

    private static HashMap<String,List<XDRLocationMapping>> urlConfig = null;

    public static void setUrlConfig(String configFileName)
    {
        urlConfig = GetUrlMappingConfig(configFileName);
    }
    
    public static void setUrlConfig(InputStream  is)
    {
        urlConfig = GetUrlMappingConfig(is);
    }

    public void registerLocationLogger(LocationLoggerCallBack callBack)
    {
        this.locationLoggerCallBack = callBack;
    }

    public void setTcpData(TcpData tcpData)
    {
        this.tcpData = tcpData;
    }

    public List<LocationInfo> parsetcpData()
    {
        return parsetcpData(this.tcpData);
    }

    public List<LocationInfo> parsetcpData(TcpData tcpData)
    {
    	return null;
    }
    
	public void parseUriLocation(TcpData tcpData2, List<LocationInfo> finalResult,
			String requestType, String host, String url)
	{
		LocationInfo responseLocationInfo = new LocationInfo();
		if(tcpData2 != null)
		{
			responseLocationInfo.frameIndex = tcpData2.frameIndex;
			responseLocationInfo.frameTimeStamp = tcpData2.frameTimeStamp;
			responseLocationInfo.outerSourceIP = tcpData2.outerSourceIP;
			responseLocationInfo.outerSourcePort = tcpData2.outerSourcePort;
			responseLocationInfo.outerTargetIP = tcpData2.outerTargetIP;
			responseLocationInfo.outerTargetPort = tcpData2.outerTargetPort;
			responseLocationInfo.innerSourceIP = tcpData2.innerSourceIP;
			responseLocationInfo.innerSourcePort = tcpData2.innerSourcePort;
			responseLocationInfo.innerTargetIP = tcpData2.innerTargetIP;
			responseLocationInfo.innerTargetPort = tcpData2.innerTargetPort;
			responseLocationInfo.targetTEID = tcpData2.targetTEID;
		}
		responseLocationInfo.host = host;
		responseLocationInfo.requestType = requestType;
		responseLocationInfo.url = url;
		
		RefreshLocationFromGetRequest(responseLocationInfo,host,url);

		if (GisUtil.isValid(responseLocationInfo.latitude,responseLocationInfo.longitude))
		{
		    finalResult.add(responseLocationInfo);
		    //System.out.println(responseLocationInfo);
		}
	}

    private BaiduRawInfo getBaiduRawInfo(String line)
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
                    /*else if (!fieldNameList.contains(keyvalue[0]))
                    {
                        System.out.println("cannot find:" + keyvalue[0]);
                    }*/
                }
            }
            
            BaiduRawInfo rawInfo = (BaiduRawInfo) result;
            if(rawInfo.cl != null)
            {
            	String[] vct = rawInfo.cl.split("\\|",-1);
            	if(vct.length>0)
            		rawInfo.cl = vct[vct.length-1];         	
            }
            
            return rawInfo;
        } 
        catch (Exception e)
        {
            return null;
        }

    }


    public static List<String> GetBaiduWifiInfo(String wf)
    {
        //001f64ede248;55;|001f64eae248;55;|001f64eaee04;55;
        if (wf == null || !wf.contains(";"))
        {
            return null;
        }
        List<String> result = new ArrayList<String>();
        String[] wifis = wf.split("\\|");
        for (String wifi : wifis)
        {
            result.add(wifi);
        }
        return result;
    }

    public static String GetTencentCellInfo(JSONObject jsonObject)
    {
        String cgi = null;
        if(jsonObject==null)
        {
            return null;
        }
        //{"mcc":460,"mnc":0,"lac":14791,"cellid":68380545,"rss":-1,"seed":1}
        cgi="";
        try
        {
            /*if(jsonObject.has("mcc"))
            {
                cgi += jsonObject.getInt("mcc");
            }
            cgi += "|";
            if(jsonObject.has("mnc"))
            {
                cgi += jsonObject.getInt("mnc");
            }
            cgi += "|";
            if(jsonObject.has("lac"))
            {
                cgi += jsonObject.getInt("lac");
            }
            cgi += "|";*/
            if(jsonObject.has("cellid"))
            {
                cgi = "" + jsonObject.getInt("cellid");
            }

            return cgi;
        }
        catch(Exception e)
        {
            return null;
        }

    }

    public static List<String> GetTencentWifiInfo(JSONArray jsonArray)
    {
        //"wifis":[{"mac":"fc:d7:33:ba:53:e2","rssi":-70},{"mac":"e4:d3:32:6a:c3:0e","rssi":-74}]
        if (jsonArray == null)
        {
            return null;
        }
        List<String> result = new ArrayList<String>();

        for (int i = 0; i < jsonArray.length(); i++)
        {
            try
            {
                JSONObject jsonObject = jsonArray.getJSONObject(i);

                String mac = null;
                String rssi = null;
                if (jsonObject.has("mac"))
                {
                    mac = jsonObject.getString("mac").replaceAll(":", "");
                }
                if (jsonObject.has("rssi"))
                {
                    rssi = String.valueOf(jsonObject.getInt("rssi")).replaceAll("-", "");
                }
                if (mac != null && rssi != null)
                {
                    result.add(mac + ";" + rssi + ";");
                }

            } catch (JSONException e)
            {
                e.printStackTrace();
            }

        }

        return result;
    }

    public static List<String> GetAmapWifiInfo(String str)
    {
        if (!str.contains("{mac="))
        {
            return null;
        }
        List<String> result = new ArrayList<String>();

        int index = str.indexOf("{mac=");

        String wifiStr = str.substring(index, str.length());

        String[] items = wifiStr.split(";");
        for (String item : items)
        {
            if (item.startsWith("{mac=") && item.endsWith("}"))
            {
                //{mac=fc8b97d4b32e,WifiAP="-Wo-LAN",signal=-89}

                String mac = null;
                String rssi = null;
                String[] abc = item.split(",");
                if(abc!=null && abc.length==3)
                {
                    mac = abc[0].substring(abc[0].indexOf("=")+1,abc[0].length()).replaceAll(":", "");
                    rssi = abc[2].substring(abc[2].indexOf("=")+1,abc[2].length() - 1).replaceAll("-", "");
                }

                if (mac != null && rssi != null)
                {
                    result.add(mac + ";" + rssi + ";");
                }

            }
        }

        return result;
    }

    public static void RefreshLocationFromGetRequest(LocationInfo responseLocationInfo, String host, String originalUrl)
    {
        boolean isLocFound = false;
        String url = null;

        try
        {
            if(originalUrl.contains("%"))
            {
                originalUrl = originalUrl.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
                //originalUrl = originalUrl.replaceAll("\\+", "%2B");
                url = URLDecoder.decode(originalUrl, "UTF-8");
            }
            else
            {
                url = originalUrl;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            url = null;
        }
        finally
        {
            if(url == null)
            {
                return;
            }

            HashMap<String,String> parameterPairs = GetParameterListFromUrl(url);
            if(parameterPairs != null)
            {
                String[] domainItems = reverse(host).split("\\.");
                if(domainItems != null && domainItems.length >= 2)
                {
                    String firstDomain = reverse(domainItems[1]) + "." + reverse(domainItems[0]);
                    if(urlConfig != null && urlConfig.containsKey(firstDomain))
                    {
                        List<XDRLocationMapping> mappingConfigList = urlConfig.get(firstDomain);

                        for(XDRLocationMapping mappingConfig : mappingConfigList)
                        {
                            if(host.equals(mappingConfig.website))
                            {
                                if(mappingConfig.coorTypePrefix != null && !mappingConfig.coorTypePrefix.equals("null") && mappingConfig.coorTypePrefix.length() > 0 && parameterPairs.containsKey(mappingConfig.coorTypePrefix))
                                {
                                    responseLocationInfo.coorType = parameterPairs.get(mappingConfig.coorTypePrefix);
                                }
                                else
                                {
                                    responseLocationInfo.coorType = mappingConfig.coorType;
                                }

                                if(!mappingConfig.longitudePrefix.equals(mappingConfig.latitudePrefix))
                                {
                                    //
                                    if(parameterPairs.containsKey(mappingConfig.longitudePrefix))
                                    {
                                        String longitudeStr = parameterPairs.get(mappingConfig.longitudePrefix);
                                        if(StringUtil.isNum(longitudeStr))
                                        {
                                            responseLocationInfo.longitude = Double.parseDouble(longitudeStr);
                                            isLocFound = true;
                                        }

                                    }
                                    if(parameterPairs.containsKey(mappingConfig.latitudePrefix))
                                    {
                                        String latitudeStr = parameterPairs.get(mappingConfig.latitudePrefix);
                                        if(StringUtil.isNum(latitudeStr))
                                        {
                                            responseLocationInfo.latitude = Double.parseDouble(latitudeStr);
                                            isLocFound = true;
                                        }

                                    }
                                }
                                else
                                {
                                    if (parameterPairs.containsKey(mappingConfig.longitudePrefix))
                                    {
                                        String lnglatStr = parameterPairs.get(mappingConfig.longitudePrefix);

                                        String[] separators = new String[]{",", ";"};
                                        for (String separator : separators)
                                        {
                                            if (lnglatStr.contains(separator))
                                            {
                                                String[] latlng = lnglatStr.split(separator);
                                                if (latlng != null && latlng.length == 2 && StringUtil.isNum(latlng[0]) && StringUtil.isNum(latlng[1]))
                                                {
                                                    double av = Double.valueOf(latlng[0]);
                                                    double bv = Double.valueOf(latlng[1]);

                                                    if (av > bv && GisUtil.isValid(bv,av))
                                                    {
                                                        responseLocationInfo.latitude = bv;
                                                        responseLocationInfo.longitude = av;
                                                        isLocFound = true;
                                                    }
                                                    else if (av < bv && GisUtil.isValid(av,bv))
                                                    {
                                                        responseLocationInfo.latitude = av;
                                                        responseLocationInfo.longitude = bv;
                                                        isLocFound = true;
                                                    }
                                                }
                                                break;
                                            }
                                        }

                                        if (!isLocFound && lnglatStr.contains("|"))
                                        {
                                            String[] latlng = lnglatStr.split("\\|");
                                            if (latlng != null && latlng.length == 2 && StringUtil.isNum(latlng[0]) && StringUtil.isNum(latlng[1]))
                                            {
                                                double av = Double.valueOf(latlng[0]);
                                                double bv = Double.valueOf(latlng[1]);

                                                if (av > bv && GisUtil.isValid(bv, av))
                                                {
                                                    responseLocationInfo.latitude = bv;
                                                    responseLocationInfo.longitude = av;
                                                    isLocFound = true;
                                                }
                                                else if (av < bv && GisUtil.isValid(av, bv))
                                                {
                                                    responseLocationInfo.latitude = av;
                                                    responseLocationInfo.longitude = bv;
                                                    isLocFound = true;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (responseLocationInfo.coorType.equalsIgnoreCase("gcj02"))
                                {
                                    double[] wgsLocation = CoordinateConvert.gcj2WGSExactly(responseLocationInfo.latitude,responseLocationInfo.longitude);
                                    responseLocationInfo.longitude = wgsLocation[1];
                                    responseLocationInfo.latitude = wgsLocation[0];
                                    responseLocationInfo.coorType = "wgs";
                                }
                                else if (responseLocationInfo.coorType.equalsIgnoreCase("bd0911"))
                                {
                                    LatLng bdLoc = new LatLng(responseLocationInfo.latitude,responseLocationInfo.longitude);
                                    LatLng gpsLoc = GisUtil.Baidu2GPS(bdLoc);
                                    responseLocationInfo.longitude = gpsLoc.longitude;
                                    responseLocationInfo.latitude = gpsLoc.latitude;
                                    responseLocationInfo.coorType = "wgs";
                                }

                            }
                        }
                    }
                }
            }

            if(isLocFound)
            {
            	if(isLocFound
            			&& responseLocationInfo.coorType != null
            			&& responseLocationInfo.coorType.toLowerCase().contains("bd09"))
                {
                	double[]  tmpBaiduLoc =CoordinateConvert.bd092WGS(responseLocationInfo.latitude, responseLocationInfo.longitude);
                	responseLocationInfo.latitude = tmpBaiduLoc[0];
                	responseLocationInfo.longitude =  tmpBaiduLoc[1];
                }
            	else if(isLocFound
            			&& responseLocationInfo.coorType != null
            			&& responseLocationInfo.coorType.toLowerCase().contains("gcj02"))
                {
                	double[]  tmpBaiduLoc =CoordinateConvert.gcj2WGSExactly(responseLocationInfo.latitude, responseLocationInfo.longitude);
                	responseLocationInfo.latitude = tmpBaiduLoc[0];
                	responseLocationInfo.longitude =  tmpBaiduLoc[1];
                }
            	else if(isLocFound
            			&& responseLocationInfo.coorType == null)
            	{
            		responseLocationInfo.latitude = 0;
            		responseLocationInfo.longitude = 0;	
            	}
                responseLocationInfo.locationType = "get";
            }
        }
    }

    public static HashMap<String,String> GetParameterListFromUrl(String url)
    {
        if(url == null)
        {
            return null;
        }
        url = StringUtil.UrlReplacePercentage(url);

        if(!url.contains("?") || !url.contains("="))
        {
            return null;
        }
        HashMap<String,String> result = new HashMap<String, String>();
        String parameters = url.substring(url.indexOf("?") + 1, url.length());
        String[] para = parameters.split("&");
        if(para != null && para.length != 0)
        {
            for(String tmp : para) 
            {
                if(tmp.contains("=")) 
                {
                    String[] paraPair = tmp.split("=");
                    if(paraPair != null && paraPair.length == 2) 
                    {
                        result.put(paraPair[0], paraPair[1]);
                    }
                }
            }
        }
        return result;
    }

    public static HashMap<String,List<XDRLocationMapping>> GetUrlMappingConfig(InputStream is)
    {
        HashMap<String,List<XDRLocationMapping>> config = new HashMap<String, List<XDRLocationMapping>>();
        BufferedReader reader = null;
        try
        {
            reader = new BufferedReader(new InputStreamReader(is, "GBK"));
            String line = null;
            while ((line = reader.readLine()) != null)
            {
                String item[] = line.trim().split("\\|");
                if (item.length >= 5)
                {
                    String webSite = item[0];
                    String longitudePrefix = item[1];
                    String latitudePrefix = item[2];
                    String coorTypePrefix = item[3];
                    String coorType = item[4];
                    String[] domainItems = reverse(webSite).split("\\.");
                    String firstDomain = reverse(domainItems[1]) + "." + reverse(domainItems[0]);

                    XDRLocationMapping tmpMapping = new XDRLocationMapping();
                    tmpMapping.website = webSite;
                    tmpMapping.longitudePrefix = longitudePrefix;
                    tmpMapping.latitudePrefix = latitudePrefix;
                    tmpMapping.coorTypePrefix = coorTypePrefix;
                    tmpMapping.coorType = coorType;

                    if(config.containsKey(firstDomain))
                    {
                        config.get(firstDomain).add(tmpMapping);
                    }
                    else
                    {
                        List<XDRLocationMapping> tmpList = new ArrayList<XDRLocationMapping>();
                        tmpList.add(tmpMapping);
                        config.put(firstDomain,tmpList);
                    }
                }
                else
                {
                    System.err.println(line + " has column number not equal to or larger than 3");
                }
            }

        } catch (Exception e)
        {
            e.printStackTrace();
        } 
        finally
        {
            if (reader != null)
            {
                try
                {
                    reader.close();
                } catch (final IOException e)
                {
                    e.printStackTrace();
                }
            }
            return config;
        }
    }
    
    public static HashMap<String,List<XDRLocationMapping>> GetUrlMappingConfig(String fileName)
    {

        HashMap<String,List<XDRLocationMapping>> config = new HashMap<String, List<XDRLocationMapping>>();

        BufferedReader reader = null;
        try
        {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "GBK"));
            String line = null;
            while ((line = reader.readLine()) != null)
            {
                String item[] = line.trim().split("\\|");
                if (item.length >= 5)
                {
                    String webSite = item[0];
                    String longitudePrefix = item[1];
                    String latitudePrefix = item[2];
                    String coorTypePrefix = item[3];
                    String coorType = item[4];
                    String[] domainItems = reverse(webSite).split("\\.");
                    String firstDomain = reverse(domainItems[1]) + "." + reverse(domainItems[0]);

                    XDRLocationMapping tmpMapping = new XDRLocationMapping();
                    tmpMapping.website = webSite;
                    tmpMapping.longitudePrefix = longitudePrefix;
                    tmpMapping.latitudePrefix = latitudePrefix;
                    tmpMapping.coorTypePrefix = coorTypePrefix;
                    tmpMapping.coorType = coorType;

                    if(config.containsKey(firstDomain))
                    {
                        config.get(firstDomain).add(tmpMapping);
                    }
                    else
                    {
                        List<XDRLocationMapping> tmpList = new ArrayList<XDRLocationMapping>();
                        tmpList.add(tmpMapping);
                        config.put(firstDomain,tmpList);
                    }
                }
                else
                {
                    System.err.println(line + " has column number not equal to or larger than 3");
                }
            }

        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            if (reader != null)
            {
                try
                {
                    reader.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            return config;
        }
    }

    public static String reverse(String s)
    {
        char[] array = s.toCharArray();
        String reverse = "";
        for (int i = array.length - 1; i >= 0; i--)
            reverse += array[i];
        return reverse;
    }


}

package cn.mastercom.bigdata.util;


import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

/**
 * @author 邝晓林
 * @version V1.0
 * @Description
 * @date 2018/09/17
 */
public class HostUtil {

    public static final String UNKNOWN_HOST = "UnknownHost";

    public static String getHostNameForLiunx() {
        try {
            return (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException uhe) {
            String host = uhe.getMessage(); // host = "hostname: hostname"
            if (host != null) {
                int colon = host.indexOf(':');
                if (colon > 0) {
                    return host.substring(0, colon);
                }
            }
            return UNKNOWN_HOST;
        }
    }


    public static String getHostName() {
        if (System.getenv("COMPUTERNAME") != null) {
            return System.getenv("COMPUTERNAME");
        } else {
            return getHostNameForLiunx();
        }
    }

    private static String getLocalIP()
    {
        Enumeration<NetworkInterface> interfaces = null;
        try {
            interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr != null && addr instanceof Inet4Address &&!addr.isLinkLocalAddress()&&!addr.isLoopbackAddress()) {
                        System.out.println("ip：" + addr.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            System.out.println("get local ip error: ");
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> getLocalIP2() {
        List<String> localIpList = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> eni = NetworkInterface.getNetworkInterfaces();
            while (eni.hasMoreElements()) {

                NetworkInterface networkCard = eni.nextElement();
                List<InterfaceAddress> ncAddrList = networkCard
                        .getInterfaceAddresses();
                Iterator<InterfaceAddress> ncAddrIterator = ncAddrList.iterator();
                String broadcastAddress = null;
                String ipv4Address = null;
                while (ncAddrIterator.hasNext()) {
                    InterfaceAddress networkCardAddress = ncAddrIterator.next();
                    InetAddress address = networkCardAddress.getAddress();

                    if (address != null && address instanceof Inet4Address && !address.isLoopbackAddress()&&!address.isLinkLocalAddress()) {
                        String hostAddress = address.getHostAddress();

                        ipv4Address = hostAddress;
                        broadcastAddress = networkCardAddress.getBroadcast().getHostAddress();

                        //有ipv4地址  && 有非0.0.0.0的广播号 才是真实ip
                        if (ipv4Address != null && !ipv4Address.isEmpty() && !"0.0.0.0".equals(broadcastAddress)) {
                            localIpList.add(ipv4Address);
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return localIpList;
    }

    public static boolean contains(String ip) {
        return getLocalIP2().contains(ip);
    }

    public static boolean containsOne(List<String> ipList) {
        if (ipList != null && !ipList.isEmpty()){
            List<String> localIpList = getLocalIP2();
//            System.out.println("ip list to filter: " + ipList);
//            System.out.println("local ip list: " + localIpList);
            for (String ip : ipList){
                if (localIpList.contains(ip)){
                    return true;
                }
            }
        }
        return false;
    }

    public static void main(String[] args){
        System.out.println(contains("192.168.3.85"));

    }
}

package cn.mastercom.bigdata.stat.userAna.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * 用于分组同车用户
 */
public class ImsiGroupHelper
{
    class ImsiPair
    {
        public String imsi1;
        public String imsi2;
        public boolean isDone1;
        public boolean isDone2;

        public ImsiPair(String imsi1, String imsi2)
        {
            this.imsi1 = imsi1;
            this.imsi2 = imsi2;

            isDone1 = false;
            isDone2 = false;
        }
    }

    private Map<String, Collection<ImsiPair>> imsi_imsiPairs_Map;
    private Set<ImsiPair> m_imsiPairSet;

    public ImsiGroupHelper()
    {
        imsi_imsiPairs_Map = new HashMap<String, Collection<ImsiPair>>();
        m_imsiPairSet = new HashSet<ImsiPair>();
    }

    public void add(String imsi1, String imsi2)
    {
        ImsiPair imsiPair = new ImsiPair(imsi1, imsi2);
        if (m_imsiPairSet.add(imsiPair))
        {
            add(imsiPair, imsiPair.imsi1);
            add(imsiPair, imsiPair.imsi2);
        }
    }

    public void clear()
    {
        for (Collection<ImsiPair> cen : imsi_imsiPairs_Map.values())
        {
            cen.clear();
        }

        imsi_imsiPairs_Map.clear();
        m_imsiPairSet.clear();
    }

    private void add(ImsiPair imsiPair, String imsi)
    {
        Collection<ImsiPair> imsiPairs = null;
        if (imsi_imsiPairs_Map.containsKey(imsi))
        {
            imsiPairs = imsi_imsiPairs_Map.get(imsi);
        }
        else
        {
            imsiPairs = new ArrayList<ImsiPair>();
            imsi_imsiPairs_Map.put(imsi, imsiPairs);
        }
        imsiPairs.add(imsiPair);
    }

    /*
     * 用户分组
     */
    public List<Set<String>> Group()
    {
        List<Set<String>> imsiSets = new ArrayList<Set<String>>();
        Set<String> okSet = new HashSet<String>();
        for (ImsiPair imsiPair : m_imsiPairSet)
        {
            if (!imsiPair.isDone1) // 此处只需判断一个
            {
                Set<String> imsiSet = new HashSet<String>();
                imsiSets.add(imsiSet);

                group(imsiSet, okSet, imsiPair);
            }
        }

        return imsiSets;
    }

    private void group(Set<String> imsiSet, Set<String> okSet, ImsiPair imsiPair)
    {
        if (!imsiPair.isDone1)
        {
            imsiSet.add(imsiPair.imsi1);
            imsiPair.isDone1 = true;
            groupOne(imsiSet, okSet, imsiPair, imsiPair.imsi1);
        }

        if (!imsiPair.isDone2)
        {
            imsiSet.add(imsiPair.imsi2);
            imsiPair.isDone2 = true;
            groupOne(imsiSet, okSet, imsiPair, imsiPair.imsi2);
        }

    }

    private void groupOne(Set<String> imsiSet, Set<String> okSet, ImsiPair imsiPair, String imsi)
    {
        // 已经蔓延过,则直接返回
        if (okSet.contains(imsi)) return;
        okSet.add(imsi);

        // 必定包含,可不写
        if (imsi_imsiPairs_Map.containsKey(imsi))
        {
            Collection<ImsiPair> imsiPairs = imsi_imsiPairs_Map.get(imsi);

            for (ImsiPair item : imsiPairs)
            {
                // 不计算自身
                if (imsiPair != item)
                {
                    group(imsiSet, okSet, item);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        ImsiGroupHelper gh = new ImsiGroupHelper();

        gh.add("1", "2");
        gh.add("2", "1");
        gh.add("3", "2");
        gh.add("3", "1");
        gh.add("4", "2");
        gh.add("1", "5");
        gh.add("1", "6");
        gh.add("1", "7");

        gh.add("8", "11");
        gh.add("8", "25");
        gh.add("8", "15");
        gh.add("8", "16");
        gh.add("9", "26");

        @SuppressWarnings("unused")
		List<Set<String>> result = gh.Group();
    }

}

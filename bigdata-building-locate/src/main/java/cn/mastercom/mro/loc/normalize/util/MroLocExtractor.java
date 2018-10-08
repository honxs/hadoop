package cn.mastercom.mro.loc.normalize.util;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.PENDING;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.SAVE;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.ItemIndex.*;

public class MroLocExtractor {
    public static final int LOG_LENGTH = 111;
    public static final int TOTAL_LENGTH = 34;
    public static final int OUT_DATE_SECONDS = 60 * 60 * 24 * 60;
    public static class ItemIndex {
        public static final int TIME = 0;
        public static final int ECI = 1;
        public static final int BUILDING_ID = 2;
        public static final int HEIGHT = 3;
        public static final int LTE_SC_RSRP = 4;
        public static final int LTE_SC_TADV = 5;
        public static final int LTE_SC_AOA = 6;
        public static final int LTE_NC_COUNT = 7;
        public static final int LTE_NC_ECI1 = 8;
        public static final int LTE_NC_RSRP1 = 9;
        public static final int LTE_NC_EARFCN1 = 10;
        public static final int LTE_NC_PCI1 = 11;
        public static final int LTE_NC_ECI2 = 12;
        public static final int LTE_NC_RSRP2 = 13;
        public static final int LTE_NC_EARFCN2 = 14;
        public static final int LTE_NC_PCI2 = 15;
        public static final int LTE_NC_ECI3 = 16;
        public static final int LTE_NC_RSRP3 = 17;
        public static final int LTE_NC_EARFCN3 = 18;
        public static final int LTE_NC_PCI3 = 19;
        public static final int LTE_NC_ECI4 = 20;
        public static final int LTE_NC_RSRP4 = 21;
        public static final int LTE_NC_EARFCN4 = 22;
        public static final int LTE_NC_PCI4 = 23;
        public static final int LTE_NC_ECI5 = 24;
        public static final int LTE_NC_RSRP5 = 25;
        public static final int LTE_NC_EARFCN5 = 26;
        public static final int LTE_NC_PCI5 = 27;
        public static final int LTE_NC_ECI6 = 28;
        public static final int LTE_NC_RSRP6 = 29;
        public static final int LTE_NC_EARFCN6 = 30;
        public static final int LTE_NC_PCI6 = 31;
        public static final int SAMPLE_COUNT = 32;
        public static final int FLAG = 33;
    }
    public static class FlagType {
        public static final int DELETE = -2;
        public static final int MERGED = -1;
        public static final int PENDING = 0;
        public static final int SAVE = 1;
    }

    public static int[] newValue(String line) {
        return newValue(line.split("\t"));
    }
    public static int[] newValue(String[] split) {
        if (null == split || LOG_LENGTH != split.length) {
            return null;
        }

        try {
            int[] value = new int[TOTAL_LENGTH];
            value[TIME] = Integer.parseInt(split[1]);
            value[ECI] = Integer.parseInt(split[3]);
            value[BUILDING_ID] = Integer.parseInt(split[4]);
            value[HEIGHT] = Integer.parseInt(split[5]);
            value[LTE_SC_RSRP] = Integer.parseInt(split[14]);
            value[LTE_SC_TADV] = Integer.parseInt(split[20]);
            value[LTE_SC_AOA] = Integer.parseInt(split[21]);
            value[LTE_NC_COUNT] = Integer.parseInt(split[25]);
            value[LTE_NC_RSRP1] = Integer.parseInt(split[27]);
            value[LTE_NC_EARFCN1] = Integer.parseInt(split[29]);
            value[LTE_NC_PCI1] = Integer.parseInt(split[30]);
            value[LTE_NC_ECI1] = Integer.parseInt(split[31]);
            value[LTE_NC_RSRP2] = Integer.parseInt(split[32]);
            value[LTE_NC_EARFCN2] = Integer.parseInt(split[34]);
            value[LTE_NC_PCI2] = Integer.parseInt(split[35]);
            value[LTE_NC_ECI2] = Integer.parseInt(split[36]);
            value[LTE_NC_RSRP3] = Integer.parseInt(split[37]);
            value[LTE_NC_EARFCN3] = Integer.parseInt(split[39]);
            value[LTE_NC_PCI3] = Integer.parseInt(split[40]);
            value[LTE_NC_ECI3] = Integer.parseInt(split[41]);
            value[LTE_NC_RSRP4] = Integer.parseInt(split[42]);
            value[LTE_NC_EARFCN4] = Integer.parseInt(split[44]);
            value[LTE_NC_PCI4] = Integer.parseInt(split[45]);
            value[LTE_NC_ECI4] = Integer.parseInt(split[46]);
            value[LTE_NC_RSRP5] = Integer.parseInt(split[47]);
            value[LTE_NC_EARFCN5] = Integer.parseInt(split[49]);
            value[LTE_NC_PCI5] = Integer.parseInt(split[50]);
            value[LTE_NC_ECI5] = Integer.parseInt(split[51]);
            value[LTE_NC_RSRP6] = Integer.parseInt(split[52]);
            value[LTE_NC_EARFCN6] = Integer.parseInt(split[54]);
            value[LTE_NC_PCI6] = Integer.parseInt(split[55]);
            value[LTE_NC_ECI6] = Integer.parseInt(split[56]);
            value[SAMPLE_COUNT] = 1;
            value[FLAG] = PENDING;
            return value;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static int[] oldValue(String line) {
        return oldValue(line.split("\t"));
    }
    private static int[] oldValue(String[] split) {
        if (null == split || (TOTAL_LENGTH - 1) != split.length || isOutOfDate(split[TIME])) {
            return null;
        }

        int[] value = new int[TOTAL_LENGTH];
        for (int i = 0; i < split.length; i++) {
            value[i] = Integer.parseInt(split[i]);
        }
        value[FLAG] = SAVE;

        return value;
    }
    private static boolean isOutOfDate(String time) {
        return System.currentTimeMillis() / 1000 - Integer.parseInt(time) > OUT_DATE_SECONDS;
    }

    public static String key(int[] value) {
        return value[ECI] + "|" + value[BUILDING_ID];
    }

    public static String outputData(int[] value) {
        StringBuilder sb = new StringBuilder();
        sb.append(value[0]);

        for (int i = 1; i < FLAG; i++) {
            sb.append("\t").append(value[i]);
        }

        return sb.toString();
    }
}

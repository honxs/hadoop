package cn.mastercom.mro.loc.normalize.util;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.*;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.ItemIndex.*;

public class MroLocHandler {
    public static int compareNewWithOld(int[] newValue, int[] oldValue) {
        // 在迭代的过程中迭代到自己了, 则作为新增
        if (PENDING == oldValue[FLAG] || newValue == oldValue) {
            return SAVE;
        }
        // old log已经被标记为无用
        if (oldValue[FLAG] < 0) {
            return PENDING;
        }

        if (shouldMerge(newValue, oldValue)) {
            return MERGED;
        }

        return PENDING;
    }
    private static boolean shouldMerge(int[] newValue, int[] oldValue) {
        return newValue[ECI] == oldValue[ECI] &&
                newValue[LTE_SC_RSRP] == oldValue[LTE_SC_RSRP] &&
                newValue[LTE_SC_TADV] == oldValue[LTE_SC_TADV] &&
                newValue[LTE_SC_AOA] == oldValue[LTE_SC_AOA] &&
                newValue[BUILDING_ID] == oldValue[BUILDING_ID] &&
                ((newValue[HEIGHT] < 0 && oldValue[HEIGHT] < 0) || (newValue[HEIGHT] > 0 && oldValue[HEIGHT] == newValue[HEIGHT])) &&
                hasSameNeighborCell(newValue, oldValue);
    }
    // 两个数据所有邻区相同 且邻区的pci rsrp earf相同
    private static boolean hasSameNeighborCell(int[] newValue, int[] oldValue) {
        if (newValue[LTE_NC_COUNT] != oldValue[LTE_NC_COUNT]) {
            return false;
        }

        final int ncCount = newValue[LTE_NC_COUNT];
        outer:
        for (int i = 0; i < ncCount; i++) {
            for (int j = 0; j < ncCount; j++) {
                boolean hasSame = isSameNeighborCell(newValue, oldValue, i, j);
                if (hasSame) {
                    continue outer;
                }
            }
            return false;
        }

        return true;
    }
    private static boolean isSameNeighborCell(int[] newValue, int[] oldValue, int newNc, int oldNc) {
        final int span = LTE_NC_ECI2 - LTE_NC_ECI1;
        return newValue[LTE_NC_ECI1 + newNc * span] == oldValue[LTE_NC_ECI1 + oldNc * span] &&
                newValue[LTE_NC_PCI1 + newNc * span] == oldValue[LTE_NC_PCI1 + oldNc * span] &&
                newValue[LTE_NC_RSRP1 + newNc * span] == oldValue[LTE_NC_RSRP1 + oldNc * span] &&
                newValue[LTE_NC_EARFCN1 + newNc * span] == oldValue[LTE_NC_EARFCN1 + oldNc * span];
    }

    public static void merge(int[] newValue, int[] oldValue) {
        newValue[FLAG] = MERGED;
        oldValue[SAMPLE_COUNT] += newValue[SAMPLE_COUNT];
        if (newValue[TIME] >= oldValue[TIME]) {
            oldValue[TIME] = newValue[TIME];
        }
        if (oldValue[HEIGHT] < 0) {
            oldValue[HEIGHT] = newValue[HEIGHT];
        }
    }
}

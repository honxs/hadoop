package cn.mastercom.mro.loc.normalize.util;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.*;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.ItemIndex.*;

import static org.junit.Assert.*;

public class MroLocHandlerTest {

    private int[] value1;
    private int[] value2;
    private int[] value3;
    private int[] value4;

    @Before
    public void before() {
        String line = "4509\t1530574213\t0\t19310215\t94961\t-1\t1101378551\t226560564\t469815163669142\t18843929912\t297878708\t\t\t35775005\t-109\t-13\t3683\t67\t0\t0\t4\t-1\t0\t-126.1\t14\t2\t0\t-110\t-12\t3683\t68\t19310216\t-115\t-15\t3683\t66\t19310214\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t-1000000\t3\t3\tMRO\tru3\t-1\t-1\t786\t-1\tstatic";

        value1 = MroLocExtractor.newValue(line);
        value2 = MroLocExtractor.newValue(line);
        value3 = MroLocExtractor.newValue(line);
        value4 = MroLocExtractor.newValue(line);

        value1[FLAG] = SAVE;
        value2[FLAG] = PENDING;
        value3[FLAG] = PENDING;
        value4[FLAG] = MERGED;

        value2[TIME] = 1530574214;

        value3[LTE_NC_ECI1] = value2[LTE_NC_ECI2];
        value3[LTE_NC_PCI1] = value2[LTE_NC_PCI2];
        value3[LTE_NC_RSRP1] = value2[LTE_NC_RSRP2];
        value3[LTE_NC_EARFCN1] = value2[LTE_NC_EARFCN2];
        value3[LTE_NC_ECI2] = value2[LTE_NC_ECI1];
        value3[LTE_NC_PCI2] = value2[LTE_NC_PCI1];
        value3[LTE_NC_RSRP2] = value2[LTE_NC_RSRP1];
        value3[LTE_NC_EARFCN2] = value2[LTE_NC_EARFCN1];

        value4[LTE_NC_EARFCN2] = 100;
    }

    @Test
    public void compareNewWithOld() {
        int flag1 = MroLocHandler.compareNewWithOld(value2, value2);
        assertEquals(SAVE, flag1);

        int flag2 = MroLocHandler.compareNewWithOld(value3, value1);
        assertEquals(MERGED, flag2);

        int flag3 = MroLocHandler.compareNewWithOld(value3, value4);
        assertEquals(PENDING, flag3);
    }

    @Test
    public void shouldMerge() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method shouldMergeMethod = MroLocHandler.class.getDeclaredMethod("shouldMerge", int[].class, int[].class);
        shouldMergeMethod.setAccessible(true);

        assertTrue((Boolean) shouldMergeMethod.invoke(null, value1, value2));
        assertTrue((Boolean) shouldMergeMethod.invoke(null, value1, value3));
        assertFalse((Boolean) shouldMergeMethod.invoke(null, value1, value4));
    }

    @Test
    public void merge() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method mergeMethod = MroLocHandler.class.getDeclaredMethod("merge", int[].class, int[].class);
        mergeMethod.setAccessible(true);

        mergeMethod.invoke(null, value2, value1);
        assertEquals(1530574214, value1[TIME]);
        assertEquals(2, value1[SAMPLE_COUNT]);
        assertEquals(MERGED, value2[FLAG]);
    }
}
package cn.mastercom.mro.loc.normalize.util;

import org.junit.Before;
import org.junit.Test;

import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.FlagType.*;
import static cn.mastercom.mro.loc.normalize.util.MroLocExtractor.ItemIndex.*;
import static org.junit.Assert.assertEquals;

public class MroLocExtractorTest {

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
    public void newValue() {
        assertEquals(1530574213, value1[TIME]);
        assertEquals( -109, value1[LTE_SC_RSRP]);
        assertEquals(-1000000, value1[LTE_NC_RSRP6]);
    }
}
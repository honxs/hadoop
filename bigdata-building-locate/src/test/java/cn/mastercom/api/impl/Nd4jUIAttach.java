package cn.mastercom.api.impl;

import org.deeplearning4j.api.storage.StatsStorage;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.ui.api.UIServer;
import org.deeplearning4j.ui.stats.StatsListener;
import org.deeplearning4j.ui.storage.InMemoryStatsStorage;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Created by Kwong on 2018/9/1.
 */
public class Nd4jUIAttach implements Answer<MultiLayerNetwork> {

    StatsStorage statsStorage;

    public Nd4jUIAttach() {
        UIServer uiServer = UIServer.getInstance();
        statsStorage = new InMemoryStatsStorage();
        uiServer.attach(statsStorage);
    }

    @Override
    public MultiLayerNetwork answer(InvocationOnMock invocationOnMock) throws Throwable {
        MultiLayerNetwork result = (MultiLayerNetwork) invocationOnMock.callRealMethod();
        result.setListeners(new StatsListener(statsStorage));
        return result;
    }

}

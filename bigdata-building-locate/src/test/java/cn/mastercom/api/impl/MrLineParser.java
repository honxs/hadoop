package cn.mastercom.api.impl;

import cn.mastercom.bean.EciMrData;
import cn.mastercom.util.FileReader;

import java.util.List;

import static cn.mastercom.util.ValueUtils.getValidInt;

/**
 * Created by Kwong on 2018/9/1.
 */
public class MrLineParser implements FileReader.LineHandler{

    final List<EciMrData> dataList;

    public MrLineParser(List<EciMrData> dataList){
        this.dataList= dataList;
    }

    @Override
    public void handle(String line) {
        String[] words = line.split("\t", -1);

        if (words.length < 100) return;

        dataList.add(new EciMrData(words[8], getValidInt(words[3]), getValidInt(words[4]), getValidInt(words[14]), getValidInt(words[15]), getValidInt(words[18]), getValidInt(words[20]), getValidInt(words[21]), getValidInt(words[22]), getValidInt(words[24]), getValidInt(words[25]), getValidInt(words[27]), getValidInt(words[28]), getValidInt(words[29]), getValidInt(words[30]), getValidInt(words[32]), getValidInt(words[33]), getValidInt(words[34]), getValidInt(words[35]), getValidInt(words[37]), getValidInt(words[38]), getValidInt(words[39]), getValidInt(words[40]), getValidInt(words[42]), getValidInt(words[43]), getValidInt(words[44]), getValidInt(words[45]), getValidInt(words[47]), getValidInt(words[48]), getValidInt(words[49]), getValidInt(words[50]), getValidInt(words[52]), getValidInt(words[53]), getValidInt(words[54]), getValidInt(words[55]), getValidInt(words[31]), getValidInt(words[36]), getValidInt(words[41]), getValidInt(words[46]), getValidInt(words[51]), getValidInt(words[56])));
    }
}

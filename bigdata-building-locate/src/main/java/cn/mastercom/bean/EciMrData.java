//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cn.mastercom.bean;

import lombok.ToString;

import java.io.Serializable;

@ToString
public class EciMrData implements Serializable{
    private String imsi;
    private int eci;
    private int building;
    private double Longitude;
    private double Latitude;
    private int LteScRSRP;
    private int LteScRSRQ;
    private int LteScBSR;
    private int LteScTadv;
    private int LteScAOA;
    private int LteScPHR;
    private int LteScSinrUL;
    private int Nccount0;
    private int LteNcRSRP1;
    private int LteNcRSRQ1;
    private int LteNcEarfcn1;
    private int LteNcPci1;
    private int LteNcRSRP2;
    private int LteNcRSRQ2;
    private int LteNcEarfcn2;
    private int LteNcPci2;
    private int LteNcRSRP3;
    private int LteNcRSRQ3;
    private int LteNcEarfcn3;
    private int LteNcPci3;
    private int LteNcRSRP4;
    private int LteNcRSRQ4;
    private int LteNcEarfcn4;
    private int LteNcPci4;
    private int LteNcRSRP5;
    private int LteNcRSRQ5;
    private int LteNcEarfcn5;
    private int LteNcPci5;
    private int LteNcRSRP6;
    private int LteNcRSRQ6;
    private int LteNcEarfcn6;
    private int LteNcPci6;
    private int floor = 0;
    private int ltenceci1;
    private int ltenceci2;
    private int ltenceci3;
    private int ltenceci4;
    private int ltenceci5;
    private int ltenceci6;

    public int getBuilding() {
        return this.building;
    }

    public void setBuilding(int building) {
        this.building = building;
    }

    public int getLtenceci1() {
        return this.ltenceci1;
    }

    public void setLtenceci1(int ltenceci1) {
        this.ltenceci1 = ltenceci1;
    }

    public int getLtenceci2() {
        return this.ltenceci2;
    }

    public void setLtenceci2(int ltenceci2) {
        this.ltenceci2 = ltenceci2;
    }

    public int getLtenceci3() {
        return this.ltenceci3;
    }

    public void setLtenceci3(int ltenceci3) {
        this.ltenceci3 = ltenceci3;
    }

    public int getLtenceci4() {
        return this.ltenceci4;
    }

    public void setLtenceci4(int ltenceci4) {
        this.ltenceci4 = ltenceci4;
    }

    public int getLtenceci5() {
        return this.ltenceci5;
    }

    public void setLtenceci5(int ltenceci5) {
        this.ltenceci5 = ltenceci5;
    }

    public int getLtenceci6() {
        return this.ltenceci6;
    }

    public void setLtenceci6(int ltenceci6) {
        this.ltenceci6 = ltenceci6;
    }

    public void setFloor(int floor) {
        this.floor = floor;
    }

    public String getImsi() {
        return this.imsi;
    }

    public int getEci() {
        return this.eci;
    }

    public double getLongitude() {
        return this.Longitude;
    }

    public int getFloor() {
        return this.floor;
    }

    public double getLatitude() {
        return this.Latitude;
    }

    public int getLteScRSRP() {
        return this.LteScRSRP;
    }

    public int getLteScRSRQ() {
        return this.LteScRSRQ;
    }

    public int getLteScBSR() {
        return this.LteScBSR;
    }

    public int getLteScTadv() {
        return this.LteScTadv;
    }

    public int getLteScAOA() {
        return this.LteScAOA;
    }

    public int getLteScPHR() {
        return this.LteScPHR;
    }

    public int getLteScSinrUL() {
        return this.LteScSinrUL;
    }

    public int getNccount0() {
        return this.Nccount0;
    }

    public int getLteNcRSRP1() {
        return this.LteNcRSRP1;
    }

    public int getLteNcRSRQ1() {
        return this.LteNcRSRQ1;
    }

    public int getLteNcEarfcn1() {
        return this.LteNcEarfcn1;
    }

    public int getLteNcPci1() {
        return this.LteNcPci1;
    }

    public int getLteNcRSRP2() {
        return this.LteNcRSRP2;
    }

    public int getLteNcRSRQ2() {
        return this.LteNcRSRQ2;
    }

    public int getLteNcEarfcn2() {
        return this.LteNcEarfcn2;
    }

    public int getLteNcPci2() {
        return this.LteNcPci2;
    }

    public int getLteNcRSRP3() {
        return this.LteNcRSRP3;
    }

    public int getLteNcRSRQ3() {
        return this.LteNcRSRQ3;
    }

    public int getLteNcEarfcn3() {
        return this.LteNcEarfcn3;
    }

    public int getLteNcPci3() {
        return this.LteNcPci3;
    }

    public int getLteNcRSRP4() {
        return this.LteNcRSRP4;
    }

    public int getLteNcRSRQ4() {
        return this.LteNcRSRQ4;
    }

    public int getLteNcEarfcn4() {
        return this.LteNcEarfcn4;
    }

    public int getLteNcPci4() {
        return this.LteNcPci4;
    }

    public int getLteNcRSRP5() {
        return this.LteNcRSRP5;
    }

    public int getLteNcRSRQ5() {
        return this.LteNcRSRQ5;
    }

    public int getLteNcEarfcn5() {
        return this.LteNcEarfcn5;
    }

    public int getLteNcPci5() {
        return this.LteNcPci5;
    }

    public int getLteNcRSRP6() {
        return this.LteNcRSRP6;
    }

    public int getLteNcRSRQ6() {
        return this.LteNcRSRQ6;
    }

    public int getLteNcEarfcn6() {
        return this.LteNcEarfcn6;
    }

    public int getLteNcPci6() {
        return this.LteNcPci6;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public void setEci(int eci) {
        this.eci = eci;
    }

    public void setLongitude(double longitude) {
        this.Longitude = longitude;
    }

    public void setLatitude(double latitude) {
        this.Latitude = latitude;
    }

    public void setLteScRSRP(int lteScRSRP) {
        this.LteScRSRP = lteScRSRP;
    }

    public void setLteScRSRQ(int lteScRSRQ) {
        this.LteScRSRQ = lteScRSRQ;
    }

    public void setLteScBSR(int lteScBSR) {
        this.LteScBSR = lteScBSR;
    }

    public void setLteScTadv(int lteScTadv) {
        this.LteScTadv = lteScTadv;
    }

    public void setLteScAOA(int lteScAOA) {
        this.LteScAOA = lteScAOA;
    }

    public void setLteScPHR(int lteScPHR) {
        this.LteScPHR = lteScPHR;
    }

    public void setLteScSinrUL(int lteScSinrUL) {
        this.LteScSinrUL = lteScSinrUL;
    }

    public void setNccount0(int nccount0) {
        this.Nccount0 = nccount0;
    }

    public void setLteNcRSRP1(int lteNcRSRP1) {
        this.LteNcRSRP1 = lteNcRSRP1;
    }

    public void setLteNcRSRQ1(int lteNcRSRQ1) {
        this.LteNcRSRQ1 = lteNcRSRQ1;
    }

    public void setLteNcEarfcn1(int lteNcEarfcn1) {
        this.LteNcEarfcn1 = lteNcEarfcn1;
    }

    public void setLteNcPci1(int lteNcPci1) {
        this.LteNcPci1 = lteNcPci1;
    }

    public void setLteNcRSRP2(int lteNcRSRP2) {
        this.LteNcRSRP2 = lteNcRSRP2;
    }

    public void setLteNcRSRQ2(int lteNcRSRQ2) {
        this.LteNcRSRQ2 = lteNcRSRQ2;
    }

    public void setLteNcEarfcn2(int lteNcEarfcn2) {
        this.LteNcEarfcn2 = lteNcEarfcn2;
    }

    public void setLteNcPci2(int lteNcPci2) {
        this.LteNcPci2 = lteNcPci2;
    }

    public void setLteNcRSRP3(int lteNcRSRP3) {
        this.LteNcRSRP3 = lteNcRSRP3;
    }

    public void setLteNcRSRQ3(int lteNcRSRQ3) {
        this.LteNcRSRQ3 = lteNcRSRQ3;
    }

    public void setLteNcEarfcn3(int lteNcEarfcn3) {
        this.LteNcEarfcn3 = lteNcEarfcn3;
    }

    public void setLteNcPci3(int lteNcPci3) {
        this.LteNcPci3 = lteNcPci3;
    }

    public void setLteNcRSRP4(int lteNcRSRP4) {
        this.LteNcRSRP4 = lteNcRSRP4;
    }

    public void setLteNcRSRQ4(int lteNcRSRQ4) {
        this.LteNcRSRQ4 = lteNcRSRQ4;
    }

    public void setLteNcEarfcn4(int lteNcEarfcn4) {
        this.LteNcEarfcn4 = lteNcEarfcn4;
    }

    public void setLteNcPci4(int lteNcPci4) {
        this.LteNcPci4 = lteNcPci4;
    }

    public void setLteNcRSRP5(int lteNcRSRP5) {
        this.LteNcRSRP5 = lteNcRSRP5;
    }

    public void setLteNcRSRQ5(int lteNcRSRQ5) {
        this.LteNcRSRQ5 = lteNcRSRQ5;
    }

    public void setLteNcEarfcn5(int lteNcEarfcn5) {
        this.LteNcEarfcn5 = lteNcEarfcn5;
    }

    public void setLteNcPci5(int lteNcPci5) {
        this.LteNcPci5 = lteNcPci5;
    }

    public void setLteNcRSRP6(int lteNcRSRP6) {
        this.LteNcRSRP6 = lteNcRSRP6;
    }

    public void setLteNcRSRQ6(int lteNcRSRQ6) {
        this.LteNcRSRQ6 = lteNcRSRQ6;
    }

    public void setLteNcEarfcn6(int lteNcEarfcn6) {
        this.LteNcEarfcn6 = lteNcEarfcn6;
    }

    public void setLteNcPci6(int lteNcPci6) {
        this.LteNcPci6 = lteNcPci6;
    }

    public EciMrData(String imsi, int eci, int longitude, int latitude, int lteScRSRP, int lteScRSRQ, int lteScBSR, int lteScTadv, int lteScAOA, int lteScPHR, int lteScSinrUL, int nccount0, int lteNcRSRP1, int lteNcRSRQ1, int lteNcEarfcn1, int lteNcPci1, int lteNcRSRP2, int lteNcRSRQ2, int lteNcEarfcn2, int lteNcPci2, int lteNcRSRP3, int lteNcRSRQ3, int lteNcEarfcn3, int lteNcPci3, int lteNcRSRP4, int lteNcRSRQ4, int lteNcEarfcn4, int lteNcPci4, int lteNcRSRP5, int lteNcRSRQ5, int lteNcEarfcn5, int lteNcPci5, int lteNcRSRP6, int lteNcRSRQ6, int lteNcEarfcn6, int lteNcPci6) {
        this.imsi = imsi;
        this.eci = eci;
        this.Longitude = (double)longitude;
        this.Latitude = (double)latitude;
        this.LteScRSRP = lteScRSRP;
        this.LteScRSRQ = lteScRSRQ;
        this.LteScBSR = lteScBSR;
        this.LteScTadv = lteScTadv;
        this.LteScAOA = lteScAOA;
        this.LteScPHR = lteScPHR;
        this.LteScSinrUL = lteScSinrUL;
        this.Nccount0 = nccount0;
        this.LteNcRSRP1 = lteNcRSRP1;
        this.LteNcRSRQ1 = lteNcRSRQ1;
        this.LteNcEarfcn1 = lteNcEarfcn1;
        this.LteNcPci1 = lteNcPci1;
        this.LteNcRSRP2 = lteNcRSRP2;
        this.LteNcRSRQ2 = lteNcRSRQ2;
        this.LteNcEarfcn2 = lteNcEarfcn2;
        this.LteNcPci2 = lteNcPci2;
        this.LteNcRSRP3 = lteNcRSRP3;
        this.LteNcRSRQ3 = lteNcRSRQ3;
        this.LteNcEarfcn3 = lteNcEarfcn3;
        this.LteNcPci3 = lteNcPci3;
        this.LteNcRSRP4 = lteNcRSRP4;
        this.LteNcRSRQ4 = lteNcRSRQ4;
        this.LteNcEarfcn4 = lteNcEarfcn4;
        this.LteNcPci4 = lteNcPci4;
        this.LteNcRSRP5 = lteNcRSRP5;
        this.LteNcRSRQ5 = lteNcRSRQ5;
        this.LteNcEarfcn5 = lteNcEarfcn5;
        this.LteNcPci5 = lteNcPci5;
        this.LteNcRSRP6 = lteNcRSRP6;
        this.LteNcRSRQ6 = lteNcRSRQ6;
        this.LteNcEarfcn6 = lteNcEarfcn6;
        this.LteNcPci6 = lteNcPci6;
    }

    public EciMrData(String imsi, int eci, int lteScRSRP, int lteScRSRQ, int lteScBSR, int lteScTadv, int lteScAOA, int lteScPHR, int lteScSinrUL, int nccount0, int lteNcRSRP1, int lteNcRSRQ1, int lteNcEarfcn1, int lteNcPci1, int lteNcRSRP2, int lteNcRSRQ2, int lteNcEarfcn2, int lteNcPci2, int lteNcRSRP3, int lteNcRSRQ3, int lteNcEarfcn3, int lteNcPci3, int lteNcRSRP4, int lteNcRSRQ4, int lteNcEarfcn4, int lteNcPci4, int lteNcRSRP5, int lteNcRSRQ5, int lteNcEarfcn5, int lteNcPci5, int lteNcRSRP6, int lteNcRSRQ6, int lteNcEarfcn6, int lteNcPci6) {
        this.imsi = imsi;
        this.eci = eci;
        this.LteScRSRP = lteScRSRP;
        this.LteScRSRQ = lteScRSRQ;
        this.LteScBSR = lteScBSR;
        this.LteScTadv = lteScTadv;
        this.LteScAOA = lteScAOA;
        this.LteScPHR = lteScPHR;
        this.LteScSinrUL = lteScSinrUL;
        this.Nccount0 = nccount0;
        this.LteNcRSRP1 = lteNcRSRP1;
        this.LteNcRSRQ1 = lteNcRSRQ1;
        this.LteNcEarfcn1 = lteNcEarfcn1;
        this.LteNcPci1 = lteNcPci1;
        this.LteNcRSRP2 = lteNcRSRP2;
        this.LteNcRSRQ2 = lteNcRSRQ2;
        this.LteNcEarfcn2 = lteNcEarfcn2;
        this.LteNcPci2 = lteNcPci2;
        this.LteNcRSRP3 = lteNcRSRP3;
        this.LteNcRSRQ3 = lteNcRSRQ3;
        this.LteNcEarfcn3 = lteNcEarfcn3;
        this.LteNcPci3 = lteNcPci3;
        this.LteNcRSRP4 = lteNcRSRP4;
        this.LteNcRSRQ4 = lteNcRSRQ4;
        this.LteNcEarfcn4 = lteNcEarfcn4;
        this.LteNcPci4 = lteNcPci4;
        this.LteNcRSRP5 = lteNcRSRP5;
        this.LteNcRSRQ5 = lteNcRSRQ5;
        this.LteNcEarfcn5 = lteNcEarfcn5;
        this.LteNcPci5 = lteNcPci5;
        this.LteNcRSRP6 = lteNcRSRP6;
        this.LteNcRSRQ6 = lteNcRSRQ6;
        this.LteNcEarfcn6 = lteNcEarfcn6;
        this.LteNcPci6 = lteNcPci6;
    }

    public EciMrData(String imsi, int eci, int buildingid, int lteScRSRP, int lteScRSRQ, int lteScBSR, int lteScTadv, int lteScAOA, int lteScPHR, int lteScSinrUL, int nccount0, int lteNcRSRP1, int lteNcRSRQ1, int lteNcEarfcn1, int lteNcPci1, int lteNcRSRP2, int lteNcRSRQ2, int lteNcEarfcn2, int lteNcPci2, int lteNcRSRP3, int lteNcRSRQ3, int lteNcEarfcn3, int lteNcPci3, int lteNcRSRP4, int lteNcRSRQ4, int lteNcEarfcn4, int lteNcPci4, int lteNcRSRP5, int lteNcRSRQ5, int lteNcEarfcn5, int lteNcPci5, int lteNcRSRP6, int lteNcRSRQ6, int lteNcEarfcn6, int lteNcPci6) {
        this.imsi = imsi;
        this.eci = eci;
        this.building = buildingid;
        this.LteScRSRP = lteScRSRP;
        this.LteScRSRQ = lteScRSRQ;
        this.LteScBSR = lteScBSR;
        this.LteScTadv = lteScTadv;
        this.LteScAOA = lteScAOA;
        this.LteScPHR = lteScPHR;
        this.LteScSinrUL = lteScSinrUL;
        this.Nccount0 = nccount0;
        this.LteNcRSRP1 = lteNcRSRP1;
        this.LteNcRSRQ1 = lteNcRSRQ1;
        this.LteNcEarfcn1 = lteNcEarfcn1;
        this.LteNcPci1 = lteNcPci1;
        this.LteNcRSRP2 = lteNcRSRP2;
        this.LteNcRSRQ2 = lteNcRSRQ2;
        this.LteNcEarfcn2 = lteNcEarfcn2;
        this.LteNcPci2 = lteNcPci2;
        this.LteNcRSRP3 = lteNcRSRP3;
        this.LteNcRSRQ3 = lteNcRSRQ3;
        this.LteNcEarfcn3 = lteNcEarfcn3;
        this.LteNcPci3 = lteNcPci3;
        this.LteNcRSRP4 = lteNcRSRP4;
        this.LteNcRSRQ4 = lteNcRSRQ4;
        this.LteNcEarfcn4 = lteNcEarfcn4;
        this.LteNcPci4 = lteNcPci4;
        this.LteNcRSRP5 = lteNcRSRP5;
        this.LteNcRSRQ5 = lteNcRSRQ5;
        this.LteNcEarfcn5 = lteNcEarfcn5;
        this.LteNcPci5 = lteNcPci5;
        this.LteNcRSRP6 = lteNcRSRP6;
        this.LteNcRSRQ6 = lteNcRSRQ6;
        this.LteNcEarfcn6 = lteNcEarfcn6;
        this.LteNcPci6 = lteNcPci6;
    }

    public EciMrData(String imsi, int eci, int buildingid, int lteScRSRP, int lteScRSRQ, int lteScBSR, int lteScTadv, int lteScAOA, int lteScPHR, int lteScSinrUL, int nccount0, int lteNcRSRP1, int lteNcRSRQ1, int lteNcEarfcn1, int lteNcPci1, int lteNcRSRP2, int lteNcRSRQ2, int lteNcEarfcn2, int lteNcPci2, int lteNcRSRP3, int lteNcRSRQ3, int lteNcEarfcn3, int lteNcPci3, int lteNcRSRP4, int lteNcRSRQ4, int lteNcEarfcn4, int lteNcPci4, int lteNcRSRP5, int lteNcRSRQ5, int lteNcEarfcn5, int lteNcPci5, int lteNcRSRP6, int lteNcRSRQ6, int lteNcEarfcn6, int lteNcPci6, int ltenceci1, int ltenceci2, int ltenceci3, int ltenceci4, int ltenceci5, int ltenceci6) {
        this.imsi = imsi;
        this.eci = eci;
        this.building = buildingid;
        this.LteScRSRP = lteScRSRP;
        this.LteScRSRQ = lteScRSRQ;
        this.LteScBSR = lteScBSR;
        this.LteScTadv = lteScTadv;
        this.LteScAOA = lteScAOA;
        this.LteScPHR = lteScPHR;
        this.LteScSinrUL = lteScSinrUL;
        this.Nccount0 = nccount0;
        this.LteNcRSRP1 = lteNcRSRP1;
        this.LteNcRSRQ1 = lteNcRSRQ1;
        this.LteNcEarfcn1 = lteNcEarfcn1;
        this.LteNcPci1 = lteNcPci1;
        this.LteNcRSRP2 = lteNcRSRP2;
        this.LteNcRSRQ2 = lteNcRSRQ2;
        this.LteNcEarfcn2 = lteNcEarfcn2;
        this.LteNcPci2 = lteNcPci2;
        this.LteNcRSRP3 = lteNcRSRP3;
        this.LteNcRSRQ3 = lteNcRSRQ3;
        this.LteNcEarfcn3 = lteNcEarfcn3;
        this.LteNcPci3 = lteNcPci3;
        this.LteNcRSRP4 = lteNcRSRP4;
        this.LteNcRSRQ4 = lteNcRSRQ4;
        this.LteNcEarfcn4 = lteNcEarfcn4;
        this.LteNcPci4 = lteNcPci4;
        this.LteNcRSRP5 = lteNcRSRP5;
        this.LteNcRSRQ5 = lteNcRSRQ5;
        this.LteNcEarfcn5 = lteNcEarfcn5;
        this.LteNcPci5 = lteNcPci5;
        this.LteNcRSRP6 = lteNcRSRP6;
        this.LteNcRSRQ6 = lteNcRSRQ6;
        this.LteNcEarfcn6 = lteNcEarfcn6;
        this.LteNcPci6 = lteNcPci6;
        this.ltenceci1 = ltenceci1;
        this.ltenceci2 = ltenceci2;
        this.ltenceci3 = ltenceci3;
        this.ltenceci4 = ltenceci4;
        this.ltenceci5 = ltenceci5;
        this.ltenceci6 = ltenceci6;
    }
}

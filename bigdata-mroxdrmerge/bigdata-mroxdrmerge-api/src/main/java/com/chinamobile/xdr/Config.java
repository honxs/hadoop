package com.chinamobile.xdr;


import com.chinamobile.util.NotProguard;

import java.util.ArrayList;

@NotProguard
public class Config  {


    private ArrayList<Integer> cell_mr=new ArrayList<Integer>();
    private ArrayList<Integer> s11=new ArrayList<Integer>();
    private ArrayList<Integer> s1_mme=new ArrayList<Integer>();
    private ArrayList<Integer> s1_u=new ArrayList<Integer>();
    private ArrayList<Integer> s6a=new ArrayList<Integer>();
    private ArrayList<Integer> sgs=new ArrayList<Integer>();
    private ArrayList<Integer> ue_mr=new ArrayList<Integer>();
    private ArrayList<Integer> uu=new ArrayList<Integer>();
    private ArrayList<Integer> x2=new ArrayList<Integer>();

    public ArrayList<Integer> getUu() {
        return uu;
    }

    public ArrayList<Integer> getCell_mr() {
        return cell_mr;
    }

    public ArrayList<Integer> getS11() {
        return s11;
    }

    public ArrayList<Integer> getS1_mme() {
        return s1_mme;
    }

    public ArrayList<Integer> getS1_u() {
        return s1_u;
    }

    public ArrayList<Integer> getS6a() {
        return s6a;
    }

    public ArrayList<Integer> getSgs() {
        return sgs;
    }

    public ArrayList<Integer> getUe_mr() {
        return ue_mr;
    }

    public ArrayList<Integer> getX2() {
        return x2;
    }
}

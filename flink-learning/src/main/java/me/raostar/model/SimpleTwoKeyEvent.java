package me.raostar.model;

import java.io.Serializable;

public class SimpleTwoKeyEvent implements Serializable {

    private String idNo;
    private double amt;


    private String key2;
    private long eventTimestamp;

    public SimpleTwoKeyEvent(String idNo, String key2,double amt) {

        this.eventTimestamp = System.currentTimeMillis();
        this.idNo = idNo;
        this.key2 = key2;
        this.amt = amt;

    }

    public String getIdNo() {
        return idNo;
    }

    public double getAmt() {
        return amt;
    }

    public long getCreationTime() {
        return this.eventTimestamp;
    }


    public String getKey2() {
        return key2;
    }

    public void setKey2(String key2) {
        this.key2 = key2;
    }
    public String toString(){
        return "Event: "+
                "idNo: "+idNo+"|"+
                "key2: "+key2+"|"+

                "amt: "+amt+"|"+
                "eventTimestamp: "+eventTimestamp+"|";


    }
}


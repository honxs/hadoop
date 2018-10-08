package cn.mastercom.bigdata.evt.locall.model;

public class ImmLocItem implements Comparable<ImmLocItem> {
    public int time;
    public int latitude;
    public int longitude;
    public int confidentType;

    public ImmLocItem(int time,int latitude, int longitude, int confidentType) {
        this.time = time;
        this.latitude = latitude;
        this.longitude = longitude;
        this.confidentType = confidentType;
    }

    @Override
    public int compareTo(ImmLocItem o) {
        if(o!=null){
            if(this.time>o.time){
                return 1;
            }else if(this.time==o.time){
                return 0;
            }
        }
        return -1;
    }
}

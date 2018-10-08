package cn.mastercom.bigdata.util;

public interface IConfigure
{
    public boolean loadConfigure();
    public boolean saveConfigure();
    
    public Object getValue(String name);
    public Object getValue(String name, Object defaultValue);
    public boolean setValue(String name, Object value);
    
}

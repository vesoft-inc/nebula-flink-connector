package org.apache.flink.connector.nebula.utils;

import com.vesoft.nebula.meta.PropertyType;
import junit.framework.TestCase;

public class NebulaUtilsTest extends TestCase {

    public void testGetHostAndPorts() {
        assert(NebulaUtils.getHostAndPorts("127.0.0.1:9669").size() == 1);
        assert(NebulaUtils.getHostAndPorts("127.0.0.1:9669,127.0.0.1:9670").size() == 2);
        try{
            NebulaUtils.getHostAndPorts(null);
        } catch (IllegalArgumentException e){
            assert (true);
        } catch (Exception e){
            assert(false);
        }

        try{
            NebulaUtils.getHostAndPorts("127.0.0.1");
        } catch (IllegalArgumentException e){
            assert(true);
        }catch(Exception e){
            assert(false);
        }
    }

    public void testIsNumeric() {
        assert(NebulaUtils.isNumeric("123456"));
        assert(NebulaUtils.isNumeric("0123456"));
        assert(NebulaUtils.isNumeric("-123456"));
        assert(NebulaUtils.isNumeric("000"));
        assert(!NebulaUtils.isNumeric("aaa"));
        assert(!NebulaUtils.isNumeric("0123aaa"));
        assert(!NebulaUtils.isNumeric("123a8"));
    }

    public void testExtraValue() {
        assert(null == NebulaUtils.extraValue(null, PropertyType.STRING));
        assert("\"\"".equals(NebulaUtils.extraValue("", PropertyType.STRING)));
        assert("\"\"".equals(NebulaUtils.extraValue("", PropertyType.FIXED_STRING)));
        assert("1".equals( NebulaUtils.extraValue(1, PropertyType.INT8)));
        assert("timestamp(\"2021-01-01T12:12:12\")".equals(NebulaUtils.extraValue("2021-01-01T12:12:12", PropertyType.TIMESTAMP)));
        assert("datetime(\"2021-01-01T12:12:12\")".equals(NebulaUtils.extraValue("2021-01-01T12:12:12", PropertyType.DATETIME)));
        assert("date(\"2021-01-01\")".equals(NebulaUtils.extraValue("2021-01-01", PropertyType.DATE)));
        assert("time(\"12:12:12\")".equals(NebulaUtils.extraValue("12:12:12", PropertyType.TIME)));
    }

    public void testMkString() {
        assertEquals("\"test\"",NebulaUtils.mkString("test", "\"", "", "\""));
        assertEquals("\"t,e,s,t\"", NebulaUtils.mkString("test", "\"", ",", "\""));
    }
}
package com.bnc.cirrostream.metadata.service;

import com.bnc.cirrostream.metadata.service.impl.H2MdSaveServiceImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author RockeyCui
 */
public class MdSaveServiceFactory {
    private String key;

    private Map<String, MdSaveService> impl = new HashMap<>();


    public static MdSaveServiceFactory getInstance() {
        return LazyHolder.INSTANCE;
    }

    private MdSaveServiceFactory() {
        impl.put("H2", new H2MdSaveServiceImpl());
    }

    private static class LazyHolder {
        private static final MdSaveServiceFactory INSTANCE = new MdSaveServiceFactory();
    }

    public MdSaveService getService() {
        return impl.get(key);
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}

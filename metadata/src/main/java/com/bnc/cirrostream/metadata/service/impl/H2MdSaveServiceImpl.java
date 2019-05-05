package com.bnc.cirrostream.metadata.service.impl;

import com.bnc.cirrostream.metadata.service.MdSaveService;
import com.bnc.cirrostream.metadata.util.H2ConnFactoryUtil;

/**
 * @author RockeyCui
 */
public class H2MdSaveServiceImpl implements MdSaveService {
    private H2ConnFactoryUtil h2ConnFactoryUtil;


    @Override
    public void save() {
        System.out.println("H2 save");

    }
}

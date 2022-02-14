package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private String segmentName;
    private Path segmentPath;
    private long currentSize;
    private SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
        this.currentSize = currentSize;
        this.segmentName = segmentName;
        this.index = index;
        this.segmentPath = segmentPath;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
        this.currentSize = currentSize;
        this.segmentName = segmentName;
        this.segmentPath = tablePath.resolve(segmentName);
        this.index = new SegmentIndex();
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }
}

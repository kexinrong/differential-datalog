package com.vmware.ddlog.util;

import org.jooq.Record;

/**
 * Interface for custom callback functions that process the deltas
 */
public interface DeltaCallBack {
    public enum DeltaType {
        ADD, DEL
    }

    public void processDelta(DeltaType type, Record record);
}
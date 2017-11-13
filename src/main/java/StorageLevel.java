import org.json.simple.JsonObject;

import java.math.BigDecimal;

public class StorageLevel {
    boolean unpersisted;
    boolean onDisk;
    boolean onMemory;
    boolean deserialized;
    int replication;

    public StorageLevel() {
        // constructor called with no arguments returns unpersisted storagelevel
        this.unpersisted = true;
        this.onDisk = false;
        this.onMemory = false;
        this.deserialized = false;
        this.replication = 0;
    }

    public StorageLevel(boolean onDisk, boolean onMemory, boolean deserialized, int replication) {
        this.unpersisted = false;
        this.onDisk = onDisk;
        this.onMemory = onMemory;
        this.deserialized = deserialized;
        this.replication = replication;
    }

    public StorageLevel(JsonObject storageLevelJson) {
        this.unpersisted = false;
        this.onDisk = (boolean) storageLevelJson.get("Use Disk");
        this.onMemory = (boolean) storageLevelJson.get("Use Memory");
        this.deserialized = (boolean) storageLevelJson.get("Deserialized");
        this.replication = ((BigDecimal) storageLevelJson.get("Replication")).intValue();
    }

    // getter
    public boolean isOnDisk() {
        return onDisk;
    }

    public boolean isOnMemory() {
        return onMemory;
    }

    public boolean isUnpersisted() {
        return unpersisted;
    }

    public boolean isCached() {
        return onMemory || onDisk;
    }

    //print
    public void print() {
        print("");
    }

    public void print(String tab) {
        System.out.println(tab + "storage level");
        System.out.println(tab + "onDisk : " + String.valueOf(onDisk));
        System.out.println(tab + "onMemory : " + String.valueOf(onMemory));
        System.out.println(tab + "deserialized : " + String.valueOf(deserialized));
        System.out.println(tab + "replication : " + String.valueOf(replication));
    }
}

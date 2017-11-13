import org.json.simple.JsonObject;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.Queue;

public class Partition {
    int index;
    String partitionId;
    int memorySize;
    int diskSize;
    StorageLevel storageLevel;                              // physical cache status
    Queue<StorageLevel> history;

    public Partition(int index) {
        this.index = index;
        this.partitionId = "";
        this.storageLevel = new StorageLevel(false, false, false, 0);
        this.memorySize = 0;
        this.diskSize = 0;
        this.history = new LinkedList<>();
    }

    // getter
    public boolean isCached() {
        return storageLevel.isCached();
    }

    // setter
    public void updateStatus(JsonObject statusJson) {
        JsonObject storageLevelJson = (JsonObject) statusJson.get("Storage Level");
        StorageLevel prev = this.storageLevel;
        this.storageLevel = new StorageLevel(storageLevelJson);
        this.history.add(prev);
        this.memorySize = ((BigDecimal) statusJson.get("Memory Size")).intValue();
        this.diskSize = ((BigDecimal) statusJson.get("Disk Size")).intValue();

    }

    // print
    public void print() {
        print("");
    }

    public int getIndex() {
        return index;
    }

    public void print(String tab) {
        System.out.println(tab + "PartitionId : " + String.valueOf(partitionId));
        System.out.println(tab + "  memorySize : " + String.valueOf(memorySize));
        System.out.println(tab + "  diskSize : " + String.valueOf(diskSize));
        storageLevel.print(tab + "  ");

    }

}

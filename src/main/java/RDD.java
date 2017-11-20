import org.json.simple.JsonObject;

import java.math.BigDecimal;
import java.util.*;


public class RDD implements Comparable<RDD> {
    int rddId;
    String callsite;
    List<RDD> parents;
    HashMap<Integer, Partition> partitions;
    StorageLevel storageLevel;                              // Annotated cache status
    int numPartitions;
    int numCachedPartitions;
    int memorySize;
    int diskSize;
    boolean hasBeenCached;                                  // whether partition has been physically cached or not

    List<StorageLevel> storageLevelHistory;

    CacheState cacheState;
    enum CacheState {
        ANNOTATED,
        CACHED,
        UNPERSISTED,
        NONE
    }

    Map<Integer, Integer> usageCounter;                     // usage counter per stage to help generating usage info
    boolean descendantCached;

    int[] usageInfo;
    // index 0 - was cached when re-used
    // index 1 - was partially cached when re-used
    // index 2 - not cached when reused, because app didn't cache
    // index 3 - not cached when reused, because had been evicted before re-use
    // index 4 - not cached when reused, because had been unpersisted before re-use
    // index 5 - cached, but not used because stage descendant was cached
    // index 6 - not cached, but OK because stage descendant was cached

    public RDD(JsonObject rddJson) {
        rddId = ((BigDecimal) rddJson.get("RDD ID")).intValue();
        callsite = (String) rddJson.get("Callsite");

        // TODO :: cleaning up code for grabing Scope information from string hashmap
        String value = (String) rddJson.get("Scope");
        value = value.substring(1, value.length()-1);           //remove curly brackets
        String[] keyValuePairs = value.split(",");              //split the string to creat key-value pairs
        for(String pair : keyValuePairs)                        //iterate over the pairs
        {
            String[] entry = pair.split(":");                   //split the pairs to get key and value
            if ("\"name\"".equals(entry[0])) {
                callsite += " - "+entry[1].substring(1, entry[1].length()-1);
                break;
            }
        }

        JsonObject storageLevelJson = (JsonObject) rddJson.get("Storage Level");
        storageLevel = new StorageLevel(storageLevelJson);

        numPartitions = ((BigDecimal) rddJson.get("Number of Partitions")).intValue();
        numCachedPartitions = ((BigDecimal) rddJson.get("Number of Cached Partitions")).intValue();
        memorySize = ((BigDecimal) rddJson.get("Memory Size")).intValue();
        diskSize = ((BigDecimal) rddJson.get("Disk Size")).intValue();

        parents = new LinkedList<>();
        partitions = new HashMap<>();
        usageCounter = new HashMap<>();
        descendantCached = true;

        storageLevelHistory = new LinkedList<>();
        usageInfo = new int[]{0, 0, 0, 0, 0, 0, 0};
        hasBeenCached = false;

        cacheState = CacheState.NONE;
        if (isAnnotated()) {
            cacheState = CacheState.ANNOTATED;
        }
    }

    // getter

    public int getId() {
        return rddId;
    }

    public List<RDD> getParents() {
        return parents;
    }

    public boolean isAnnotated() {
        return storageLevel.isCached();
    }

    public boolean isCached() {
        return cacheState == CacheState.CACHED;
    }

    public boolean isUnpersisted() {
        return cacheState == CacheState.UNPERSISTED;
    }

    public Partition getPartition(int id) {

        return partitions.get(id);
    }

    // setter
    public void addParent(RDD parent) {
        parents.add(parent);
    }

    public void addPartition(int id, Partition partition) {
        partitions.put(id, partition);
    }

    public void unpersist() {
        cacheState = CacheState.UNPERSISTED;

        StorageLevel prev = this.storageLevel;
        this.storageLevel = new StorageLevel();
        this.storageLevelHistory.add(prev);

        memorySize = 0;
        diskSize = 0;
        numCachedPartitions = 0;

    }

    public void incrementUsage(int stageId) {
        if (usageCounter.containsKey(stageId)) {
            usageCounter.put(stageId, usageCounter.get(stageId) + 1);
        } else {
            usageCounter.put(stageId, 1);
        }
    }

    public void updatePartitionStatus(int partitionId, JsonObject statusJson) {
        // partition is either being cached or evicted
        Partition partition = partitions.get(partitionId);
        partition.updateStatus(statusJson);
        if (partition.isCached()) {
            hasBeenCached = true;
            numCachedPartitions++;
            cacheState = CacheState.CACHED;
        } else {
            numCachedPartitions--;
            if (numCachedPartitions == 0) {
                cacheState = CacheState.ANNOTATED;
            }
        }
    }

    public void updateStateCounter(int stageId, Boolean descendantCached, Set<Integer> stageRdds) {

        // index 0 - was cached when re-used
        // index 1 - was partially cached when re-used
        // index 2 - not cached when reused, because app didn't cache
        // index 3 - not cached when reused, because had been evicted before re-use
        // index 4 - not cached when reused, because had been unpersisted before re-use
        // index 5 - cached, but not used because stage descendant was cached
        // index 6 - not cached, but OK because stage descendant was cached

        usageCounter.put(stageId, usageCounter.get(stageId) - 1);
        this.descendantCached = descendantCached;

        if (usageCounter.get(stageId) != 0) return;

        // update current state counter
        // proceed with recursion

        int index = -1;

        if (this.descendantCached) {
            if (cacheState == CacheState.CACHED) {
                // cached, but not used because stage descendant was cached
                index = 5;
            } else {
                // not cached, but OK because stage descendant was cached
                index = 6;
            }
        } else {
            switch (cacheState) {
                case ANNOTATED:
                    // anotated but nothing was in cache
//                    System.out.println("USE OF EMPTY RDD " + Integer.toString(rddId) + " BY STAGE " + Integer.toString(stageId));
                    index = 2;
                    if (hasBeenCached) {
                        // evicted
                        index = 3;
                    }
                    break;
                case UNPERSISTED:
                    // unpersisted
//                    System.out.println("USE OF UNPERSISTED RDD " + Integer.toString(rddId) + " BY STAGE " + Integer.toString(stageId));
                    index = 4;
                    break;
                case CACHED:
                    // used to notify ancestors
                    // if you want to only keep track fully stored cached, put this inside of following if case
                    this.descendantCached = true;
                    if (numPartitions == numCachedPartitions) {
                        // fully stored
                        index = 0;
                    } else {
                        // partially in cache
                        index = 1;
                    }
                    break;
                case NONE:
                    index = 2;
                    break;
            }
        }

        usageInfo[index] += 1;

        // enable to see updates as it happen
        // printUsageInfo(index);

        boolean recursion = true;
        for (RDD parent : parents) {
            if (!stageRdds.contains(parent.getId())) {
                recursion = false;
            }
        }

        if (recursion) {
            for (RDD parent : parents) {
                parent.updateStateCounter(stageId, this.descendantCached, stageRdds);
            }
        }
    }

    // print
    public void print() {
        print("");
    }

    public void print(String tab) {
        System.out.println(tab + "RDD ID : " + Integer.toString(rddId) + " (" + callsite + ") : ");
        storageLevel.print(tab + "  ");

        if (partitions.size() > 0) {
            System.out.println(tab + "  " + "< PARTITIONS >");
            for (Map.Entry<Integer, Partition> entry : partitions.entrySet()) {
                entry.getValue().print(tab + "  ");
            }
        }
    }

    public void printCacheStatus(String tab) {

        StringBuilder sb = new StringBuilder();
        sb.append(tab + "RDD " + Integer.toString(rddId) + " (" + callsite + ") : ");

        switch (cacheState) {
            case ANNOTATED:
                sb.append("ANNOTATED (" + numPartitions + ")");
                break;
            case UNPERSISTED:
                // unpersisted
                sb.append("UNPERSISTED");
                break;
            case CACHED:
                if (numPartitions == numCachedPartitions) {
                    // fully stored
                    sb.append("FULLY CACHED (" + numPartitions + ")");
                } else {
                    // partially in cache
                    sb.append("PARTIALLY CACHED (");
                    sb.append(numCachedPartitions + "/" + numPartitions + ")");
                }
                break;
            case NONE:
                sb.append("NOT CACHED");
                break;
        }
        System.out.println(sb.toString());

    }

    public int compareTo(RDD other) {
        return rddId - other.getId();
    }

    public void printRDDStats(String tab) {
        System.out.println(tab + "RDD " + rddId);
    }

    public void printUsage() {
        StringBuffer sb = new StringBuffer("RDD " + Integer.toString(rddId) + " { ");

        for (int i : usageInfo) {
            sb.append(i);
            sb.append(" ");
        }
        sb.append("}");

        if (usageInfo[2] > 5) {
            sb.append(" - better to cache");
        } else if (usageInfo[3] > 1) {
            sb.append(" - partitions not in cache");
        } else if (usageInfo[4] > 1) {
            sb.append(" - use of unpersisted cache");
        }


        System.out.println(sb.toString());
    }

    private void printUsageInfo(int index) {
        // index 0 - was cached when re-used
        // index 1 - was partially cached when re-used
        // index 2 - not cached when reused, because app didn't cache
        // index 3 - not cached when reused, because had been evicted before re-use
        // index 4 - not cached when reused, because had been unpersisted before re-use
        // index 5 - cached, but not used because stage descendant was cached
        // index 6 - not cached, but OK because stage descendant was cached

        StringBuilder sb = new StringBuilder();
        sb.append("     RDD " + Integer.toString(rddId) + " : ");
        switch (index) {
            case 0:
                sb.append("All partitions are in cache on ");
                break;
            case 1:
                sb.append(numCachedPartitions);
                sb.append("/");
                sb.append(numPartitions);
                sb.append(" partitions are in cache on ");
                break;
            case 2:
                sb.append("Not cached because not annotated");
                break;
            case 3:
                sb.append("Annotated but no partition is in cache");
                break;
            case 4:
                sb.append("Previously cached, but unpersisted");
                break;
            case 5:
                sb.append("Not cached but stage descendant was cached");
                break;
            case 6:
                sb.append("not cached, but OK because stage descendant was cached");
                break;
            default:
                // TODO :: handle error
                System.err.println("Unhandled usage info");
        }

        System.out.println(sb.toString());
    }
}

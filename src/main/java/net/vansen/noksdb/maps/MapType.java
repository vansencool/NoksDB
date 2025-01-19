package net.vansen.noksdb.maps;

/**
 * Enums for the available maps in NoksDB.
 * <p>
 * Please check the <a href="https://github.com/vansencool/NoksDB/tree/main/results">benchmarks</a> for more information.
 */
public enum MapType {

    /**
     * Pretty good, is recommended the most for now as it does not have any issues (A better new map type may be added in the future).
     */
    CONCURRENT_HASH_MAP,

    /**
     * Not really recommended.
     */
    HASH_MAP,

    /**
     * Experimental map, may be removed in the future, use at your own risk (Have seen some issues in {@link net.vansen.noksdb.bulk.BulkBuilder}, but it works now. But may have more issues).
     */
    NOKS_MAP
}

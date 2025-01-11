package net.vansen.noksdb.compression;

import com.github.luben.zstd.Zstd;

public class ZstdCompression implements Compression {

    @Override
    public byte[] compress(byte[] data) {
        return Zstd.compress(data, 22);
    }

    @Override
    public byte[] decompress(byte[] data, int length) {
        return Zstd.decompress(data, length);
    }

    @Override
    public boolean writeLength() {
        return true;
    }
}
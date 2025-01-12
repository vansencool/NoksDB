package net.vansen.noksdb.setup;

import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.compression.Compression;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * A builder class for configuring and creating a new {@link NoksDB} instance.
 */
@SuppressWarnings("unused")
public class NoksDBSetup {

    /**
     * The file used to store the database.
     * Default: "noksdb.dat".
     */
    public File storageFile = new File("noksdb.dat");

    /**
     * Whether auto-saving is enabled. If true, the database is saved after every write.
     * Default: true.
     */
    public boolean autoSave = true;

    /**
     * Whether auto-saving should run asynchronously.
     * Default: false.
     */
    public boolean autoSaveAsync = false;

    /**
     * The compression method to use for saving the database.
     * Default: null (no compression).
     */
    public Compression compressor;

    /**
     * The executor service used for asynchronous tasks.
     * Default: null (creates a default executor if not provided).
     */
    public ExecutorService executor;

    /**
     * Sets the file used to store the database.
     *
     * @param file The storage file.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup storageFile(@NotNull File file) {
        this.storageFile = file;
        return this;
    }

    /**
     * Enables or disables auto-saving.
     *
     * @param autoSave True to enable auto-saving, false to disable it.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup autoSave(boolean autoSave) {
        this.autoSave = autoSave;
        return this;
    }

    /**
     * Enables asynchronous auto-saving.
     *
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup autoSaveAsync() {
        this.autoSaveAsync = true;
        return this;
    }

    /**
     * Sets the executor service for handling asynchronous tasks.
     *
     * @param executor The executor service to use.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup executor(@NotNull ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    /**
     * Sets the compression method for saving and loading the database.
     *
     * @param compressor The compression method to use.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup compression(Compression compressor) {
        this.compressor = compressor;
        return this;
    }

    /**
     * Builds a new {@link NoksDB} instance with the configured settings.
     *
     * @return A new {@link NoksDB} instance.
     */
    public NoksDB build() {
        return new NoksDB(this);
    }
}
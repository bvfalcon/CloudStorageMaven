package com.gkatzioura.maven.cloud.abs.plugin;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

import java.time.Duration;
import java.util.Iterator;
import java.util.function.Consumer;

public class PrefixKeysIterator implements Iterator<BlobItem> {

    private final BlobContainerClient cloudBlobContainer;
    private final String prefix;

    private final Iterator<BlobItem> tempListing;

    public PrefixKeysIterator(final BlobContainerClient cloudBlobContainer, final String prefix) {
        this.cloudBlobContainer = cloudBlobContainer;
        this.prefix = prefix;
        this.tempListing = cloudBlobContainer.listBlobs(new ListBlobsOptions().setPrefix(prefix), Duration.ofMinutes(1)).iterator();
    }

    @Override
    public boolean hasNext() {
        return tempListing.hasNext();
    }

    @Override
    public BlobItem next() {
        return tempListing.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachRemaining(Consumer<? super BlobItem> action) {
        throw new UnsupportedOperationException();
    }
}

/*
 * Copyright 2018 Emmanouil Gkatziouras
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gkatzioura.maven.cloud.abs;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.gkatzioura.maven.cloud.transfer.TransferProgress;
import com.gkatzioura.maven.cloud.transfer.TransferProgressFileInputStream;
import com.gkatzioura.maven.cloud.transfer.TransferProgressFileOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.gkatzioura.maven.cloud.abs.ContentTypeResolver.getContentType;

public class AzureStorageRepository {

    private final String container;
    private final AzureClientFactory azureClientFactory;
    private BlobContainerClient blobContainer;

    private static final Logger LOGGER = Logger.getLogger(AzureStorageRepository.class.getName());

    public AzureStorageRepository(String directory) {
        this.azureClientFactory = new AzureClientFactory();
        this.container = directory;
    }

    public void connect(AuthenticationInfo authenticationInfo) throws AuthenticationException {

        try {
            BlobServiceClient storageClient = azureClientFactory.create(authenticationInfo);
            blobContainer = storageClient.getBlobContainerClient(container);

        } catch (BlobStorageException e) {
            throw new AuthenticationException("Provide valid credentials");
        }
    }

    public void copy(String resourceName, File destination, TransferProgress transferProgress) throws ResourceDoesNotExistException {

        LOGGER.log(Level.FINER, String.format("Downloading key %s from container %s into %s", resourceName, container, destination.getAbsolutePath()));

        try {

            BlobClient cloudBlob = blobContainer.getBlobClient(resourceName);

            if (!cloudBlob.exists()) {
                LOGGER.log(Level.FINER, "Blob {} does not exist", resourceName);
                throw new ResourceDoesNotExistException(resourceName);
            }

            try (OutputStream outputStream = new TransferProgressFileOutputStream(destination, transferProgress);
                 InputStream inputStream = cloudBlob.openInputStream()) {
                IOUtils.copy(inputStream, outputStream);
            }
        } catch (BlobStorageException | IOException e) {
            throw new ResourceDoesNotExistException("Could not download file from repo", e);
        }
    }

    public boolean newResourceAvailable(String resourceName, long timeStamp) throws ResourceDoesNotExistException {

        LOGGER.log(Level.FINER, String.format("Checking if new key %s exists", resourceName));

        try {
            BlobClient cloudBlob = blobContainer.getBlobClient(resourceName);
            if (!cloudBlob.exists()) {
                return false;
            }

            long updated = cloudBlob.getProperties().getLastModified().toInstant().toEpochMilli();
            return updated > timeStamp;
        } catch (BlobStorageException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch cloud blob", e);
            throw new ResourceDoesNotExistException(resourceName);
        }
    }

    public void put(File file, String destination, TransferProgress transferProgress) throws TransferFailedException {

        LOGGER.log(Level.FINER, String.format("Uploading key %s ", destination));
        try {

            BlobClient blob = blobContainer.getBlobClient(destination);
            try (InputStream inputStream = new TransferProgressFileInputStream(file, transferProgress)) {
                BlobHttpHeaders headers = new BlobHttpHeaders();
                headers.setContentType(getContentType(file));
                blob.uploadWithResponse(inputStream, file.length(), null, headers, null, null, new BlobRequestConditions(), null, Context.NONE);
            }
        } catch (BlobStorageException | IOException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch cloud blob", e);
            throw new TransferFailedException(destination);
        }
    }


    public boolean exists(String resourceName) throws TransferFailedException {

        try {
            BlobContainerClient blob = this.blobContainer;
            blob.getBlobClient(resourceName);
            return blob.exists();
        } catch (BlobStorageException e) {
            LOGGER.log(Level.SEVERE, "Could not fetch cloud blob", e);
            throw new TransferFailedException(resourceName);
        }
    }

    public List<String> list(String path) {
        LOGGER.info(String.format("Listing files for %s", path));
        List<String> blobs = new ArrayList<>();
        Iterable<BlobItem> blobItems = blobContainer.listBlobs();
        for (BlobItem blobItem : blobItems) {
            blobs.add(blobItem.getName());
        }
        return blobs;
    }

    public void disconnect() {
        // don't disconnect after each module
//        blobContainer = null;
    }

}

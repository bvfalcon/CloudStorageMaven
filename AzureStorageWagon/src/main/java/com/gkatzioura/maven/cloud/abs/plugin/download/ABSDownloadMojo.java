package com.gkatzioura.maven.cloud.abs.plugin.download;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.gkatzioura.maven.cloud.KeyIteratorConcated;
import com.gkatzioura.maven.cloud.abs.AzureClientFactory;
import com.gkatzioura.maven.cloud.abs.plugin.PrefixKeysIterator;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.wagon.authentication.AuthenticationException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Mojo(name = "abs-download")
public class ABSDownloadMojo extends AbstractMojo {

    private BlobServiceClient cloudStorageAccount;

    @Parameter(property = "abs-download.container")
    private String container;

    @Parameter(property = "abs-download.keys")
    private List<String> keys;

    @Parameter(property = "abs-download.downloadPath")
    private String downloadPath;

    private static final Logger LOGGER = Logger.getLogger(ABSDownloadMojo.class.getName());

    public ABSDownloadMojo(String container, List<String> keys, String downloadPath) throws AuthenticationException {
        this();
        this.container = container;
        this.keys = keys;
        this.downloadPath = downloadPath;
    }

    public ABSDownloadMojo() throws AuthenticationException {
        try {
            String connectionString = new AzureClientFactory().create();
            cloudStorageAccount = new BlobServiceClientBuilder()
                    .endpoint(connectionString)
                    .buildClient();

        } catch (Exception e) {
            throw new AuthenticationException("Could not setup azure client", e);
        }
    }

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            BlobContainerClient blobContainer = cloudStorageAccount.getBlobContainerClient(container);
//            blobContainer.getMetadata();

            if (keys.size() == 1) {
                downloadSingleFile(blobContainer, keys.get(0));
                return;
            }

            List<Iterator<BlobItem>> prefixKeysIterators = keys.stream()
                    .map(pi -> new PrefixKeysIterator(blobContainer, pi))
                    .collect(Collectors.toList());
            Iterator<BlobItem> keyIteratorConcatenated = new KeyIteratorConcated<BlobItem>(prefixKeysIterators);

            while (keyIteratorConcatenated.hasNext()) {
                BlobItem key = keyIteratorConcatenated.next();
                downloadFile(blobContainer, key);
            }

        } catch (BlobStorageException e) {
            throw new MojoFailureException("Could not get container " + container, e);
        }
    }

    private void downloadSingleFile(BlobContainerClient cloudBlobContainer, String key) throws MojoExecutionException {
        File file = new File(downloadPath);

        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }

        try {
            BlobClient cloudBlob = cloudBlobContainer.getBlobClient(key);

            if (!cloudBlob.exists()) {
                LOGGER.log(Level.FINER, "Blob {} does not exist", key);
                throw new MojoExecutionException("Could not find blob " + key);
            }

            try (BlobInputStream blobInputStream = cloudBlob.openInputStream();
                 FileOutputStream fileOutputStream = new FileOutputStream(file)
            ) {
                IOUtils.copy(blobInputStream, fileOutputStream);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Could not download abs file");
                throw new MojoExecutionException("Could not download abs file " + key);
            }
        } catch (BlobStorageException e) {
            throw new MojoExecutionException("Could not fetch abs file " + key, e);
        }
    }

    private void downloadFile(BlobContainerClient cloudBlobContainer, BlobItem listBlobItem) throws MojoExecutionException {
        // todo

//        String key = listBlobItem.getUri().getPath().replace("/" + container + "/", "");
        String key = "";
        File file = new File(createFullFilePath(key));

        if (file.getParent() != null) {
            file.getParentFile().mkdirs();
        }

        if (isDirectory(cloudBlobContainer, key)) {
            return;
        }

        final BlobClient cloudBlob;

        try {
            cloudBlob = cloudBlobContainer.getBlobClient(key);
        } catch (BlobStorageException e) {
            throw new MojoExecutionException("Could not fetch abs file " + key, e);
        }

        try (InputStream objectInputStream = cloudBlob.openInputStream();
             FileOutputStream fileOutputStream = new FileOutputStream(file)
        ) {
            IOUtils.copy(objectInputStream, fileOutputStream);
        } catch (IOException | BlobStorageException e) {
            LOGGER.log(Level.SEVERE, "Could not download abs file");
            throw new MojoExecutionException("Could not download abs file " + key, e);
        }
    }

    private final String createFullFilePath(String key) {
        String fullPath = downloadPath + "/" + key;
        return fullPath;
    }

    private final boolean isDirectory(BlobContainerClient container, String key) {
        try {
            // todo
//            return container.getDirectoryReference(key).listBlobs().iterator().hasNext();
            return false;
        } catch (BlobStorageException e) {
            LOGGER.log(Level.SEVERE, "Abs key is not a directory");
            return false;
        }
    }

}

package com.gkatzioura.maven.cloud.abs.plugin.upload;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.gkatzioura.maven.cloud.abs.AzureClientFactory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.wagon.authentication.AuthenticationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static com.gkatzioura.maven.cloud.abs.ContentTypeResolver.getContentType;

@Mojo(name = "abs-upload")
public class ABSUploadMojo extends AbstractMojo {

    private BlobServiceClient cloudStorageAccount;

    @Parameter(property = "abs-upload.container")
    private String container;

    @Parameter(property = "abs-upload.path")
    private String path;

    @Parameter(property = "abs-upload.key")
    private String key;

    public ABSUploadMojo() throws AuthenticationException {
        try {
            String connectionString = new AzureClientFactory().create();
            cloudStorageAccount = new BlobServiceClientBuilder()
                    .endpoint(connectionString)
                    .buildClient();
        } catch (Exception e) {
            throw new AuthenticationException("Could not setup azure client", e);
        }
    }

    public ABSUploadMojo(String container, String path, String key) throws AuthenticationException {
        this();
        this.container = container;
        this.path = path;
        this.key = key;
    }

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            BlobContainerClient blobContainer = cloudStorageAccount.getBlobContainerClient(container);

            if (isDirectory()) {
                List<String> filesToUpload = findFilesToUpload(path);

                for (String fileToUpload : filesToUpload) {
                    String generateKeyName = generateKeyName(fileToUpload);
                    uploadFileToStorage(blobContainer, generateKeyName, new File(fileToUpload));
                }
            } else {
                uploadFileToStorage(blobContainer, keyIfNull(), new File(path));
            }

        } catch (BlobStorageException e) {
            throw new MojoFailureException("Could not get container " + container, e);
        }
    }

    private List<String> findFilesToUpload(String filePath) {
        List<String> totalFiles = new ArrayList<>();

        File file = new File(filePath);

        if (file.isDirectory()) {
            File[] files = file.listFiles();

            for (File lFile : files) {
                if (lFile.isDirectory()) {
                    List<String> filesFound = findFilesToUpload(lFile.getAbsolutePath());
                    totalFiles.addAll(filesFound);
                } else {
                    totalFiles.add(lFile.getAbsolutePath());
                }
            }

        } else {
            totalFiles.add(file.getAbsolutePath());
        }

        return totalFiles;
    }

    private String generateKeyName(String fullFilePath) {
        StringBuilder keyNameBuilder = new StringBuilder();

        String absolutePath = new File(path).getAbsolutePath();

        if (key != null) {
            keyNameBuilder.append(key);
            if (!fullFilePath.startsWith("/")) {
                keyNameBuilder.append("/");
            }
            keyNameBuilder.append(fullFilePath.replace(absolutePath, ""));
        } else {
            final String clearFilePath = fullFilePath.replace(absolutePath, "");
            final String filePathToAppend = clearFilePath.startsWith("/") ? clearFilePath.replaceFirst("/", "") : clearFilePath;
            keyNameBuilder.append(filePathToAppend);
        }
        return keyNameBuilder.toString();
    }

    private void uploadFileToStorage(BlobContainerClient blobContainer, String key, File file) throws MojoExecutionException {
        try {
            BlobClient blob = blobContainer.getBlobClient(key);
            try (InputStream inputStream = new FileInputStream(file)) {
                BlobHttpHeaders headers = new BlobHttpHeaders();
                headers.setContentType(getContentType(file));
                blob.uploadWithResponse(inputStream, file.length(), null, headers, null, null, new BlobRequestConditions(), null, Context.NONE);
            }
        } catch (IOException | BlobStorageException e) {
            throw new MojoExecutionException("Could not upload file " + file.getName(), e);
        }
    }

    private boolean isDirectory() {
        return new File(path).isDirectory();
    }

    private String keyIfNull() {
        if (key == null) {
            return new File(path).getName();
        } else {
            return key;
        }
    }

}

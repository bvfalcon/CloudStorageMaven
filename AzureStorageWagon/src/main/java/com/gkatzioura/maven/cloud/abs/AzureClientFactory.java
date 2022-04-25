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

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;

import java.util.logging.Logger;

public class AzureClientFactory {

    private static final String CONNECTION_STRING_TEMPLATE = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net";

    private static final Logger LOGGER = Logger.getLogger(AzureClientFactory.class.getName());

    public BlobServiceClient create(AuthenticationInfo authenticationInfo) throws AuthenticationException {

        if (authenticationInfo == null) {
            throw new AuthenticationException("Please provide storage account credentials");
        }

        String username = authenticationInfo.getUserName();
        String password = authenticationInfo.getPassword();

        if (username == null || username.isEmpty()) {
            // if no username is provided, then we expect that the password is a shared access signature (SAS) URL
            return new BlobServiceClientBuilder()
                    .endpoint(password)
                    .buildClient();

        } else if (authenticationInfo.getPassphrase() != null && !authenticationInfo.getPassphrase().isEmpty()) {
            // if no password is provided, then we expect that the username is the path to a properties file, which contains
            // the following properties: client_id, client_secret, tenant_id and storage_account_url properties,
            // for use when authenticating via Azure AD
            ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                    .clientId(authenticationInfo.getUserName())
                    .clientSecret(authenticationInfo.getPassword())
                    .tenantId(authenticationInfo.getPassphrase())
                    .build();
            return new BlobServiceClientBuilder()
                    .endpoint(authenticationInfo.getPrivateKey())
                    .credential(credential)
                    .buildClient();

        } else {
            return new BlobServiceClientBuilder()
                    .connectionString(String.format(CONNECTION_STRING_TEMPLATE, username, password))
                    .buildClient();
        }
    }

    /**
     * This shall create the connection string based on the environmental params
     *
     * @return
     * @throws AuthenticationException
     */
    public String create() throws AuthenticationException {
        String accountName = System.getenv("ACCOUNT_NAME");
        String accountKey = System.getenv("ACCOUNT_KEY");

        if (accountName == null || accountKey == null) {
            throw new AuthenticationException("Please provide storage account credentials using environmental variables");
        }

        return String.format(CONNECTION_STRING_TEMPLATE, accountName, accountKey);
    }

}

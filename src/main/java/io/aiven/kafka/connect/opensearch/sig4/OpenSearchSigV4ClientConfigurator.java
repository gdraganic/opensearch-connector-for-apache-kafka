/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.opensearch.sig4;

import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_ASSUME_ROLE_ARN_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG;

import java.util.Objects;

import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;
import io.aiven.kafka.connect.opensearch.spi.OpenSearchClientConfigurator;

import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Configures AWS SigV4 request signing on the OpenSearch HTTP client.
 *
 * <p>
 * Credential resolution order:
 * <ol>
 * <li>If {@code aws.access_key_id} and {@code aws.secret_access_key} are set, static credentials are used.</li>
 * <li>If {@code aws.assume_role_arn} is set, STS AssumeRole is used.</li>
 * <li>If the {@code AWS_ROLE_ARN} environment variable is present, STS AssumeRole is used with the default credential
 * chain as source credentials (supports IRSA role chaining in EKS).</li>
 * <li>Otherwise the AWS SDK {@link DefaultCredentialsProvider} chain is used (IRSA, environment variables, EC2
 * instance profiles, etc.).</li>
 * </ol>
 *
 * @see <a href="https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/pull/357">Upstream PR #357</a>
 */
public class OpenSearchSigV4ClientConfigurator implements OpenSearchClientConfigurator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSigV4ClientConfigurator.class);

    @Override
    public boolean apply(final OpenSearchSinkConnectorConfig config, final HttpAsyncClientBuilder builder) {
        return false;
    }

    public static AwsCredentialsProvider buildCredentialsProvider(final OpenSearchSinkConnectorConfig config,
            final String region) {

        // 1. Explicit static credentials
        final String accessKeyId = config.getString(AWS_ACCESS_KEY_ID_CONFIG);
        final Password secretAccessKey = config.getPassword(AWS_SECRET_ACCESS_KEY_CONFIG);
        if (Objects.nonNull(accessKeyId) && Objects.nonNull(secretAccessKey)) {
            LOGGER.info("Using static AWS credentials");
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey.value()));
        }

        // 2. STS AssumeRole when aws.assume_role_arn is configured or AWS_ROLE_ARN env var is set
        String roleArn = config.getString(AWS_ASSUME_ROLE_ARN_CONFIG);
        if (roleArn == null || roleArn.isBlank()) {
            roleArn = System.getenv("AWS_ROLE_ARN");
        }
        if (roleArn != null && !roleArn.isBlank()) {
            LOGGER.info("Configuring STS AssumeRole credentials for role {}", roleArn);
            final String sessionName = "kafka-connect-opensearch-" + System.currentTimeMillis();
            final var stsClient = StsClient.builder().region(Region.of(region)).build();
            final var assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(roleArn)
                    .roleSessionName(sessionName)
                    .build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequest)
                    .build();
        }

        // 3. Default credentials provider chain
        LOGGER.info("Using AWS default credentials provider chain");
        return DefaultCredentialsProvider.create();
    }
}

/*
 * Copyright 2019 Aiven Oy
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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

public class OpenSearchSigV4ConfigDefContributor implements ConfigDefContributor {

    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access_key_id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret_access_key";
    public static final String AWS_REGION_CONFIG = "aws.region";
    public static final String AWS_SERVICE_NAME_CONFIG = "aws.service.name";

    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS Access key id. "
            + "When provided together with aws.secret_access_key, static credentials are used. "
            + "Otherwise the AWS SDK default credentials provider chain is used.";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS secret access key. "
            + "When provided together with aws.access_key_id, static credentials are used. "
            + "Otherwise the AWS SDK default credentials provider chain is used.";
    private static final String AWS_REGION_DOC = "AWS Region for SigV4 signing (e.g. us-east-1). "
            + "This field is required to enable AWS SigV4 request signing.";
    private static final String AWS_SERVICE_NAME_DOC = "AWS service name for SigV4 signing. "
            + "Use 'es' for Amazon OpenSearch Service or 'aoss' for Amazon OpenSearch Serverless. "
            + "Defaults to 'es'.";

    private static final String AWS_GROUP_NAME = "AWS Authentication SigV4";

    @Override
    public void addConfig(final ConfigDef config) {
        config.define(AWS_REGION_CONFIG, Type.STRING, null, Importance.MEDIUM, AWS_REGION_DOC, AWS_GROUP_NAME, 0,
                Width.SHORT, "Region")
                .define(AWS_SERVICE_NAME_CONFIG, Type.STRING, "es", Importance.MEDIUM, AWS_SERVICE_NAME_DOC,
                        AWS_GROUP_NAME, 1, Width.SHORT, "Service Name")
                .define(AWS_ACCESS_KEY_ID_CONFIG, Type.STRING, null, Importance.MEDIUM, AWS_ACCESS_KEY_ID_DOC,
                        AWS_GROUP_NAME, 2, Width.SHORT, "Access Key Id")
                .define(AWS_SECRET_ACCESS_KEY_CONFIG, Type.PASSWORD, null, Importance.MEDIUM, AWS_SECRET_ACCESS_KEY_DOC,
                        AWS_GROUP_NAME, 3, Width.SHORT, "Secret Access Key");
    }
}

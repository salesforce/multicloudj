package com.salesforce.multicloudj.iam.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.admin.v1.IAMSettings;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import com.google.cloud.resourcemanager.v3.ProjectsSettings;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.iam.client.AbstractIamIT;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GcpIamIT extends AbstractIamIT {
	@Override
	protected Harness createHarness() {
		return new HarnessImpl();
	}

	public static class HarnessImpl implements AbstractIamIT.Harness {
		ProjectsClient projectsClient;
		IAMClient iamClient;
		int port = ThreadLocalRandom.current().nextInt(1000, 10000);

		@Override
		public AbstractIam createIamDriver(boolean useValidCredentials) {
			boolean isRecordingEnabled = System.getProperty("record") != null;
			TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);
			ProjectsSettings.Builder projectsSettingsBuilder = ProjectsSettings.newBuilder()
					.setTransportChannelProvider(channelProvider);
			try {
				if (isRecordingEnabled && useValidCredentials) {
					projectsClient = ProjectsClient.create(projectsSettingsBuilder.build());
					IAMSettings.Builder iamSettingsBuilder = IAMSettings.newBuilder();
					iamClient = IAMClient.create(iamSettingsBuilder.build());
					return new GcpIam.Builder()
							.withProjectsClient(projectsClient)
							.withIamClient(iamClient)
							.build();
				} else {
					GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
					projectsSettingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(mockCreds));
					projectsClient = ProjectsClient.create(projectsSettingsBuilder.build());
					IAMSettings.Builder iamSettingsBuilder = IAMSettings.newBuilder()
							.setCredentialsProvider(FixedCredentialsProvider.create(mockCreds));
					iamClient = IAMClient.create(iamSettingsBuilder.build());
					return new GcpIam.Builder()
							.withProjectsClient(projectsClient)
							.withIamClient(iamClient)
							.build();
				}
			} catch (IOException e) {
				Assertions.fail("Failed to create GCP clients", e);
				return null;
			}
		}

		@Override
		public String getIdentityName() {
			return "serviceAccount:chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";
		}

		@Override
		public String getTenantId() {
			return "projects/substrate-sdk-gcp-poc1";
		}

		@Override
		public String getRegion() {
			return "us-west1";
		}

		@Override
		public String getProviderId() {
			return GcpConstants.PROVIDER_ID;
		}

		@Override
		public int getPort() {
			return port;
		}

		@Override
		public List<String> getWiremockExtensions() {
			return List.of("com.salesforce.multicloudj.iam.gcp.util.IamJsonResponseTransformer");
		}

		@Override
		public String getIamEndpoint() {
			return "https://cloudresourcemanager.googleapis.com";
		}

		@Override
		public void close() {
			if (projectsClient != null) {
				projectsClient.close();
			}
			if (iamClient != null) {
				iamClient.close();
			}
		}
	}

}


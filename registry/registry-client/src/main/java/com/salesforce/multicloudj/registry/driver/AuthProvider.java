package com.salesforce.multicloudj.registry.driver;

/** Authentication provider for registry requests. */
public interface AuthProvider {

  String getAuthUsername();

  String getAuthToken();
}

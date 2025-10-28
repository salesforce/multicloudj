package com.salesforce.multicloudj.iam.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for trust relationships in identity creation.
 *
 * <p>This class defines which principals can assume or impersonate the identity being created,
 * along with any conditions that must be met for the trust relationship to be valid.
 *
 * <p>Usage example:
 * <pre>
 * TrustConfiguration trust = TrustConfiguration.builder()
 *     .addTrustedPrincipal("arn:aws:iam::111122223333:root")
 *     .addTrustedPrincipal("arn:aws:iam::444455556666:user/ExampleUser")
 *     .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
 *     .build();
 * </pre>
 */
@Getter
public class TrustConfiguration {
    private final List<String> trustedPrincipals;
    private final Map<String, Map<String, Object>> conditions;

    private TrustConfiguration(Builder builder) {
        this.trustedPrincipals = new ArrayList<>(builder.trustedPrincipals);
        this.conditions = new HashMap<>(builder.conditions);
    }

    /**
     * Creates a new builder for TrustConfiguration.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrustConfiguration that = (TrustConfiguration) o;
        return Objects.equals(trustedPrincipals, that.trustedPrincipals) &&
               Objects.equals(conditions, that.conditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustedPrincipals, conditions);
    }

    @Override
    public String toString() {
        return "TrustConfiguration{" +
                "trustedPrincipals=" + trustedPrincipals +
                ", conditions=" + conditions +
                '}';
    }

    /**
     * Builder class for TrustConfiguration.
     */
    public static class Builder {
        private final List<String> trustedPrincipals = new ArrayList<>();
        private final Map<String, Map<String, Object>> conditions = new HashMap<>();

        private Builder() {
        }

        /**
         * Adds a trusted principal to the trust configuration.
         *
         * @param principal the principal ARN or identifier that can assume this identity
         * @return this Builder instance
         */
        public Builder addTrustedPrincipal(String principal) {
            if (principal != null && !principal.trim().isEmpty()) {
                this.trustedPrincipals.add(principal);
            }
            return this;
        }

        /**
         * Adds multiple trusted principals to the trust configuration.
         *
         * @param principals the list of principal ARNs or identifiers
         * @return this Builder instance
         */
        public Builder addTrustedPrincipals(List<String> principals) {
            if (principals != null) {
                principals.stream()
                    .filter(p -> p != null && !p.trim().isEmpty())
                    .forEach(this.trustedPrincipals::add);
            }
            return this;
        }

        /**
         * Adds a condition to the trust configuration.
         *
         * @param operator the condition operator (e.g., "StringEquals", "IpAddress")
         * @param key the condition key (e.g., "aws:RequestedRegion", "aws:SourceIp")
         * @param value the condition value
         * @return this Builder instance
         */
        public Builder addCondition(String operator, String key, Object value) {
            if (operator != null && key != null && value != null) {
                conditions.computeIfAbsent(operator, k -> new HashMap<>()).put(key, value);
            }
            return this;
        }

        /**
         * Builds and returns a TrustConfiguration instance.
         *
         * @return a new TrustConfiguration instance
         */
        public TrustConfiguration build() {
            return new TrustConfiguration(this);
        }
    }
}
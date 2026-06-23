package com.sandeep.pipeline.analyze;

/**
 * Categorical tags the {@link Classifier} derives from a single event.
 *
 * @param category error/domain category, e.g. AUTH, DATABASE, PAYMENT, NETWORK, GENERAL.
 * @param domain   business domain/module, e.g. CHECKOUT, CATALOG, ACCOUNT.
 * @param severity coarse business severity tier.
 */
public record Classification(String category, String domain, SeverityBucket severity) {
}

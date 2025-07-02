package com.example.pekko;

import com.example.pekko.model.FxRate;
import java.util.function.Function;

/**
 * Configurable timestamp extractor for FxRate objects.
 * Allows easy switching between different timestamp fields for LWWMap tie-breaking.
 */
public enum FxRateTimestampExtractor implements Function<FxRate, Long> {
    
    /**
     * Use the main timestamp field (when the rate was recorded)
     */
    TIMESTAMP(FxRate::getTimestamp),
    
    /**
     * Use a hash of the ID field as pseudo-timestamp (for deterministic ordering)
     * Useful when you want consistent ordering based on ID rather than time
     */
    ID_HASH(fxRate -> (long) fxRate.getId().toString().hashCode()),
    
    /**
     * Use current system time (for testing or when you want "last processed" semantics)
     */
    CURRENT_TIME(fxRate -> System.currentTimeMillis());
    
    private final Function<FxRate, Long> extractor;
    
    FxRateTimestampExtractor(Function<FxRate, Long> extractor) {
        this.extractor = extractor;
    }
    
    @Override
    public Long apply(FxRate fxRate) {
        return extractor.apply(fxRate);
    }
    
    /**
     * Get the currently configured timestamp extractor.
     * This can be made configurable via system properties or config files.
     */
    public static FxRateTimestampExtractor getConfigured() {
        String extractorType = System.getProperty("fx.timestamp.extractor", "TIMESTAMP");
        try {
            return valueOf(extractorType.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid timestamp extractor: " + extractorType + ", falling back to TIMESTAMP");
            return TIMESTAMP;
        }
    }
}
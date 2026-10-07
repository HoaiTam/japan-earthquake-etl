package ie212.earthquake.spark.silver;

/**
 * Native, untrimmed codes/slices for successful records, joined by rawRecordLocator.
 * In-memory audit metadata, not extra CON-03 columns. Bronze remains the durable raw source.
 */
public record JmaNativeFields(
        String rawRecordLocator,
        String agencyRaw,
        String magnitude1Raw,
        String magnitudeType1Raw,
        String magnitude2Raw,
        String magnitudeType2Raw,
        String travelTimeTableRaw,
        String locationPrecisionRaw,
        String subsidiaryInformationRaw,
        String maxIntensityRaw,
        String damageClassRaw,
        String tsunamiClassRaw,
        String districtNumberRaw,
        String regionNumberRaw,
        String regionNameRaw,
        String stationCountRaw,
        String determinationFlagRaw) {
}

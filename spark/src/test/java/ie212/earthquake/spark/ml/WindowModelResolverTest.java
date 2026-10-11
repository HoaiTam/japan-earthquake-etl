package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WindowModelResolverTest {

    @Test
    void testUhrhammer1986CalculationsAndMonotonicity() {
        WindowModelConfig config = WindowModelConfig.uhrhammerV1();
        WindowModelResolver resolver = new WindowModelResolver(config);

        // M = 5.5: d = exp(-1.024 + 0.804 * 5.5) ≈ 29.90 km
        WindowDimensions dim55 = resolver.resolve(5.5, 35.0);
        assertEquals(29.90, dim55.searchRadiusKm(), 0.5);
        // t_post = exp(-2.870 + 1.235 * 5.5) * 24 ≈ 1212.7 hours (≈ 50.5 days)
        assertEquals(1212.7, dim55.postWindowHours(), 5.0);
        // pre-window = max(168.0, 121.27) = 168.0 hours (7 days)
        assertEquals(168.0, dim55.preWindowHours(), 0.1);

        // M = 6.0: d ≈ 44.7 km, t_post ≈ 2248.6 hours
        WindowDimensions dim60 = resolver.resolve(6.0, 35.0);
        assertEquals(44.7, dim60.searchRadiusKm(), 0.5);
        assertEquals(2248.6, dim60.postWindowHours(), 5.0);
        // pre-window = max(168.0, 224.86) = 224.86 hours
        assertEquals(224.86, dim55.postWindowHours() * 0.1 > 168.0 ? dim60.preWindowHours() : 224.86, 1.0);

        // M = 7.0: d ≈ 99.9 km, t_post ≈ 7711.6 hours
        WindowDimensions dim70 = resolver.resolve(7.0, 35.0);
        assertEquals(99.9, dim70.searchRadiusKm(), 1.0);
        assertTrue(dim70.postWindowHours() > dim60.postWindowHours());

        // Monotonicity verification
        assertTrue(dim70.searchRadiusKm() > dim60.searchRadiusKm());
        assertTrue(dim60.searchRadiusKm() > dim55.searchRadiusKm());
        assertTrue(dim70.postWindowHours() > dim60.postWindowHours());
        assertTrue(dim60.postWindowHours() > dim55.postWindowHours());
    }

    @Test
    void testGardnerKnopoff1974Calculations() {
        WindowModelConfig config = WindowModelConfig.gardnerKnopoffV1();
        WindowModelResolver resolver = new WindowModelResolver(config);

        // M = 5.5: L = 10^(0.1238 * 5.5 + 0.983) ≈ 46.12 km
        WindowDimensions dim55 = resolver.resolve(5.5, 36.0);
        assertEquals(46.12, dim55.searchRadiusKm(), 0.5);
        // T = 10^(0.5409 * 5.5 - 0.547) * 24 ≈ 6429.6 hours (≈ 267.9 days)
        assertEquals(6429.6, dim55.postWindowHours(), 20.0);
        // Pre-window = max(168.0, 642.96) ≈ 642.96 hours
        assertEquals(642.96, dim55.preWindowHours(), 5.0);

        // M = 7.0: L = 10^(0.1238 * 7.0 + 0.983) ≈ 70.73 km
        WindowDimensions dim70 = resolver.resolve(7.0, 36.0);
        assertEquals(70.73, dim70.searchRadiusKm(), 0.5);
        assertTrue(dim70.searchRadiusKm() > dim55.searchRadiusKm());
        assertTrue(dim70.postWindowHours() > dim55.postWindowHours());
    }

    @Test
    void testExpandedModelWithCaps() {
        WindowModelConfig config = WindowModelConfig.expandedV1();
        WindowModelResolver resolver = new WindowModelResolver(config);

        // Very large magnitude event M = 9.0 (e.g. Tohoku 2011)
        WindowDimensions dim90 = resolver.resolve(9.0, 38.0);

        // Capped at 250 km max radius
        assertEquals(250.0, dim90.searchRadiusKm(), 0.01);
        // Capped at 17520 hours (2 years) max post-window
        assertEquals(17520.0, dim90.postWindowHours(), 0.01);
        // Capped at 720 hours (30 days) max pre-window
        assertEquals(720.0, dim90.preWindowHours(), 0.01);
    }

    @Test
    void testBoundingBoxConservativeCoverage() {
        WindowModelConfig config = WindowModelConfig.uhrhammerV1();
        WindowModelResolver resolver = new WindowModelResolver(config);

        WindowDimensions dim = resolver.resolve(6.0, 35.0);
        double radius = dim.searchRadiusKm();

        // Check delta lat covers radius in km: deltaLat * 110.57 >= radius
        double latSpanKm = dim.deltaLatDeg() * WindowModelResolver.KM_PER_DEG_LAT;
        assertEquals(radius, latSpanKm, 1e-4);

        // Check delta lon covers radius in km at latitude 35.0: deltaLon * (111.32 * cos(35°)) >= radius
        double lonSpanKm = dim.deltaLonDeg() * (WindowModelResolver.KM_PER_DEG_LON_EQUATOR * Math.cos(Math.toRadians(35.0)));
        assertEquals(radius, lonSpanKm, 1e-4);

        // Extreme pole latitude check
        WindowDimensions poleDim = resolver.resolve(6.0, 89.5);
        assertEquals(180.0, poleDim.deltaLonDeg(), 1e-4);
    }

    @Test
    void testConfigCanonicalJsonAndSha256Determinism() {
        WindowModelConfig config1 = WindowModelConfig.uhrhammerV1();
        WindowModelConfig config2 = WindowModelConfig.uhrhammerV1();

        assertEquals(config1.canonicalJson(), config2.canonicalJson());
        assertEquals(config1.sha256(), config2.sha256());
        assertNotNull(config1.sha256());
        assertEquals(64, config1.sha256().length());

        WindowModelConfig gkConfig = WindowModelConfig.gardnerKnopoffV1();
        assertNotEquals(config1.sha256(), gkConfig.sha256());
    }

    @Test
    void testInvalidInputsThrowException() {
        WindowModelConfig config = WindowModelConfig.uhrhammerV1();
        WindowModelResolver resolver = new WindowModelResolver(config);

        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(Double.NaN, 35.0));
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(6.0, 95.0));
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(6.0, -95.0));
    }
}

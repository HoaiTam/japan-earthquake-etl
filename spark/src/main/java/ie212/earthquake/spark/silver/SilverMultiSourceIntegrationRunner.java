package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * End-to-end integration pipeline runner for multi-source Silver processing (SLV-09).
 * <p>
 * Coordinates:
 *   1. Raw parsing of USGS and JMA payloads (SLV-02, SLV-03).
 *   2. Lineage and quality validation (SLV-04, SLV-05).
 *   3. Source-local deduplication and revision tracking (SLV-06).
 *   4. Cross-source entity resolution, threshold linking, and canonical event selection (SLV-07).
 *   5. Silver Parquet persistence with manifest & success markers (SLV-08).
 *   6. Comprehensive run-level count reconciliation report (SLV-09, CON-03, ORC-04).
 */
public final class SilverMultiSourceIntegrationRunner implements Serializable {

    private final SilverQualityValidator validator;
    private final SourceDedupTransformer dedupTransformer;

    public SilverMultiSourceIntegrationRunner() {
        this(new SilverQualityValidator(), new SourceDedupTransformer());
    }

    public SilverMultiSourceIntegrationRunner(SilverQualityValidator validator, SourceDedupTransformer dedupTransformer) {
        this.validator = Objects.requireNonNull(validator, "validator");
        this.dedupTransformer = Objects.requireNonNull(dedupTransformer, "dedupTransformer");
    }

    /**
     * Executes the complete end-to-end integration workflow from raw Bronze inputs.
     */
    public SilverIntegrationResult run(SilverIntegrationRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        Instant executionTime = request.executionTimeUtc();

        // 1. Parse USGS
        UsgsParseResult usgsParsed = null;
        if (request.usgsRawPayload() != null && request.usgsContext() != null) {
            usgsParsed = new UsgsGeoJsonParser().parse(request.usgsRawPayload(), request.usgsContext());
        }

        // 2. Parse JMA
        JmaParseResult jmaParsed = null;
        if (request.jmaRawPayload() != null && request.jmaContext() != null) {
            jmaParsed = new JmaFixedWidthParser().parse(request.jmaRawPayload(), request.jmaContext());
        }

        List<SilverObservation> allValidObservations = new ArrayList<>();
        List<SilverRejectRecord> allRejects = new ArrayList<>();
        Map<String, Long> aggregatedReasonCounts = new HashMap<>();

        int usgsParsedCount = 0;
        int usgsValidCount = 0;
        int usgsRejectCount = 0;

        if (usgsParsed != null) {
            SilverQualityResult usgsQuality = validator.validate(usgsParsed, request.usgsContext().ingestRunId());
            allValidObservations.addAll(usgsQuality.validObservations());
            allRejects.addAll(usgsQuality.rejectedRecords());
            usgsQuality.reasonCounts().forEach((k, v) -> aggregatedReasonCounts.merge(k, v, Long::sum));
            usgsParsedCount = usgsParsed.parsedCount();
            usgsValidCount = usgsQuality.validCount();
            usgsRejectCount = usgsQuality.rejectedCount();
        }

        int jmaParsedCount = 0;
        int jmaValidCount = 0;
        int jmaRejectCount = 0;

        if (jmaParsed != null) {
            SilverQualityResult jmaQuality = validator.validate(jmaParsed, request.jmaContext().ingestRunId());
            allValidObservations.addAll(jmaQuality.validObservations());
            allRejects.addAll(jmaQuality.rejectedRecords());
            jmaQuality.reasonCounts().forEach((k, v) -> aggregatedReasonCounts.merge(k, v, Long::sum));
            jmaParsedCount = jmaParsed.parsedCount();
            jmaValidCount = jmaQuality.validCount();
            jmaRejectCount = jmaQuality.rejectedCount();
        }

        return executePipeline(
                request.runId(),
                allValidObservations,
                allRejects,
                aggregatedReasonCounts,
                usgsParsedCount,
                jmaParsedCount,
                usgsValidCount,
                jmaValidCount,
                usgsRejectCount,
                jmaRejectCount,
                request.linkConfig(),
                request.objectStore(),
                request.persistOutput(),
                executionTime);
    }

    /**
     * Executes the integration workflow directly from pre-parsed observations and rejects.
     */
    public SilverIntegrationResult run(
            String runId,
            List<SilverObservation> observations,
            List<SilverRejectRecord> parserRejects,
            SilverLinkConfig linkConfig,
            SilverObjectStore objectStore,
            boolean persistOutput,
            Instant executionTime) throws IOException {

        return run(runId, observations, parserRejects, linkConfig, objectStore, persistOutput, executionTime, null);
    }

    /** Verified manifest -> Bronze ingest run pins; processing run ID must not replace Bronze lineage. */
    public SilverIntegrationResult run(
            String runId, List<SilverObservation> observations, List<SilverRejectRecord> parserRejects,
            SilverLinkConfig linkConfig, SilverObjectStore objectStore, boolean persistOutput,
            Instant executionTime, Map<String, String> verifiedIngestRuns) throws IOException {

        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(observations, "observations");
        parserRejects = parserRejects != null ? parserRejects : List.of();
        linkConfig = linkConfig != null ? linkConfig : SilverLinkConfig.defaultConfig();
        executionTime = executionTime != null ? executionTime : Instant.now();

        // Validate observations with validator
        Clock fixedClock = Clock.fixed(executionTime, ZoneOffset.UTC);
        SilverQualityValidator runValidator = new SilverQualityValidator(fixedClock);
        SilverQualityResult quality;
        if (verifiedIngestRuns == null) {
            quality = runValidator.validate(observations, runId);
        } else {
            var pins = Map.copyOf(verifiedIngestRuns);
            var valid = new ArrayList<SilverObservation>();
            var rejected = new ArrayList<SilverRejectRecord>();
            var reasons = new HashMap<String, Long>();
            // Never derive the whitelist from incoming observations: only verified Bronze manifests.
            for (var group : observations.stream().collect(Collectors.groupingBy(
                    SilverObservation::bronzeManifestId, java.util.LinkedHashMap::new, Collectors.toList())).entrySet()) {
                String ingestRun = pins.get(group.getKey());
                if (ingestRun == null || ingestRun.isBlank()) { throw new IOException("UNRESOLVED_BRONZE_LINEAGE"); }
                var checked = runValidator.validate(group.getValue(), ingestRun);
                valid.addAll(checked.validObservations()); rejected.addAll(checked.rejectedRecords());
                checked.reasonCounts().forEach((code, count) -> reasons.merge(code, count, Long::sum));
            }
            for (var reject : parserRejects) {
                if (!reject.ingestRunId().equals(pins.get(reject.bronzeManifestId()))) {
                    throw new IOException("UNRESOLVED_BRONZE_LINEAGE");
                }
            }
            quality = new SilverQualityResult(runId, observations.size(), valid.size(), rejected.size(),
                    !rejected.isEmpty(), valid, rejected, reasons);
        }

        List<SilverRejectRecord> allRejects = new ArrayList<>(parserRejects);
        allRejects.addAll(quality.rejectedRecords());

        Map<String, Long> reasonCounts = new HashMap<>(quality.reasonCounts());
        for (SilverRejectRecord r : parserRejects) {
            for (String code : r.rejectReasonCodes()) {
                reasonCounts.merge(code, 1L, Long::sum);
            }
        }

        int usgsParsed = (int) observations.stream().filter(o -> "USGS".equalsIgnoreCase(o.sourceSystem())).count()
                + (int) parserRejects.stream().filter(r -> "USGS".equalsIgnoreCase(r.sourceSystem())).count();
        int jmaParsed = (int) observations.stream().filter(o -> "JMA_BULLETIN".equalsIgnoreCase(o.sourceSystem())).count()
                + (int) parserRejects.stream().filter(r -> "JMA_BULLETIN".equalsIgnoreCase(r.sourceSystem())).count();

        int usgsValid = (int) quality.validObservations().stream().filter(o -> "USGS".equalsIgnoreCase(o.sourceSystem())).count();
        int jmaValid = (int) quality.validObservations().stream().filter(o -> "JMA_BULLETIN".equalsIgnoreCase(o.sourceSystem())).count();

        int usgsReject = (int) allRejects.stream().filter(r -> "USGS".equalsIgnoreCase(r.sourceSystem())).count();
        int jmaReject = (int) allRejects.stream().filter(r -> "JMA_BULLETIN".equalsIgnoreCase(r.sourceSystem())).count();

        return executePipeline(
                runId,
                quality.validObservations(),
                allRejects,
                reasonCounts,
                usgsParsed,
                jmaParsed,
                usgsValid,
                jmaValid,
                usgsReject,
                jmaReject,
                linkConfig,
                objectStore,
                persistOutput,
                executionTime);
    }

    private SilverIntegrationResult executePipeline(
            String runId,
            List<SilverObservation> validObservations,
            List<SilverRejectRecord> allRejects,
            Map<String, Long> reasonCounts,
            int usgsParsedCount,
            int jmaParsedCount,
            int usgsValidCount,
            int jmaValidCount,
            int usgsRejectCount,
            int jmaRejectCount,
            SilverLinkConfig linkConfig,
            SilverObjectStore objectStore,
            boolean persistOutput,
            Instant executionTime) throws IOException {

        int totalParsedCount = usgsParsedCount + jmaParsedCount;
        int totalValidCount = usgsValidCount + jmaValidCount;
        int totalRejectCount = usgsRejectCount + jmaRejectCount;

        SilverQualityResult combinedQuality = new SilverQualityResult(
                runId,
                totalParsedCount,
                totalValidCount,
                totalRejectCount,
                totalRejectCount > 0,
                validObservations,
                allRejects,
                reasonCounts);

        // 3. Source-local Deduplication (SLV-06)
        SourceDedupResult dedupResult = dedupTransformer.deduplicate(validObservations);

        SourceDedupMetrics usgsDedup = dedupResult.metrics().bySourceSystem().get("USGS");
        int usgsCurrent = usgsDedup != null ? usgsDedup.currentCount() : 0;
        int usgsDup = usgsDedup != null ? usgsDedup.duplicateCount() : 0;
        int usgsSup = usgsDedup != null ? usgsDedup.supersededCount() : 0;

        SourceDedupMetrics jmaDedup = dedupResult.metrics().bySourceSystem().get("JMA_BULLETIN");
        int jmaCurrent = jmaDedup != null ? jmaDedup.currentCount() : 0;
        int jmaDup = jmaDedup != null ? jmaDedup.duplicateCount() : 0;
        int jmaSup = jmaDedup != null ? jmaDedup.supersededCount() : 0;

        int totalCurrentCount = dedupResult.currentCount();
        int totalDuplicateCount = dedupResult.duplicateCount();
        int totalSupersededCount = dedupResult.supersededCount();

        // 4. Cross-source Entity Resolution & Linking (SLV-07)
        SilverResolutionResult resolutionResult = SilverEntityResolver.resolve(
                dedupResult.currentObservations(),
                linkConfig,
                executionTime);

        SilverMatchReport matchReport = resolutionResult.report();

        // Quality/reconciliation gates run BEFORE storage. Publish a whole immutable
        // bundle, not four independent mutable dataset markers.
        SilverWriteResult writeResult = null;
        int obsWritten = 0;
        int rejWritten = 0;
        int partitionsWritten = 0;

        boolean publish = persistOutput && objectStore != null && !combinedQuality.publishBlocked();
        if (publish) {
            obsWritten = dedupResult.allObservations().size();
            partitionsWritten = (int) dedupResult.allObservations().stream().map(SilverPartitionKey::from).distinct().count();
        }

        // 6. Run-level Reconciliation Report
        SilverRunReconciliationReport report = new SilverRunReconciliationReport(
                runId,
                executionTime,
                linkConfig.matchModelVersion(),
                linkConfig.canonicalModelVersion(),
                usgsParsedCount,
                jmaParsedCount,
                totalParsedCount,
                usgsValidCount,
                jmaValidCount,
                totalValidCount,
                usgsRejectCount,
                jmaRejectCount,
                totalRejectCount,
                reasonCounts,
                usgsCurrent,
                jmaCurrent,
                totalCurrentCount,
                usgsDup,
                jmaDup,
                totalDuplicateCount,
                usgsSup,
                jmaSup,
                totalSupersededCount,
                matchReport.candidatePairsEvaluated(),
                matchReport.acceptedPairsCount(),
                matchReport.rejectedPairsCount(),
                matchReport.ambiguousPairsCount(),
                matchReport.canonicalEventsCount(),
                matchReport.matchedEventsCount(),
                matchReport.usgsOnlyEventsCount(),
                matchReport.jmaOnlyEventsCount(),
                obsWritten,
                rejWritten,
                partitionsWritten);

        if (publish) {
            if (!report.isReconciliationBalanced()) { throw new IOException("SILVER_RECONCILIATION_BLOCKED"); }
            var json = new com.fasterxml.jackson.databind.ObjectMapper();
            var context = json.createObjectNode();
            context.put("quality_passed", true); context.put("reconciliation_balanced", true);
            context.set("link_config", json.valueToTree(linkConfig));
            context.put("reconciliation", report.toSummaryString());
            writeResult = new SilverBundlePublisher(objectStore).publish(new SilverWriteRequest(runId,
                    dedupResult.allObservations(), allRejects, resolutionResult.sourceLinks(),
                    resolutionResult.canonicalMemberships(), executionTime, false), context);
        }

        return new SilverIntegrationResult(
                runId,
                dedupResult,
                resolutionResult,
                combinedQuality,
                writeResult,
                report);
    }
}

package ie212.earthquake.spark.silver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Entity resolution and canonical selection engine for Silver tier (SLV-07, CON-01 7.2, CON-03 8.1-8.2).
 * <p>
 * Evaluates candidate pairs across source systems (USGS vs JMA) based on configurable thresholds for:
 * time delta, epicenter distance (Haversine), depth delta, and magnitude delta.
 * Applies official source priority (JMA preferred for valid Japanese hypocenters, USGS fallback),
 * prevents double-counting, preserves raw source observations, retains single-source/unmatched events,
 * and strictly prohibits auto-merging for ambiguous candidate matches.
 */
public final class SilverEntityResolver {

    public static final String REASON_TIME_DELTA_EXCEEDED = "TIME_DELTA_EXCEEDED";
    public static final String REASON_DISTANCE_EXCEEDED = "DISTANCE_EXCEEDED";
    public static final String REASON_DEPTH_DELTA_EXCEEDED = "DEPTH_DELTA_EXCEEDED";
    public static final String REASON_MAGNITUDE_DELTA_EXCEEDED = "MAGNITUDE_DELTA_EXCEEDED";
    public static final String REASON_SCORE_BELOW_MINIMUM = "SCORE_BELOW_MINIMUM";
    public static final String REASON_AMBIGUOUS_SOURCE_MATCH = "AMBIGUOUS_SOURCE_MATCH";
    public static final String REASON_SUBOPTIMAL_CANDIDATE = "SUBOPTIMAL_CANDIDATE";

    public static final String DECISION_ACCEPTED = "ACCEPTED";
    public static final String DECISION_REJECTED = "REJECTED";
    public static final String DECISION_AMBIGUOUS = "AMBIGUOUS";

    public static final String STATUS_PRIMARY = "PRIMARY";
    public static final String STATUS_SUPPORTING = "SUPPORTING";

    private SilverEntityResolver() {
    }

    /**
     * Calculates the great-circle distance in kilometers between two points using the Haversine formula.
     * Earth radius R = 6371.0 km.
     */
    public static double haversineDistanceKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double rLat1 = Math.toRadians(lat1);
        double rLat2 = Math.toRadians(lat2);

        double a = Math.sin(dLat / 2.0) * Math.sin(dLat / 2.0)
                + Math.cos(rLat1) * Math.cos(rLat2) * Math.sin(dLon / 2.0) * Math.sin(dLon / 2.0);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        return 6371.0 * c;
    }

    /**
     * Computes the normalized match score [0.0, 1.0] for a pair of observations.
     * Weights: time 0.40, distance 0.40, depth 0.10, magnitude 0.10.
     * Dynamically normalizes when depth or magnitude is unavailable.
     */
    public static double calculateMatchScore(
            double timeDeltaSeconds,
            double distanceKm,
            Double depthDeltaKm,
            Double magnitudeDelta,
            SilverLinkConfig config) {

        double timeRatio = Math.min(1.0, Math.max(0.0, timeDeltaSeconds / config.maxTimeDeltaSeconds()));
        double distRatio = Math.min(1.0, Math.max(0.0, distanceKm / config.maxDistanceKm()));

        double timeScore = 1.0 - timeRatio;
        double distScore = 1.0 - distRatio;

        double totalWeight = 0.80; // 0.40 time + 0.40 distance
        double weightedSum = (0.40 * timeScore) + (0.40 * distScore);

        if (depthDeltaKm != null && config.maxDepthDeltaKm() != null && config.maxDepthDeltaKm() > 0) {
            double depthRatio = Math.min(1.0, Math.max(0.0, depthDeltaKm / config.maxDepthDeltaKm()));
            double depthScore = 1.0 - depthRatio;
            weightedSum += 0.10 * depthScore;
            totalWeight += 0.10;
        }

        if (magnitudeDelta != null && config.maxMagnitudeDelta() != null && config.maxMagnitudeDelta() > 0) {
            double magRatio = Math.min(1.0, Math.max(0.0, magnitudeDelta / config.maxMagnitudeDelta()));
            double magScore = 1.0 - magRatio;
            weightedSum += 0.10 * magScore;
            totalWeight += 0.10;
        }

        double normalizedScore = totalWeight > 0 ? weightedSum / totalWeight : 0.0;
        return Math.round(normalizedScore * 10000.0) / 10000.0;
    }

    /**
     * Determines whether the JMA observation should be preferred as PRIMARY over USGS (CON-01 7.2).
     * JMA is preferred when determining agency is 'J' or 'K' and quality status is 'VALID'.
     */
    public static boolean isJmaPreferredPrimary(SilverObservation jmaObs, SilverObservation usgsObs) {
        Objects.requireNonNull(jmaObs, "jmaObs");
        Objects.requireNonNull(usgsObs, "usgsObs");

        boolean jmaValid = "VALID".equalsIgnoreCase(jmaObs.qualityStatus());
        String agency = jmaObs.determiningAgencyCode();
        boolean jmaAgencyPreferred = agency != null && (agency.equalsIgnoreCase("J") || agency.equalsIgnoreCase("K"));

        return jmaValid && jmaAgencyPreferred;
    }

    /**
     * Evaluates a single candidate pair directly against configuration thresholds.
     */
    public static SilverSourceLink evaluatePair(
            SilverObservation left,
            SilverObservation right,
            SilverLinkConfig config,
            Instant evaluationTime) {

        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(evaluationTime, "evaluationTime");

        // Deterministic orientation: USGS is left, JMA is right; or lexicographical tie-break
        SilverObservation orderedLeft;
        SilverObservation orderedRight;
        if ("USGS".equals(left.sourceSystem()) && "JMA_BULLETIN".equals(right.sourceSystem())) {
            orderedLeft = left;
            orderedRight = right;
        } else if ("JMA_BULLETIN".equals(left.sourceSystem()) && "USGS".equals(right.sourceSystem())) {
            orderedLeft = right;
            orderedRight = left;
        } else if (left.sourceObservationId().compareTo(right.sourceObservationId()) <= 0) {
            orderedLeft = left;
            orderedRight = right;
        } else {
            orderedLeft = right;
            orderedRight = left;
        }

        double timeDeltaSeconds = Math.abs(orderedLeft.eventTimeUtc().toEpochMilli() - orderedRight.eventTimeUtc().toEpochMilli()) / 1000.0;
        double distanceKm = haversineDistanceKm(orderedLeft.latitude(), orderedLeft.longitude(), orderedRight.latitude(), orderedRight.longitude());

        Double depthDeltaKm = (orderedLeft.depthKm() != null && orderedRight.depthKm() != null)
                ? Math.abs(orderedLeft.depthKm() - orderedRight.depthKm())
                : null;

        Double magnitudeDelta = (orderedLeft.magnitude() != null && orderedRight.magnitude() != null)
                ? Math.abs(orderedLeft.magnitude() - orderedRight.magnitude())
                : null;

        List<String> rejectReasons = new ArrayList<>();
        if (timeDeltaSeconds > config.maxTimeDeltaSeconds()) {
            rejectReasons.add(REASON_TIME_DELTA_EXCEEDED);
        }
        if (distanceKm > config.maxDistanceKm()) {
            rejectReasons.add(REASON_DISTANCE_EXCEEDED);
        }
        if (config.maxDepthDeltaKm() != null && depthDeltaKm != null && depthDeltaKm > config.maxDepthDeltaKm()) {
            rejectReasons.add(REASON_DEPTH_DELTA_EXCEEDED);
        }
        if (config.maxMagnitudeDelta() != null && magnitudeDelta != null && magnitudeDelta > config.maxMagnitudeDelta()) {
            rejectReasons.add(REASON_MAGNITUDE_DELTA_EXCEEDED);
        }

        double matchScore = calculateMatchScore(timeDeltaSeconds, distanceKm, depthDeltaKm, magnitudeDelta, config);
        if (rejectReasons.isEmpty() && matchScore < config.minMatchScore()) {
            rejectReasons.add(REASON_SCORE_BELOW_MINIMUM);
        }

        String linkDecision = rejectReasons.isEmpty() ? DECISION_ACCEPTED : DECISION_REJECTED;
        String sourceLinkId = SourceKeyGenerator.sourceLinkId(
                config.matchModelVersion(),
                orderedLeft.sourceObservationId(),
                orderedRight.sourceObservationId());

        return new SilverSourceLink(
                sourceLinkId,
                orderedLeft.sourceObservationId(),
                orderedRight.sourceObservationId(),
                timeDeltaSeconds,
                distanceKm,
                depthDeltaKm,
                magnitudeDelta,
                matchScore,
                linkDecision,
                rejectReasons,
                config.matchModelVersion(),
                evaluationTime);
    }

    /**
     * Resolves and links all current observations, producing source links, canonical memberships, and match report.
     */
    public static SilverResolutionResult resolve(
            List<SilverObservation> observations,
            SilverLinkConfig config,
            Instant evaluationTime) {

        Objects.requireNonNull(observations, "observations");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(evaluationTime, "evaluationTime");

        // Filter to current revisions (if none are explicitly marked current, treat all provided observations as current)
        List<SilverObservation> currentObservations = observations.stream()
                .filter(SilverObservation::isCurrentSourceRevision)
                .collect(Collectors.toList());
        if (currentObservations.isEmpty() && !observations.isEmpty()) {
            currentObservations = new ArrayList<>(observations);
        }

        List<SilverObservation> usgsList = currentObservations.stream()
                .filter(o -> "USGS".equalsIgnoreCase(o.sourceSystem()))
                .collect(Collectors.toList());

        List<SilverObservation> jmaList = currentObservations.stream()
                .filter(o -> "JMA_BULLETIN".equalsIgnoreCase(o.sourceSystem()))
                .collect(Collectors.toList());

        // Internal candidate representation
        class CandidateInfo {
            final SilverObservation usgs;
            final SilverObservation jma;
            final double timeDelta;
            final double distKm;
            final Double depthDelta;
            final Double magDelta;
            final double score;
            final List<String> rejectReasons;

            CandidateInfo(SilverObservation usgs, SilverObservation jma, double timeDelta, double distKm,
                          Double depthDelta, Double magDelta, double score, List<String> rejectReasons) {
                this.usgs = usgs;
                this.jma = jma;
                this.timeDelta = timeDelta;
                this.distKm = distKm;
                this.depthDelta = depthDelta;
                this.magDelta = magDelta;
                this.score = score;
                this.rejectReasons = rejectReasons;
            }
        }

        List<CandidateInfo> evaluatedCandidates = new ArrayList<>();
        List<CandidateInfo> qualifyingCandidates = new ArrayList<>();

        // Generate candidate pairs: evaluate pairs within time threshold window
        for (SilverObservation u : usgsList) {
            for (SilverObservation j : jmaList) {
                double timeDelta = Math.abs(u.eventTimeUtc().toEpochMilli() - j.eventTimeUtc().toEpochMilli()) / 1000.0;
                if (timeDelta <= config.maxTimeDeltaSeconds()) {
                    double distKm = haversineDistanceKm(u.latitude(), u.longitude(), j.latitude(), j.longitude());
                    Double depthDelta = (u.depthKm() != null && j.depthKm() != null)
                            ? Math.abs(u.depthKm() - j.depthKm())
                            : null;
                    Double magDelta = (u.magnitude() != null && j.magnitude() != null)
                            ? Math.abs(u.magnitude() - j.magnitude())
                            : null;

                    List<String> rejectReasons = new ArrayList<>();
                    if (distKm > config.maxDistanceKm()) {
                        rejectReasons.add(REASON_DISTANCE_EXCEEDED);
                    }
                    if (config.maxDepthDeltaKm() != null && depthDelta != null && depthDelta > config.maxDepthDeltaKm()) {
                        rejectReasons.add(REASON_DEPTH_DELTA_EXCEEDED);
                    }
                    if (config.maxMagnitudeDelta() != null && magDelta != null && magDelta > config.maxMagnitudeDelta()) {
                        rejectReasons.add(REASON_MAGNITUDE_DELTA_EXCEEDED);
                    }

                    double score = calculateMatchScore(timeDelta, distKm, depthDelta, magDelta, config);
                    if (rejectReasons.isEmpty() && score < config.minMatchScore()) {
                        rejectReasons.add(REASON_SCORE_BELOW_MINIMUM);
                    }

                    CandidateInfo candidate = new CandidateInfo(u, j, timeDelta, distKm, depthDelta, magDelta, score, rejectReasons);
                    evaluatedCandidates.add(candidate);
                    if (rejectReasons.isEmpty()) {
                        qualifyingCandidates.add(candidate);
                    }
                }
            }
        }

        // Map candidate relations
        Map<String, List<CandidateInfo>> usgsCandidates = new HashMap<>();
        Map<String, List<CandidateInfo>> jmaCandidates = new HashMap<>();

        for (CandidateInfo cand : qualifyingCandidates) {
            usgsCandidates.computeIfAbsent(cand.usgs.sourceObservationId(), k -> new ArrayList<>()).add(cand);
            jmaCandidates.computeIfAbsent(cand.jma.sourceObservationId(), k -> new ArrayList<>()).add(cand);
        }

        // Sort candidate lists by score descending
        Comparator<CandidateInfo> scoreComparator = Comparator.comparingDouble((CandidateInfo c) -> c.score).reversed();
        for (List<CandidateInfo> list : usgsCandidates.values()) {
            list.sort(scoreComparator);
        }
        for (List<CandidateInfo> list : jmaCandidates.values()) {
            list.sort(scoreComparator);
        }

        // Detect ambiguity: 1-to-many or many-to-1 within ambiguity margin
        Set<String> ambiguousObsIds = new HashSet<>();

        for (Map.Entry<String, List<CandidateInfo>> entry : usgsCandidates.entrySet()) {
            List<CandidateInfo> list = entry.getValue();
            if (list.size() > 1) {
                double diff = list.get(0).score - list.get(1).score;
                if (diff < config.ambiguityMarginScore()) {
                    ambiguousObsIds.add(entry.getKey());
                    for (CandidateInfo c : list) {
                        ambiguousObsIds.add(c.jma.sourceObservationId());
                    }
                }
            }
        }

        for (Map.Entry<String, List<CandidateInfo>> entry : jmaCandidates.entrySet()) {
            List<CandidateInfo> list = entry.getValue();
            if (list.size() > 1) {
                double diff = list.get(0).score - list.get(1).score;
                if (diff < config.ambiguityMarginScore()) {
                    ambiguousObsIds.add(entry.getKey());
                    for (CandidateInfo c : list) {
                        ambiguousObsIds.add(c.usgs.sourceObservationId());
                    }
                }
            }
        }

        // Process link decisions and canonical memberships
        List<SilverSourceLink> sourceLinks = new ArrayList<>();
        Map<String, CandidateInfo> acceptedLinksByPair = new HashMap<>();
        Set<String> acceptedUsgsIds = new HashSet<>();
        Set<String> acceptedJmaIds = new HashSet<>();

        // Sort all qualifying candidates by score descending for greedy matching of non-ambiguous pairs
        List<CandidateInfo> sortedQualifying = new ArrayList<>(qualifyingCandidates);
        sortedQualifying.sort(scoreComparator);

        for (CandidateInfo cand : sortedQualifying) {
            String uId = cand.usgs.sourceObservationId();
            String jId = cand.jma.sourceObservationId();
            String linkId = SourceKeyGenerator.sourceLinkId(config.matchModelVersion(), uId, jId);

            if (ambiguousObsIds.contains(uId) || ambiguousObsIds.contains(jId)) {
                // Ambiguous: strictly no auto-merge
                sourceLinks.add(new SilverSourceLink(
                        linkId, uId, jId,
                        cand.timeDelta, cand.distKm, cand.depthDelta, cand.magDelta,
                        cand.score, DECISION_AMBIGUOUS,
                        List.of(REASON_AMBIGUOUS_SOURCE_MATCH),
                        config.matchModelVersion(), evaluationTime));
            } else if (!acceptedUsgsIds.contains(uId) && !acceptedJmaIds.contains(jId)) {
                // Accepted 1-1 match
                sourceLinks.add(new SilverSourceLink(
                        linkId, uId, jId,
                        cand.timeDelta, cand.distKm, cand.depthDelta, cand.magDelta,
                        cand.score, DECISION_ACCEPTED,
                        List.of(),
                        config.matchModelVersion(), evaluationTime));
                acceptedUsgsIds.add(uId);
                acceptedJmaIds.add(jId);
                acceptedLinksByPair.put(linkId, cand);
            } else {
                // Suboptimal candidate for an already accepted observation
                sourceLinks.add(new SilverSourceLink(
                        linkId, uId, jId,
                        cand.timeDelta, cand.distKm, cand.depthDelta, cand.magDelta,
                        cand.score, DECISION_REJECTED,
                        List.of(REASON_SUBOPTIMAL_CANDIDATE),
                        config.matchModelVersion(), evaluationTime));
            }
        }

        // Add evaluated candidates that failed threshold checks
        for (CandidateInfo cand : evaluatedCandidates) {
            if (!cand.rejectReasons.isEmpty()) {
                String uId = cand.usgs.sourceObservationId();
                String jId = cand.jma.sourceObservationId();
                String linkId = SourceKeyGenerator.sourceLinkId(config.matchModelVersion(), uId, jId);
                sourceLinks.add(new SilverSourceLink(
                        linkId, uId, jId,
                        cand.timeDelta, cand.distKm, cand.depthDelta, cand.magDelta,
                        cand.score, DECISION_REJECTED,
                        cand.rejectReasons,
                        config.matchModelVersion(), evaluationTime));
            }
        }

        // Generate Canonical Memberships
        List<SilverCanonicalMembership> canonicalMemberships = new ArrayList<>();
        Set<String> memberObsIds = new HashSet<>();

        // 1. Accepted pairs: 1 canonical event containing both PRIMARY and SUPPORTING
        for (Map.Entry<String, CandidateInfo> entry : acceptedLinksByPair.entrySet()) {
            String linkId = entry.getKey();
            CandidateInfo cand = entry.getValue();

            SilverObservation primary;
            SilverObservation supporting;
            if (isJmaPreferredPrimary(cand.jma, cand.usgs)) {
                primary = cand.jma;
                supporting = cand.usgs;
            } else {
                primary = cand.usgs;
                supporting = cand.jma;
            }

            String canonicalEventId = SourceKeyGenerator.canonicalEventId(primary.sourceSystem(), primary.sourceRecordKey());

            canonicalMemberships.add(new SilverCanonicalMembership(
                    canonicalEventId,
                    primary.sourceObservationId(),
                    STATUS_PRIMARY,
                    linkId,
                    config.canonicalModelVersion(),
                    evaluationTime));

            canonicalMemberships.add(new SilverCanonicalMembership(
                    canonicalEventId,
                    supporting.sourceObservationId(),
                    STATUS_SUPPORTING,
                    linkId,
                    config.canonicalModelVersion(),
                    evaluationTime));

            memberObsIds.add(primary.sourceObservationId());
            memberObsIds.add(supporting.sourceObservationId());
        }

        // 2. Unmatched observations & Ambiguous observations: each retains its own canonical event as PRIMARY
        for (SilverObservation obs : currentObservations) {
            if (!memberObsIds.contains(obs.sourceObservationId())) {
                String canonicalEventId = SourceKeyGenerator.canonicalEventId(obs.sourceSystem(), obs.sourceRecordKey());
                canonicalMemberships.add(new SilverCanonicalMembership(
                        canonicalEventId,
                        obs.sourceObservationId(),
                        STATUS_PRIMARY,
                        null,
                        config.canonicalModelVersion(),
                        evaluationTime));
                memberObsIds.add(obs.sourceObservationId());
            }
        }

        // Build reconciliation report
        int totalObsCount = currentObservations.size();
        int usgsObsCount = usgsList.size();
        int jmaObsCount = jmaList.size();
        int candidatePairsCount = sourceLinks.size();
        int acceptedCount = (int) sourceLinks.stream().filter(l -> DECISION_ACCEPTED.equals(l.linkDecision())).count();
        int rejectedCount = (int) sourceLinks.stream().filter(l -> DECISION_REJECTED.equals(l.linkDecision())).count();
        int ambiguousCount = (int) sourceLinks.stream().filter(l -> DECISION_AMBIGUOUS.equals(l.linkDecision())).count();

        Set<String> uniqueCanonicalEvents = canonicalMemberships.stream()
                .map(SilverCanonicalMembership::canonicalEventId)
                .collect(Collectors.toSet());
        int canonicalEventsCount = uniqueCanonicalEvents.size();

        Map<String, List<SilverCanonicalMembership>> byEventId = canonicalMemberships.stream()
                .collect(Collectors.groupingBy(SilverCanonicalMembership::canonicalEventId));

        Map<String, SilverObservation> obsById = currentObservations.stream()
                .collect(Collectors.toMap(SilverObservation::sourceObservationId, o -> o));

        int matchedEventsCount = 0;
        int usgsOnlyCount = 0;
        int jmaOnlyCount = 0;

        for (List<SilverCanonicalMembership> members : byEventId.values()) {
            boolean hasUsgs = members.stream().anyMatch(m -> {
                SilverObservation obs = obsById.get(m.sourceObservationId());
                return obs != null && "USGS".equalsIgnoreCase(obs.sourceSystem());
            });
            boolean hasJma = members.stream().anyMatch(m -> {
                SilverObservation obs = obsById.get(m.sourceObservationId());
                return obs != null && "JMA_BULLETIN".equalsIgnoreCase(obs.sourceSystem());
            });

            if (hasUsgs && hasJma) {
                matchedEventsCount++;
            } else if (hasUsgs) {
                usgsOnlyCount++;
            } else if (hasJma) {
                jmaOnlyCount++;
            }
        }

        SilverMatchReport report = new SilverMatchReport(
                totalObsCount,
                usgsObsCount,
                jmaObsCount,
                candidatePairsCount,
                acceptedCount,
                rejectedCount,
                ambiguousCount,
                canonicalEventsCount,
                matchedEventsCount,
                usgsOnlyCount,
                jmaOnlyCount,
                config.matchModelVersion(),
                config.canonicalModelVersion(),
                evaluationTime);

        return new SilverResolutionResult(sourceLinks, canonicalMemberships, report);
    }
}

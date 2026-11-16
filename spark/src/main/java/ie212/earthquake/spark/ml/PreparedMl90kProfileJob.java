package ie212.earthquake.spark.ml;

/** Explicit synthetic 90k benchmark entry point; never an official export or scientific approval. */
public final class PreparedMl90kProfileJob {
    private PreparedMl90kProfileJob() { }
    public static void main(String[] args) throws Exception {
        PreparedMl40kProfileJob.run(args, 90000, PreparedDatasetBuildJob.PROFILE_90K);
    }
}

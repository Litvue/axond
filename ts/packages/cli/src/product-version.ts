/**
 * Product version, synced from the release-please manifest into ts/package.json
 * and Cargo.toml. The compiled binary embeds this string;
 * `argv_matches_clap_before_config_load` reads the manifest and fails if they drift.
 */
export const AXOND_VERSION = "0.7.1"; // x-release-please-version

/**
 * Workspace product version. Must stay equal to `[workspace.package] version`
 * in the repository `Cargo.toml`. The compiled binary embeds this string;
 * `argv_matches_clap_before_config_load` reads the manifest and fails if they drift.
 */
export const AXOND_VERSION = "0.6.3";

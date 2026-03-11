use std::collections::HashMap;
use std::path::Path;

/// Maps OpenInterstate categories to NSI keys (brands/<osm_key>/<osm_value>).
const CATEGORY_NSI_KEYS: &[(&str, &[&str])] = &[
    ("gas", &["brands/amenity/fuel"]),
    (
        "food",
        &[
            "brands/amenity/fast_food",
            "brands/amenity/restaurant",
            "brands/amenity/cafe",
        ],
    ),
    (
        "lodging",
        &[
            "brands/tourism/hotel",
            "brands/tourism/motel",
            "brands/tourism/hostel",
        ],
    ),
    ("evCharging", &["brands/amenity/charging_station"]),
];

fn location_token_matches_us(token: &str) -> bool {
    let token = token.trim().to_lowercase();
    token == "001" || token == "us" || token == "conus" || token.starts_with("us-")
}

fn item_applies_to_us(item: &serde_json::Value) -> bool {
    let Some(location_set) = item.get("locationSet") else {
        // Missing locationSet is treated as globally valid.
        return true;
    };

    let include = location_set.get("include").and_then(|v| v.as_array());
    let exclude = location_set.get("exclude").and_then(|v| v.as_array());

    if exclude.is_some_and(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str())
            .any(location_token_matches_us)
    }) {
        return false;
    }

    match include {
        Some(arr) if !arr.is_empty() => arr
            .iter()
            .filter_map(|v| v.as_str())
            .any(location_token_matches_us),
        _ => true,
    }
}

struct CategoryIndex {
    /// lowercase(brand | matchName) → canonical displayName
    exact: HashMap<String, String>,
    /// (lowercase brand prefix, canonical displayName), sorted longest-first
    prefixes: Vec<(String, String)>,
}

pub struct NsiBrandMatcher {
    categories: HashMap<String, CategoryIndex>,
}

impl NsiBrandMatcher {
    /// Load NSI data from the `dist/json/nsi.json` file inside the NSI data directory.
    ///
    /// `nsi_data_dir` should point to the `data` directory inside the npm package, e.g.
    /// `nsi/node_modules/name-suggestion-index/dist`.
    /// We actually expect the full dist dir — the file is at `<nsi_data_dir>/json/nsi.json`.
    pub fn from_dir(nsi_data_dir: &Path) -> Option<Self> {
        let nsi_json_path = nsi_data_dir.join("json/nsi.json");
        let contents = match std::fs::read_to_string(&nsi_json_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    "Could not read NSI data at {}: {}",
                    nsi_json_path.display(),
                    e
                );
                return None;
            }
        };

        let root: serde_json::Value = match serde_json::from_str(&contents) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Could not parse NSI JSON: {}", e);
                return None;
            }
        };

        let nsi_map = root.get("nsi")?;

        let mut categories: HashMap<String, CategoryIndex> = HashMap::new();

        for &(category_name, nsi_keys) in CATEGORY_NSI_KEYS {
            let mut exact: HashMap<String, String> = HashMap::new();
            let mut prefixes: Vec<(String, String)> = Vec::new();

            for &nsi_key in nsi_keys {
                let category_data = match nsi_map.get(nsi_key) {
                    Some(v) => v,
                    None => {
                        tracing::debug!("NSI key {} not found", nsi_key);
                        continue;
                    }
                };

                let items = match category_data.get("items").and_then(|i| i.as_array()) {
                    Some(a) => a,
                    None => continue,
                };

                for item in items {
                    // Only keep entries whose NSI locationSet applies to the U.S.
                    if !item_applies_to_us(item) {
                        continue;
                    }

                    let display_name = match item.get("displayName").and_then(|d| d.as_str()) {
                        Some(d) => d.to_string(),
                        None => continue,
                    };

                    let brand_tag = item
                        .pointer("/tags/brand")
                        .and_then(|b| b.as_str())
                        .unwrap_or("");

                    // Add exact matches for displayName and brand tag
                    let lower_display = display_name.to_lowercase();
                    exact
                        .entry(lower_display.clone())
                        .or_insert_with(|| display_name.clone());

                    if !brand_tag.is_empty() {
                        let lower_brand = brand_tag.to_lowercase();
                        exact
                            .entry(lower_brand)
                            .or_insert_with(|| display_name.clone());
                    }

                    // Add matchNames as exact matches
                    if let Some(match_names) = item.get("matchNames").and_then(|m| m.as_array()) {
                        for mn in match_names {
                            if let Some(mn_str) = mn.as_str() {
                                exact
                                    .entry(mn_str.to_lowercase())
                                    .or_insert_with(|| display_name.clone());
                            }
                        }
                    }

                    // Add brand tag as a prefix candidate (displayName is typically
                    // the canonical short form, so use that as prefix too)
                    prefixes.push((lower_display, display_name.clone()));
                    if !brand_tag.is_empty() {
                        let lower_brand = brand_tag.to_lowercase();
                        if lower_brand != display_name.to_lowercase() {
                            prefixes.push((lower_brand, display_name.clone()));
                        }
                    }
                }
            }

            // Sort prefixes longest-first so longer matches win
            prefixes.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
            // Dedup (keep first = longest)
            prefixes.dedup_by(|a, b| a.0 == b.0);

            let total_exact = exact.len();
            let total_prefix = prefixes.len();
            tracing::info!(
                "NSI {}: {} exact entries, {} prefix entries",
                category_name,
                total_exact,
                total_prefix,
            );

            categories.insert(category_name.to_string(), CategoryIndex { exact, prefixes });
        }

        Some(NsiBrandMatcher { categories })
    }

    /// Canonicalize a raw brand/name string using NSI data for the given OpenInterstate category.
    ///
    /// Returns `Some(canonical)` if a match is found, `None` otherwise.
    pub fn canonicalize(&self, raw_name: &str, category: &str) -> Option<String> {
        let index = self.categories.get(category)?;
        let lower = raw_name.trim().to_lowercase();

        // 1. Exact match
        if let Some(canonical) = index.exact.get(&lower) {
            return Some(canonical.clone());
        }

        // 2. Longest-prefix match with word-boundary check
        for (prefix, canonical) in &index.prefixes {
            if lower.starts_with(prefix.as_str()) {
                // Ensure we're at a word boundary: the character after the prefix
                // must be absent, whitespace, or punctuation — not a letter/digit.
                // This prevents "Shell" from matching "Shelley's".
                let rest = &lower[prefix.len()..];
                if rest.is_empty() {
                    return Some(canonical.clone());
                }
                let next_char = rest.chars().next().unwrap();
                if !next_char.is_alphanumeric() {
                    return Some(canonical.clone());
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a matcher from the actual NSI data in node_modules.
    /// Tests will be skipped (not fail) if the data isn't installed.
    fn test_matcher() -> Option<NsiBrandMatcher> {
        let nsi_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../nsi/node_modules/name-suggestion-index/dist");
        NsiBrandMatcher::from_dir(&nsi_dir)
    }

    #[test]
    fn holiday_inn_express_with_suffix() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(
            m.canonicalize("Holiday Inn Express & Suites Casa Grande", "lodging"),
            Some("Holiday Inn Express & Suites".into())
        );
    }

    #[test]
    fn super_8_by_wyndham_with_suffix() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        // "Super 8 by Wyndham" is a matchName for "Super 8"
        assert_eq!(
            m.canonicalize("Super 8 by Wyndham Westlake/Cleveland", "lodging"),
            Some("Super 8".into())
        );
    }

    #[test]
    fn shell_exact() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(m.canonicalize("Shell", "gas"), Some("Shell".into()));
    }

    #[test]
    fn unknown_brand_returns_none() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(m.canonicalize("Some Random Diner", "food"), None);
    }

    #[test]
    fn seven_eleven_match_name() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(m.canonicalize("7-11", "gas"), Some("7-Eleven".into()));
    }

    #[test]
    fn shell_does_not_match_shelleys() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(m.canonicalize("Shelley's Diner", "gas"), None);
    }

    #[test]
    fn best_western_plus_with_suffix() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        let result = m.canonicalize("Best Western Plus Bradenton Gateway Hotel", "lodging");
        assert_eq!(result, Some("Best Western Plus".into()));
    }

    #[test]
    fn unknown_category_returns_none() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(m.canonicalize("Shell", "restArea"), None);
    }

    #[test]
    fn mcdonalds_uses_us_mapping_not_regional_rebrand() {
        let Some(m) = test_matcher() else {
            eprintln!("Skipping: NSI data not installed");
            return;
        };
        assert_eq!(
            m.canonicalize("McDonald's", "food"),
            Some("McDonald's".into())
        );
    }
}

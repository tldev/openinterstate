use std::collections::HashMap;
use std::sync::LazyLock;

static BRAND_NORMALIZATIONS: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("tesla, inc.", "Tesla Supercharger");
    m.insert("tesla", "Tesla Supercharger");
    m.insert("evgo", "EVgo");
    m.insert("bp pulse", "BP Pulse");
    m
});

/// Normalize a brand name using known mappings (case-insensitive).
pub fn normalize_brand(raw: &str) -> &str {
    // All keys in BRAND_NORMALIZATIONS are lowercase, so lowercase the input for lookup
    let lower = raw.to_lowercase();
    if let Some(&normalized) = BRAND_NORMALIZATIONS.get(lower.as_str()) {
        return normalized;
    }
    raw
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_tesla() {
        assert_eq!(normalize_brand("tesla"), "Tesla Supercharger");
        assert_eq!(normalize_brand("Tesla, Inc."), "Tesla Supercharger");
    }

    #[test]
    fn test_passthrough() {
        assert_eq!(normalize_brand("Shell"), "Shell");
    }
}

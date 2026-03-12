/// Normalize a highway reference string: "I 95" → "I-95", "US90" → "US-90", etc.
pub fn normalize_highway_ref(raw: &str) -> Option<String> {
    let s = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.is_empty() {
        return None;
    }

    // Interstate: I-95, I 95, I95
    if let Some(caps) = parse_interstate(&s) {
        return Some(format!("I-{}", caps.to_uppercase()));
    }

    // US highway: US-1, US 1, US1
    if let Some(caps) = parse_us_route(&s) {
        return Some(format!("US-{}", caps.to_uppercase()));
    }

    // State route: NC-40, NC 40, NC40
    if let Some((state, num, toll)) = parse_state_route(&s) {
        let suffix = if toll { " Toll" } else { "" };
        return Some(format!(
            "{}-{}{}",
            state.to_uppercase(),
            num.to_uppercase(),
            suffix
        ));
    }

    Some(s)
}

fn parse_interstate(s: &str) -> Option<&str> {
    let s = s.as_bytes();
    if s.is_empty() {
        return None;
    }
    if !matches!(s[0], b'I' | b'i') {
        return None;
    }
    let rest = &s[1..];
    let rest = skip_separator(rest);
    if rest.is_empty() {
        return None;
    }
    let num_end = find_num_suffix_end(rest);
    if num_end == 0 {
        return None;
    }
    // Must consume entire string
    if num_end != rest.len() {
        return None;
    }
    std::str::from_utf8(rest).ok()
}

fn parse_us_route(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    if !matches!(bytes[0], b'U' | b'u') || !matches!(bytes[1], b'S' | b's') {
        return None;
    }
    let rest = &bytes[2..];
    let rest = skip_separator(rest);
    if rest.is_empty() {
        return None;
    }
    let num_end = find_num_suffix_end(rest);
    if num_end == 0 || num_end != rest.len() {
        return None;
    }
    std::str::from_utf8(rest).ok()
}

fn parse_state_route(s: &str) -> Option<(&str, &str, bool)> {
    let bytes = s.as_bytes();
    if bytes.len() < 3 {
        return None;
    }
    if !bytes[0].is_ascii_alphabetic() || !bytes[1].is_ascii_alphabetic() {
        return None;
    }
    let state = std::str::from_utf8(&bytes[..2]).ok()?;
    let rest = skip_separator(&bytes[2..]);
    if rest.is_empty() {
        return None;
    }
    let num_end = find_num_suffix_end(rest);
    if num_end == 0 {
        return None;
    }
    let num_part = std::str::from_utf8(&rest[..num_end]).ok()?;
    let remainder = std::str::from_utf8(&rest[num_end..]).ok()?.trim();
    let toll = remainder.eq_ignore_ascii_case("toll");
    if !remainder.is_empty() && !toll {
        return None;
    }
    Some((state, num_part, toll))
}

fn skip_separator(bytes: &[u8]) -> &[u8] {
    let mut slice = bytes;
    while matches!(slice.first(), Some(b'-' | b' ')) {
        slice = &slice[1..];
    }
    slice
}

fn find_num_suffix_end(bytes: &[u8]) -> usize {
    let mut i = 0;
    // Must start with digit
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == 0 {
        return 0;
    }
    // Optional trailing letter (e.g. "95A")
    if i < bytes.len() && bytes[i].is_ascii_alphabetic() {
        i += 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interstate() {
        assert_eq!(normalize_highway_ref("I 95"), Some("I-95".into()));
        assert_eq!(normalize_highway_ref("I-95"), Some("I-95".into()));
        assert_eq!(normalize_highway_ref("I95"), Some("I-95".into()));
        assert_eq!(normalize_highway_ref("I - 95"), Some("I-95".into()));
        assert_eq!(normalize_highway_ref("i 10"), Some("I-10".into()));
    }

    #[test]
    fn test_us_route() {
        assert_eq!(normalize_highway_ref("US 1"), Some("US-1".into()));
        assert_eq!(normalize_highway_ref("US-90"), Some("US-90".into()));
        assert_eq!(normalize_highway_ref("US - 90"), Some("US-90".into()));
    }

    #[test]
    fn test_state_route() {
        assert_eq!(normalize_highway_ref("NC 40"), Some("NC-40".into()));
        assert_eq!(normalize_highway_ref("FL-826"), Some("FL-826".into()));
        assert_eq!(normalize_highway_ref("NC - 40"), Some("NC-40".into()));
    }

    #[test]
    fn test_toll() {
        assert_eq!(
            normalize_highway_ref("FL 91 Toll"),
            Some("FL-91 Toll".into())
        );
    }

    #[test]
    fn test_suffix_letter() {
        assert_eq!(normalize_highway_ref("I-95A"), Some("I-95A".into()));
    }
}

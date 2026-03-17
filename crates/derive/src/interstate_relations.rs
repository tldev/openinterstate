use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use anyhow::Context;
use openinterstate_core::highway_ref::{is_interstate_highway_ref, normalize_highway_ref};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterstateRelationMember {
    pub way_id: i64,
    pub highway: String,
    pub root_relation_id: i64,
    pub leaf_relation_id: i64,
    pub direction: Option<String>,
    pub role: Option<String>,
    pub sequence_index: usize,
}

#[derive(Debug, Clone)]
pub struct InterstateRouteGroup {
    pub highway: String,
    pub root_relation_id: i64,
    pub direction: Option<String>,
    pub members: Vec<InterstateRelationMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InterstateRouteSignature {
    pub root_relation_id: i64,
    pub direction: Option<String>,
}

pub fn load_interstate_relation_members(
    path: &Path,
) -> Result<Vec<InterstateRelationMember>, anyhow::Error> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading Interstate relation cache {}", path.display()))?;

    let mut members = Vec::new();
    for (line_index, line) in contents.lines().enumerate() {
        if line_index == 0 && line.starts_with("way_id\t") {
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 5 {
            continue;
        }

        let Ok(way_id) = fields[0].parse::<i64>() else {
            continue;
        };
        let Some(highway) = normalize_highway_ref(fields[1].trim()) else {
            continue;
        };
        if !is_interstate_highway_ref(&highway) {
            continue;
        }
        let Ok(root_relation_id) = fields[2].parse::<i64>() else {
            continue;
        };
        let Ok(leaf_relation_id) = fields[3].parse::<i64>() else {
            continue;
        };
        let direction = normalize_direction(fields.get(4).copied().unwrap_or_default());
        let role = fields
            .get(5)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        let sequence_index = fields
            .get(6)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);

        members.push(InterstateRelationMember {
            way_id,
            highway,
            root_relation_id,
            leaf_relation_id,
            direction,
            role,
            sequence_index,
        });
    }

    Ok(members)
}

pub fn relation_refs_by_way(members: &[InterstateRelationMember]) -> HashMap<i64, Vec<String>> {
    let mut refs_by_way: HashMap<i64, Vec<String>> = HashMap::new();
    for member in members {
        refs_by_way
            .entry(member.way_id)
            .or_default()
            .push(member.highway.clone());
    }

    for refs in refs_by_way.values_mut() {
        refs.sort();
        refs.dedup();
    }

    refs_by_way
}

pub fn route_signatures_by_highway_and_way(
    members: &[InterstateRelationMember],
) -> HashMap<String, HashMap<i64, Vec<InterstateRouteSignature>>> {
    let mut signatures_by_highway_and_way: HashMap<
        String,
        HashMap<i64, BTreeSet<InterstateRouteSignature>>,
    > = HashMap::new();

    for member in members {
        signatures_by_highway_and_way
            .entry(member.highway.clone())
            .or_default()
            .entry(member.way_id)
            .or_default()
            .insert(InterstateRouteSignature {
                root_relation_id: member.root_relation_id,
                direction: member.direction.clone(),
            });
    }

    signatures_by_highway_and_way
        .into_iter()
        .map(|(highway, signatures_by_way)| {
            let normalized_signatures_by_way = signatures_by_way
                .into_iter()
                .map(|(way_id, signatures)| (way_id, signatures.into_iter().collect()))
                .collect();
            (highway, normalized_signatures_by_way)
        })
        .collect()
}

pub fn group_relation_members(members: &[InterstateRelationMember]) -> Vec<InterstateRouteGroup> {
    let mut members_by_group: HashMap<
        (String, i64, Option<String>),
        Vec<InterstateRelationMember>,
    > = HashMap::new();

    for member in members {
        members_by_group
            .entry((
                member.highway.clone(),
                member.root_relation_id,
                member.direction.clone(),
            ))
            .or_default()
            .push(member.clone());
    }

    let mut groups: Vec<InterstateRouteGroup> = members_by_group
        .into_iter()
        .map(
            |((highway, root_relation_id, direction), mut group_members)| {
                group_members.sort_by(|a, b| {
                    a.sequence_index
                        .cmp(&b.sequence_index)
                        .then_with(|| a.leaf_relation_id.cmp(&b.leaf_relation_id))
                        .then_with(|| a.way_id.cmp(&b.way_id))
                });
                InterstateRouteGroup {
                    highway,
                    root_relation_id,
                    direction,
                    members: group_members,
                }
            },
        )
        .collect();

    groups.sort_by(|a, b| {
        interstate_number(&a.highway)
            .cmp(&interstate_number(&b.highway))
            .then_with(|| a.highway.cmp(&b.highway))
            .then_with(|| a.root_relation_id.cmp(&b.root_relation_id))
            .then_with(|| a.direction.cmp(&b.direction))
    });
    groups
}

pub fn normalize_direction(raw: &str) -> Option<String> {
    let value = raw.trim().to_ascii_lowercase();
    match value.as_str() {
        "" => None,
        "north" | "northbound" | "n" => Some("north".to_string()),
        "south" | "southbound" | "s" => Some("south".to_string()),
        "east" | "eastbound" | "e" => Some("east".to_string()),
        "west" | "westbound" | "w" => Some("west".to_string()),
        _ => None,
    }
}

fn interstate_number(highway: &str) -> i32 {
    highway
        .strip_prefix("I-")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{group_relation_members, normalize_direction, InterstateRelationMember};

    #[test]
    fn normalizes_compass_direction_variants() {
        assert_eq!(normalize_direction("north"), Some("north".to_string()));
        assert_eq!(normalize_direction("Northbound"), Some("north".to_string()));
        assert_eq!(normalize_direction("S"), Some("south".to_string()));
        assert_eq!(normalize_direction(""), None);
    }

    #[test]
    fn groups_members_by_root_and_direction() {
        let groups = group_relation_members(&[
            InterstateRelationMember {
                way_id: 2,
                highway: "I-95".to_string(),
                root_relation_id: 10,
                leaf_relation_id: 10,
                direction: Some("north".to_string()),
                role: None,
                sequence_index: 1,
            },
            InterstateRelationMember {
                way_id: 1,
                highway: "I-95".to_string(),
                root_relation_id: 10,
                leaf_relation_id: 10,
                direction: Some("north".to_string()),
                role: None,
                sequence_index: 0,
            },
            InterstateRelationMember {
                way_id: 3,
                highway: "I-95".to_string(),
                root_relation_id: 10,
                leaf_relation_id: 11,
                direction: Some("south".to_string()),
                role: None,
                sequence_index: 0,
            },
        ]);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].members[0].way_id, 1);
        assert_eq!(groups[0].members[1].way_id, 2);
        assert_eq!(groups[1].members[0].way_id, 3);
    }
}

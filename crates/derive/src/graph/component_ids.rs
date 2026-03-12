use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::anyhow;
use sqlx::PgPool;

use super::compress::{CompressedEdge, ExitCorridorEntry};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ComponentMerge {
    highway: String,
    from_component: i32,
    to_component: i32,
}

#[derive(Debug, Default)]
struct HighwayComponentPlan {
    transient_to_stable: HashMap<i32, i32>,
    merges: Vec<(i32, i32)>,
}

pub(super) async fn stabilize_component_ids(
    pool: &PgPool,
    edges: &mut [CompressedEdge],
    corridor_entries: &mut [ExitCorridorEntry],
) -> Result<(), anyhow::Error> {
    let transient_components = collect_transient_components(edges, corridor_entries);
    if transient_components.is_empty() {
        return Ok(());
    }

    let (mappings_by_highway, mut merges) =
        build_stabilization_plan(pool, &transient_components).await?;

    apply_component_mappings(edges, corridor_entries, &mappings_by_highway);
    apply_component_merges(pool, &mut merges).await?;

    Ok(())
}

fn collect_transient_components(
    edges: &[CompressedEdge],
    corridor_entries: &[ExitCorridorEntry],
) -> HashMap<String, BTreeMap<i32, HashSet<i64>>> {
    let mut transient_components: HashMap<String, BTreeMap<i32, HashSet<i64>>> = HashMap::new();

    for edge in edges {
        let by_component = transient_components
            .entry(edge.highway.clone())
            .or_default();
        let nodes = by_component.entry(edge.component).or_default();
        nodes.insert(edge.start_node);
        nodes.insert(edge.end_node);
    }

    for entry in corridor_entries {
        transient_components
            .entry(entry.highway.clone())
            .or_default()
            .entry(entry.component)
            .or_default()
            .insert(entry.node_id);
    }

    transient_components
}

async fn build_stabilization_plan(
    pool: &PgPool,
    transient_components: &HashMap<String, BTreeMap<i32, HashSet<i64>>>,
) -> Result<(HashMap<String, HashMap<i32, i32>>, Vec<ComponentMerge>), anyhow::Error> {
    let mut highways: Vec<String> = transient_components.keys().cloned().collect();
    highways.sort();

    let mut mappings_by_highway: HashMap<String, HashMap<i32, i32>> = HashMap::new();
    let mut merges: Vec<ComponentMerge> = Vec::new();

    for highway in highways {
        let Some(components) = transient_components.get(&highway) else {
            continue;
        };
        if components.is_empty() {
            continue;
        }

        let (max_existing_component, existing_by_node) =
            load_existing_components_for_highway(pool, &highway, components).await?;

        let plan =
            plan_highway_component_mapping(components, &existing_by_node, max_existing_component)?;
        if !plan.transient_to_stable.is_empty() {
            mappings_by_highway.insert(highway.clone(), plan.transient_to_stable);
        }
        for (from_component, to_component) in plan.merges {
            merges.push(ComponentMerge {
                highway: highway.clone(),
                from_component,
                to_component,
            });
        }
    }

    Ok((mappings_by_highway, merges))
}

async fn load_existing_components_for_highway(
    pool: &PgPool,
    highway: &str,
    transient_components: &BTreeMap<i32, HashSet<i64>>,
) -> Result<(i32, HashMap<i64, HashSet<i32>>), anyhow::Error> {
    let mut node_union: HashSet<i64> = HashSet::new();
    for nodes in transient_components.values() {
        node_union.extend(nodes.iter().copied());
    }
    let node_list: Vec<i64> = node_union.iter().copied().collect();

    let (max_existing_component,): (i32,) =
        sqlx::query_as("SELECT COALESCE(MAX(component), -1) FROM highway_edges WHERE highway = $1")
            .bind(highway)
            .fetch_one(pool)
            .await?;

    if node_list.is_empty() {
        return Ok((max_existing_component, HashMap::new()));
    }

    let overlaps: Vec<(i32, i64, i64)> = sqlx::query_as(
        "SELECT component, start_node, end_node \
         FROM highway_edges \
         WHERE highway = $1 \
           AND (start_node = ANY($2) OR end_node = ANY($2))",
    )
    .bind(highway)
    .bind(&node_list)
    .fetch_all(pool)
    .await?;

    let mut existing_by_node: HashMap<i64, HashSet<i32>> = HashMap::new();
    for (component, start_node, end_node) in overlaps {
        if node_union.contains(&start_node) {
            existing_by_node
                .entry(start_node)
                .or_default()
                .insert(component);
        }
        if node_union.contains(&end_node) {
            existing_by_node
                .entry(end_node)
                .or_default()
                .insert(component);
        }
    }

    Ok((max_existing_component, existing_by_node))
}

fn apply_component_mappings(
    edges: &mut [CompressedEdge],
    corridor_entries: &mut [ExitCorridorEntry],
    mappings_by_highway: &HashMap<String, HashMap<i32, i32>>,
) {
    for edge in edges {
        if let Some(mapping) = mappings_by_highway.get(&edge.highway) {
            if let Some(&stable) = mapping.get(&edge.component) {
                edge.component = stable;
            }
        }
    }

    for entry in corridor_entries {
        if let Some(mapping) = mappings_by_highway.get(&entry.highway) {
            if let Some(&stable) = mapping.get(&entry.component) {
                entry.component = stable;
            }
        }
    }
}

async fn apply_component_merges(
    pool: &PgPool,
    merges: &mut Vec<ComponentMerge>,
) -> Result<(), anyhow::Error> {
    merges.sort();
    merges.dedup();

    if merges.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Stabilizing component ids by merging {} existing highway-component pairs",
        merges.len()
    );

    let mut tx = pool.begin().await?;
    for merge in merges {
        sqlx::query(
            "UPDATE highway_edges \
             SET component = $3 \
             WHERE highway = $1 AND component = $2",
        )
        .bind(&merge.highway)
        .bind(merge.from_component)
        .bind(merge.to_component)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE exit_corridors \
             SET graph_component = $3 \
             WHERE highway = $1 AND graph_component = $2",
        )
        .bind(&merge.highway)
        .bind(merge.from_component)
        .bind(merge.to_component)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(())
}

fn plan_highway_component_mapping(
    transient_components: &BTreeMap<i32, HashSet<i64>>,
    existing_components_by_node: &HashMap<i64, HashSet<i32>>,
    max_existing_component: i32,
) -> Result<HighwayComponentPlan, anyhow::Error> {
    let mut plan = HighwayComponentPlan::default();
    let mut existing_remap: HashMap<i32, i32> = HashMap::new();
    let mut next_component = max_existing_component;

    for (&transient_component, nodes) in transient_components {
        let mut touched_existing: BTreeSet<i32> = BTreeSet::new();
        for node in nodes {
            if let Some(existing_components) = existing_components_by_node.get(node) {
                for &existing_component in existing_components {
                    let resolved = resolve_component(existing_component, &mut existing_remap);
                    touched_existing.insert(resolved);
                }
            }
        }

        let stable_component = if let Some(&canonical_existing) = touched_existing.first() {
            for &other in touched_existing.iter().skip(1) {
                if other != canonical_existing {
                    existing_remap.insert(other, canonical_existing);
                }
            }
            canonical_existing
        } else {
            next_component = next_component
                .checked_add(1)
                .ok_or_else(|| anyhow!("component id overflow while assigning stable component"))?;
            next_component
        };

        plan.transient_to_stable
            .insert(transient_component, stable_component);
    }

    let mut merge_sources: Vec<i32> = existing_remap.keys().copied().collect();
    merge_sources.sort_unstable();
    merge_sources.dedup();
    for source in merge_sources {
        let resolved = resolve_component(source, &mut existing_remap);
        if source != resolved {
            plan.merges.push((source, resolved));
        }
    }

    Ok(plan)
}

fn resolve_component(component: i32, remap: &mut HashMap<i32, i32>) -> i32 {
    let mut cur = component;
    let mut trail: Vec<i32> = Vec::new();

    while let Some(&parent) = remap.get(&cur) {
        if parent == cur {
            break;
        }
        trail.push(cur);
        cur = parent;
    }

    for node in trail {
        remap.insert(node, cur);
    }

    cur
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_reuses_existing_component_by_shared_node() {
        let mut transient = BTreeMap::new();
        transient.insert(0, HashSet::from([10_i64, 11_i64]));
        transient.insert(1, HashSet::from([20_i64, 21_i64]));

        let mut existing_by_node = HashMap::new();
        existing_by_node.insert(10_i64, HashSet::from([7_i32]));

        let plan = plan_highway_component_mapping(&transient, &existing_by_node, 9).unwrap();
        assert_eq!(plan.transient_to_stable.get(&0), Some(&7));
        assert_eq!(plan.transient_to_stable.get(&1), Some(&10));
        assert!(plan.merges.is_empty());
    }

    #[test]
    fn plan_merges_multiple_existing_components_deterministically() {
        let mut transient = BTreeMap::new();
        transient.insert(0, HashSet::from([100_i64, 101_i64]));

        let mut existing_by_node = HashMap::new();
        existing_by_node.insert(100_i64, HashSet::from([9_i32]));
        existing_by_node.insert(101_i64, HashSet::from([3_i32]));

        let plan = plan_highway_component_mapping(&transient, &existing_by_node, 12).unwrap();
        assert_eq!(plan.transient_to_stable.get(&0), Some(&3));
        assert_eq!(plan.merges, vec![(9, 3)]);
    }
}

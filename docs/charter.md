# Project Charter

## Mission

OpenInterstate exists to make the U.S. Interstate system easier to understand
and easier to build with by publishing usable, reproducible interstate datasets.

## Problem

OpenStreetMap contains the raw ingredients, but turning them into a coherent
interstate model still requires substantial transformation work.

OpenInterstate aims to publish that derived layer openly.

## Initial Product Definition

OpenInterstate will publish:

1. directional interstate corridors
2. normalized exits and exit aliases
3. linked places and reachability metadata
4. reference routes for exploration, QA, and examples
5. release manifests, checksums, and schema docs

## Non-Goals

OpenInterstate will not initially focus on:

1. Pike runtime packaging
2. Pike `/v1/feed` contracts
3. consumer app features
4. broad non-interstate road coverage
5. legal foundation setup

## Relationship To Pike

Pike is a downstream consumer and product.

OpenInterstate should be useful even if Pike did not exist. Pike should be able
to consume OpenInterstate outputs without forcing Pike-specific naming or data
packaging onto the public project.

## Decision Principles

1. Use neutral public names.
2. Favor reproducible releases over one-off exports.
3. Keep lineage and attribution explicit.
4. Optimize for developers and GIS users first.
5. Avoid overbuilding governance before the project has users.

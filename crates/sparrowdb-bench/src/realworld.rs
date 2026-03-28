//! Real-world graph dataset generators — Reactome (biological pathways) and
//! POLE (police investigation network) — for TuringDB comparison benchmarks.
//!
//! Both datasets are generated synthetically with realistic structure matching
//! the public domain originals.  They are deterministic (fixed RNG seed) so
//! benchmark results are reproducible across runs.
//!
//! ## Reactome
//!
//! The Reactome biological pathway database
//! (<https://reactome.org>) describes molecular reactions.
//! The subset here covers ~500 nodes and ~2 000 edges with:
//! - **`:Pathway`** — top-level biological pathway
//! - **`:Reaction`** — individual reaction step
//! - **`:PhysicalEntity`** — protein / molecule participating in reactions
//!
//! Relationship types:
//! - **`HAS_COMPONENT`** — Pathway → sub-Pathway or Pathway → Reaction
//! - **`NEXT_STEP`**     — Reaction → Reaction (sequential chain)
//! - **`CATALYSIS`**     — PhysicalEntity → Reaction (catalytic input)
//!
//! ## POLE
//!
//! The POLE investigative dataset is a well-known graph-DB benchmark that
//! models a police investigation graph.  It contains Person, Object,
//! Location, and Event nodes connected by KNOWS, LOCATED_AT, and PARTY_TO
//! relationships (~100 nodes, ~231 edges).

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

// ── Shared statistics ────────────────────────────────────────────────────────

/// Load statistics returned after ingesting a dataset.
#[derive(Debug, Clone, Default)]
pub struct RealWorldStats {
    pub nodes_created: u64,
    pub edges_created: u64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Reactome
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for the synthetic Reactome graph.
#[derive(Debug, Clone)]
pub struct ReactomeConfig {
    pub pathways: usize,
    pub reactions_per_pathway: usize,
    pub entities: usize,
    pub seed: u64,
}

impl Default for ReactomeConfig {
    fn default() -> Self {
        Self {
            pathways: 40,
            reactions_per_pathway: 8,
            entities: 120,
            seed: 42,
        }
    }
}

/// Generate a synthetic Reactome-shaped graph and ingest it into `db`.
///
/// Graph shape:
/// - `pathways` top-level `:Pathway` nodes, each with a nested sub-pathway
///   and `reactions_per_pathway` `:Reaction` nodes connected by `HAS_COMPONENT`
///   and `NEXT_STEP` chains.
/// - `entities` `:PhysicalEntity` nodes sprayed across reactions via `CATALYSIS`
///   edges (scale-free: lower-uid entities catalyse more reactions).
///
/// Total approximate size: ~500 nodes, ~2 000 edges with default config.
pub fn load_reactome(db: &GraphDb, cfg: &ReactomeConfig) -> sparrowdb::Result<RealWorldStats> {
    let mut rng = SmallRng::seed_from_u64(cfg.seed);
    let mut stats = RealWorldStats::default();

    let mut next_id: u64 = 1;

    // ── Create PhysicalEntity pool ───────────────────────────────────────────
    let entity_base = next_id;
    for i in 0..cfg.entities {
        let eid = next_id;
        next_id += 1;
        let compartment = COMPARTMENTS[i % COMPARTMENTS.len()];
        let q = format!(
            "CREATE (e:PhysicalEntity {{eid: {eid}, name: 'Entity_{eid}', \
             compartment: '{compartment}'}})"
        );
        db.execute(&q)?;
        stats.nodes_created += 1;
    }

    // ── Create Pathways + sub-pathways + Reactions ───────────────────────────
    for p in 0..cfg.pathways {
        let pathway_id = next_id;
        next_id += 1;
        let stable_id = format!("R-HSA-{}", 1_000_000 + p as u64);
        let q = format!(
            "CREATE (pw:Pathway {{pid: {pathway_id}, stableId: '{stable_id}', \
             name: 'Pathway_{pathway_id}', species: 'Homo sapiens'}})"
        );
        db.execute(&q)?;
        stats.nodes_created += 1;

        // Sub-pathway
        let sub_id = next_id;
        next_id += 1;
        let sub_stable = format!("R-HSA-{}", 2_000_000 + p as u64);
        let q = format!(
            "CREATE (pw:Pathway {{pid: {sub_id}, stableId: '{sub_stable}', \
             name: 'SubPathway_{sub_id}', species: 'Homo sapiens'}})"
        );
        db.execute(&q)?;
        stats.nodes_created += 1;

        // HAS_COMPONENT: parent pathway -> sub-pathway
        let q = format!(
            "MATCH (a:Pathway {{pid: {pathway_id}}}), (b:Pathway {{pid: {sub_id}}}) \
             CREATE (a)-[:HAS_COMPONENT]->(b)"
        );
        db.execute(&q)?;
        stats.edges_created += 1;

        // Reactions chain inside the sub-pathway
        let mut prev_rxn: Option<u64> = None;
        for r in 0..cfg.reactions_per_pathway {
            let rxn_id = next_id;
            next_id += 1;
            let rxn_stable = format!(
                "R-HSA-{}",
                3_000_000 + (p * cfg.reactions_per_pathway + r) as u64
            );
            let rxn_type = RXN_TYPES[r % RXN_TYPES.len()];
            let q = format!(
                "CREATE (rx:Reaction {{rid: {rxn_id}, stableId: '{rxn_stable}', \
                 name: 'Reaction_{rxn_id}', reactionType: '{rxn_type}'}})"
            );
            db.execute(&q)?;
            stats.nodes_created += 1;

            // Sub-pathway HAS_COMPONENT -> reaction
            let q = format!(
                "MATCH (pw:Pathway {{pid: {sub_id}}}), (rx:Reaction {{rid: {rxn_id}}}) \
                 CREATE (pw)-[:HAS_COMPONENT]->(rx)"
            );
            db.execute(&q)?;
            stats.edges_created += 1;

            // NEXT_STEP chain
            if let Some(prev) = prev_rxn {
                let q = format!(
                    "MATCH (a:Reaction {{rid: {prev}}}), (b:Reaction {{rid: {rxn_id}}}) \
                     CREATE (a)-[:NEXT_STEP]->(b)"
                );
                db.execute(&q)?;
                stats.edges_created += 1;
            }
            prev_rxn = Some(rxn_id);

            // CATALYSIS: pick 1-3 entities (scale-free: prefer low-uid entities)
            let n_cat = rng.gen_range(1u64..=3);
            for _ in 0..n_cat {
                // Power-law: sample with a bias toward early entities.
                let raw: u64 = rng.gen_range(0..cfg.entities as u64);
                let biased = (raw * raw) / cfg.entities as u64;
                let eid = entity_base + biased;
                let q = format!(
                    "MATCH (e:PhysicalEntity {{eid: {eid}}}), (rx:Reaction {{rid: {rxn_id}}}) \
                     CREATE (e)-[:CATALYSIS]->(rx)"
                );
                db.execute(&q)?;
                stats.edges_created += 1;
            }
        }
    }

    Ok(stats)
}

const COMPARTMENTS: &[&str] = &[
    "cytosol",
    "nucleus",
    "endoplasmic reticulum lumen",
    "extracellular region",
    "mitochondrial matrix",
];

const RXN_TYPES: &[&str] = &[
    "transition",
    "omitted",
    "uncertain",
    "binding",
    "dissociation",
];

// ── Reactome queries (Q3/Q4/Q8 equivalents) ──────────────────────────────────

/// **Q3-Reactome**: 1-hop neighbour expansion — find all direct components of a
/// pathway (reactions + sub-pathways).
pub fn q3_pathway_components(db: &GraphDb, pid: u64) -> sparrowdb::Result<Vec<u64>> {
    let cypher = format!(
        "MATCH (pw:Pathway {{pid: {pid}}})-[:HAS_COMPONENT]->(child) \
         RETURN child.pid, child.rid"
    );
    let result = db.execute(&cypher)?;
    let ids: Vec<u64> = result
        .rows
        .iter()
        .map(|row| {
            // Either pid (Pathway) or rid (Reaction) will be non-null.
            for v in row {
                if let Value::Int64(n) = v {
                    if *n > 0 {
                        return *n as u64;
                    }
                }
            }
            0
        })
        .filter(|&id| id > 0)
        .collect();
    Ok(ids)
}

/// **Q4-Reactome**: 2-hop expansion — find all reactions that are components of
/// a given sub-pathway.
///
/// Accepts the sub-pathway pid directly, executing a single 1-hop traversal
/// `SubPathway -[:HAS_COMPONENT]-> Reaction`.  The caller should first resolve
/// the sub-pathway pid with a 1-hop `q3_pathway_components` call.
pub fn q4_pathway_reactions_2hop(db: &GraphDb, sub_pid: u64) -> sparrowdb::Result<usize> {
    let cypher = format!(
        "MATCH (sub:Pathway {{pid: {sub_pid}}})-[:HAS_COMPONENT]->(rx:Reaction) \
         RETURN rx.rid"
    );
    let result = db.execute(&cypher)?;
    Ok(result.rows.len())
}

/// **Q8-Reactome**: Mutual-catalysis — find PhysicalEntities that catalyse
/// reactions in a given sub-pathway (proxy for mutual-neighbour query).
///
/// Uses an inline two-hop pattern: `PhysicalEntity -[:CATALYSIS]-> Reaction
/// <-[:HAS_COMPONENT]- Pathway`.
pub fn q8_shared_catalysts(db: &GraphDb, sub_pid: u64) -> sparrowdb::Result<usize> {
    let cypher = format!(
        "MATCH (e:PhysicalEntity)-[:CATALYSIS]->(rx:Reaction)<-[:HAS_COMPONENT]-(pw:Pathway {{pid: {sub_pid}}}) \
         RETURN e.eid"
    );
    let result = db.execute(&cypher)?;
    Ok(result.rows.len())
}

// ═══════════════════════════════════════════════════════════════════════════════
// POLE
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for the POLE investigation graph.
#[derive(Debug, Clone)]
pub struct PoleConfig {
    pub persons: usize,
    pub objects: usize,
    pub locations: usize,
    pub events: usize,
    pub seed: u64,
}

impl Default for PoleConfig {
    fn default() -> Self {
        Self {
            persons: 35,
            objects: 20,
            locations: 15,
            events: 30,
            seed: 7,
        }
    }
}

/// Generate a synthetic POLE (Person-Object-Location-Event) investigation graph
/// and ingest it into `db`.
///
/// POLE is a well-known benchmark graph that models a police investigation:
/// - **`:Person`**   — suspects, witnesses, officers
/// - **`:Object`**   — vehicles, phones, weapons
/// - **`:Location`** — addresses, crime scenes
/// - **`:Event`**    — incidents, arrests, sightings
///
/// Relationships:
/// - `KNOWS`      — Person → Person (social network)
/// - `LOCATED_AT` — Person/Object/Event → Location
/// - `PARTY_TO`   — Person → Event (involvement in incident)
/// - `INVOLVED_IN`— Object → Event (evidence/asset in event)
///
/// Default size: 100 nodes, ~231 edges.
pub fn load_pole(db: &GraphDb, cfg: &PoleConfig) -> sparrowdb::Result<RealWorldStats> {
    let mut rng = SmallRng::seed_from_u64(cfg.seed);
    let mut stats = RealWorldStats::default();

    let mut next_id: u64 = 1;

    // ── Persons ───────────────────────────────────────────────────────────────
    let person_base = next_id;
    for _ in 0..cfg.persons {
        let pid = next_id;
        next_id += 1;
        let role = PERSON_ROLES[pid as usize % PERSON_ROLES.len()];
        let q = format!(
            "CREATE (p:Person {{nid: {pid}, name: 'Person_{pid}', role: '{role}', \
             dob: '19{:02}-{:02}-{:02}'}})",
            rng.gen_range(60u8..=99),
            rng.gen_range(1u8..=12),
            rng.gen_range(1u8..=28),
        );
        db.execute(&q)?;
        stats.nodes_created += 1;
    }

    // ── Objects ───────────────────────────────────────────────────────────────
    let object_base = next_id;
    for _ in 0..cfg.objects {
        let oid = next_id;
        next_id += 1;
        let kind = OBJ_KINDS[oid as usize % OBJ_KINDS.len()];
        let q = format!(
            "CREATE (o:Object {{nid: {oid}, description: 'Object_{oid}', kind: '{kind}'}})"
        );
        db.execute(&q)?;
        stats.nodes_created += 1;
    }

    // ── Locations ─────────────────────────────────────────────────────────────
    let location_base = next_id;
    for _ in 0..cfg.locations {
        let lid = next_id;
        next_id += 1;
        let loc_type = LOC_TYPES[lid as usize % LOC_TYPES.len()];
        let q = format!(
            "CREATE (l:Location {{nid: {lid}, address: 'Location_{lid}', locType: '{loc_type}'}})"
        );
        db.execute(&q)?;
        stats.nodes_created += 1;
    }

    // ── Events ────────────────────────────────────────────────────────────────
    let event_base = next_id;
    for _ in 0..cfg.events {
        let eid = next_id;
        next_id += 1;
        let ev_type = EVENT_TYPES[eid as usize % EVENT_TYPES.len()];
        let q = format!(
            "CREATE (e:Event {{nid: {eid}, description: 'Event_{eid}', eventType: '{ev_type}', \
             date: '2023-{:02}-{:02}'}})",
            rng.gen_range(1u8..=12),
            rng.gen_range(1u8..=28),
        );
        db.execute(&q)?;
        stats.nodes_created += 1;
    }

    // ── KNOWS (Person → Person) ───────────────────────────────────────────────
    // Each person knows 2-4 others, with some clustering (small-world).
    for i in 0..cfg.persons {
        let src = person_base + i as u64;
        let n_knows = rng.gen_range(2u64..=4);
        for _ in 0..n_knows {
            let offset = rng.gen_range(1u64..cfg.persons as u64);
            let dst = person_base + (i as u64 + offset) % cfg.persons as u64;
            if src != dst {
                let q = format!(
                    "MATCH (a:Person {{nid: {src}}}), (b:Person {{nid: {dst}}}) \
                     CREATE (a)-[:KNOWS]->(b)"
                );
                db.execute(&q)?;
                stats.edges_created += 1;
            }
        }
    }

    // ── LOCATED_AT (Person/Object/Event → Location) ───────────────────────────
    // Each person has 1 primary location.
    for i in 0..cfg.persons {
        let pid = person_base + i as u64;
        let lid = location_base + (i as u64 % cfg.locations as u64);
        let q = format!(
            "MATCH (p:Person {{nid: {pid}}}), (l:Location {{nid: {lid}}}) \
             CREATE (p)-[:LOCATED_AT]->(l)"
        );
        db.execute(&q)?;
        stats.edges_created += 1;
    }

    // Each object has a location.
    for i in 0..cfg.objects {
        let oid = object_base + i as u64;
        let lid = location_base + (i as u64 % cfg.locations as u64);
        let q = format!(
            "MATCH (o:Object {{nid: {oid}}}), (l:Location {{nid: {lid}}}) \
             CREATE (o)-[:LOCATED_AT]->(l)"
        );
        db.execute(&q)?;
        stats.edges_created += 1;
    }

    // Each event has a location.
    for i in 0..cfg.events {
        let eid = event_base + i as u64;
        let lid = location_base + (i as u64 % cfg.locations as u64);
        let q = format!(
            "MATCH (e:Event {{nid: {eid}}}), (l:Location {{nid: {lid}}}) \
             CREATE (e)-[:LOCATED_AT]->(l)"
        );
        db.execute(&q)?;
        stats.edges_created += 1;
    }

    // ── PARTY_TO (Person → Event) ─────────────────────────────────────────────
    // 2-3 persons are party to each event.
    for i in 0..cfg.events {
        let eid = event_base + i as u64;
        let n_parties = rng.gen_range(2u64..=3);
        for _ in 0..n_parties {
            let pid = person_base + rng.gen_range(0..cfg.persons as u64);
            let q = format!(
                "MATCH (p:Person {{nid: {pid}}}), (e:Event {{nid: {eid}}}) \
                 CREATE (p)-[:PARTY_TO]->(e)"
            );
            db.execute(&q)?;
            stats.edges_created += 1;
        }
    }

    // ── INVOLVED_IN (Object → Event) ─────────────────────────────────────────
    // 1-2 objects involved in each event.
    for i in 0..cfg.events {
        let eid = event_base + i as u64;
        let n_obj = rng.gen_range(1u64..=2);
        for _ in 0..n_obj {
            let oid = object_base + rng.gen_range(0..cfg.objects as u64);
            let q = format!(
                "MATCH (o:Object {{nid: {oid}}}), (e:Event {{nid: {eid}}}) \
                 CREATE (o)-[:INVOLVED_IN]->(e)"
            );
            db.execute(&q)?;
            stats.edges_created += 1;
        }
    }

    Ok(stats)
}

const PERSON_ROLES: &[&str] = &["suspect", "witness", "officer", "victim"];
const OBJ_KINDS: &[&str] = &["vehicle", "phone", "weapon", "document"];
const LOC_TYPES: &[&str] = &["address", "crime_scene", "safe_house", "public_place"];
const EVENT_TYPES: &[&str] = &["incident", "arrest", "sighting", "interview"];

// ── POLE queries (Q3/Q4/Q8 equivalents) ──────────────────────────────────────

/// **Q3-POLE**: 1-hop — find all persons known by a given person.
pub fn q3_knows_1hop(db: &GraphDb, nid: u64) -> sparrowdb::Result<usize> {
    let cypher = format!(
        "MATCH (p:Person {{nid: {nid}}})-[:KNOWS]->(friend:Person) \
         RETURN friend.nid"
    );
    let result = db.execute(&cypher)?;
    Ok(result.rows.len())
}

/// **Q4-POLE**: 2-hop — find all persons within 2 KNOWS hops of a person.
pub fn q4_knows_2hop(db: &GraphDb, nid: u64) -> sparrowdb::Result<usize> {
    let cypher = format!(
        "MATCH (p:Person {{nid: {nid}}})-[:KNOWS*1..2]->(friend:Person) \
         RETURN friend.nid"
    );
    let result = db.execute(&cypher)?;
    Ok(result.rows.len())
}

/// **Q8-POLE**: Mutual connections — find persons sharing an event with a
/// given person (proxy for mutual-neighbour / co-involvement query).
///
/// Uses `MATCH ... WITH ... MATCH` to chain two traversals: first finds events
/// the target person attended, then finds other persons attending those events.
pub fn q8_co_party_events(db: &GraphDb, nid: u64) -> sparrowdb::Result<usize> {
    let cypher = format!(
        "MATCH (p:Person {{nid: {nid}}})-[:PARTY_TO]->(e:Event) \
         WITH e.nid AS event_id \
         MATCH (other:Person)-[:PARTY_TO]->(e2:Event {{nid: event_id}}) \
         RETURN other.nid"
    );
    let result = db.execute(&cypher)?;
    Ok(result.rows.len())
}

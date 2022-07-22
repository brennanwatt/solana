use {
    crate::{broadcast_stage::BroadcastStage, retransmit_stage::RetransmitStage},
    itertools::Itertools,
    lru::LruCache,
    rand::{seq::SliceRandom, Rng, SeedableRng},
    rand_chacha::ChaChaRng,
    solana_gossip::{
        cluster_info::{compute_retransmit_peers, ClusterInfo},
        contact_info::ContactInfo,
        crds::GossipRoute,
        crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        crds_value::{CrdsData, CrdsValue},
        weighted_shuffle::WeightedShuffle,
    },
    solana_ledger::shred::Shred,
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::{Epoch, Slot},
        feature_set,
        pubkey::Pubkey,
        signature::Keypair,
        timing::timestamp,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        any::TypeId,
        cmp::Reverse,
        collections::HashMap,
        iter::{once, repeat_with},
        marker::PhantomData,
        net::SocketAddr,
        ops::Deref,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

#[allow(clippy::large_enum_variant)]
enum NodeId {
    // TVU node obtained through gossip (staked or not).
    ContactInfo(ContactInfo),
    // Staked node with no contact-info in gossip table.
    Pubkey(Pubkey),
}

pub struct Node {
    node: NodeId,
    stake: u64,
}

impl Node {
    fn id(&self) -> Pubkey {
        match &self.node {
            NodeId::ContactInfo(contact_info) => contact_info.id,
            NodeId::Pubkey(pk) => *pk,
        }
    }
}

pub struct ClusterNodes<T> {
    pubkey: Pubkey, // The local node itself.
    // All staked nodes + other known tvu-peers + the node itself;
    // sorted by (stake, pubkey) in descending order.
    nodes: Vec<Node>,
    // Reverse index from nodes pubkey to their index in self.nodes.
    index: HashMap<Pubkey, /*index:*/ usize>,
    weighted_shuffle: WeightedShuffle</*stake:*/ u64>,
    _phantom: PhantomData<T>,
}

type CacheEntry<T> = Option<(/*as of:*/ Instant, Arc<ClusterNodes<T>>)>;

pub struct ClusterNodesCache<T> {
    // Cache entries are wrapped in Arc<Mutex<...>>, so that, when needed, only
    // one thread does the computations to update the entry for the epoch.
    cache: Mutex<LruCache<Epoch, Arc<Mutex<CacheEntry<T>>>>>,
    ttl: Duration, // Time to live.
}

impl Node {
    #[inline]
    fn pubkey(&self) -> Pubkey {
        match &self.node {
            NodeId::Pubkey(pubkey) => *pubkey,
            NodeId::ContactInfo(node) => node.id,
        }
    }

    #[inline]
    fn contact_info(&self) -> Option<&ContactInfo> {
        match &self.node {
            NodeId::Pubkey(_) => None,
            NodeId::ContactInfo(node) => Some(node),
        }
    }
}

impl<T> ClusterNodes<T> {
    pub(crate) fn num_peers(&self) -> usize {
        self.nodes.len().saturating_sub(1)
    }

    // A peer is considered live if they generated their contact info recently.
    pub(crate) fn num_peers_live(&self, now: u64) -> usize {
        self.nodes
            .iter()
            .filter(|node| node.pubkey() != self.pubkey)
            .filter_map(|node| node.contact_info())
            .filter(|node| {
                let elapsed = if node.wallclock < now {
                    now - node.wallclock
                } else {
                    node.wallclock - now
                };
                elapsed < CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS
            })
            .count()
    }
}

impl ClusterNodes<BroadcastStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        new_cluster_nodes(cluster_info, stakes)
    }

    pub(crate) fn get_broadcast_addrs(
        &self,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
        socket_addr_space: &SocketAddrSpace,
    ) -> Vec<SocketAddr> {
        const MAX_CONTACT_INFO_AGE: Duration = Duration::from_secs(2 * 60);
        let shred_seed = shred.seed(self.pubkey, root_bank);
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let index = match self.weighted_shuffle.first(&mut rng) {
            None => return Vec::default(),
            Some(index) => index,
        };
        if let Some(node) = self.nodes[index].contact_info() {
            let now = timestamp();
            let age = Duration::from_millis(now.saturating_sub(node.wallclock));
            if age < MAX_CONTACT_INFO_AGE
                && ContactInfo::is_valid_address(&node.tvu, socket_addr_space)
            {
                return vec![node.tvu];
            }
        }
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let nodes: Vec<&Node> = self
            .weighted_shuffle
            .clone()
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        if nodes.is_empty() {
            return Vec::default();
        }
        if drop_redundant_turbine_path(shred.slot(), root_bank) {
            let peers = once(nodes[0]).chain(get_retransmit_peers(fanout, 0, &nodes));
            let addrs = peers.filter_map(Node::contact_info).map(|peer| peer.tvu);
            return addrs
                .filter(|addr| ContactInfo::is_valid_address(addr, socket_addr_space))
                .collect();
        }
        let (neighbors, children) = compute_retransmit_peers(fanout, 0, &nodes);
        neighbors[..1]
            .iter()
            .filter_map(|node| Some(node.contact_info()?.tvu))
            .chain(
                neighbors[1..]
                    .iter()
                    .filter_map(|node| Some(node.contact_info()?.tvu_forwards)),
            )
            .chain(
                children
                    .iter()
                    .filter_map(|node| Some(node.contact_info()?.tvu)),
            )
            .filter(|addr| ContactInfo::is_valid_address(addr, socket_addr_space))
            .collect()
    }
}

impl ClusterNodes<RetransmitStage> {
    pub fn get_retransmit_addrs(
        &self,
        slot_leader: Pubkey,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
    ) -> Vec<SocketAddr> {
        let (neighbors, children) =
            self.get_retransmit_peers(slot_leader, shred, root_bank, fanout);
        if neighbors.is_empty() {
            let peers = children.into_iter().filter_map(Node::contact_info);
            return peers.map(|peer| peer.tvu).collect();
        }
        // If the node is on the critical path (i.e. the first node in each
        // neighborhood), it should send the packet to tvu socket of its
        // children and also tvu_forward socket of its neighbors. Otherwise it
        // should only forward to tvu_forwards socket of its children.
        if neighbors[0].pubkey() != self.pubkey {
            return children
                .iter()
                .filter_map(|node| Some(node.contact_info()?.tvu_forwards))
                .collect();
        }
        // First neighbor is this node itself, so skip it.
        neighbors[1..]
            .iter()
            .filter_map(|node| Some(node.contact_info()?.tvu_forwards))
            .chain(
                children
                    .iter()
                    .filter_map(|node| Some(node.contact_info()?.tvu)),
            )
            .collect()
    }

    pub fn get_retransmit_peers(
        &self,
        slot_leader: Pubkey,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
    ) -> (
        Vec<&Node>, // neighbors
        Vec<&Node>, // children
    ) {
        let shred_seed = shred.seed(slot_leader, root_bank);
        let mut weighted_shuffle = self.weighted_shuffle.clone();
        // Exclude slot leader from list of nodes.
        if slot_leader == self.pubkey {
            error!("retransmit from slot leader: {}", slot_leader);
        } else if let Some(index) = self.index.get(&slot_leader) {
            weighted_shuffle.remove_index(*index);
        };
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == self.pubkey)
            .unwrap();
        if drop_redundant_turbine_path(shred.slot(), root_bank) {
            let peers = get_retransmit_peers(fanout, self_index, &nodes);
            return (Vec::default(), peers.collect());
        }
        let (neighbors, children) = compute_retransmit_peers(fanout, self_index, &nodes);
        // Assert that the node itself is included in the set of neighbors, at
        // the right offset.
        debug_assert_eq!(neighbors[self_index % fanout].pubkey(), self.pubkey);
        (neighbors, children)
    }

    pub fn get_retransmit_ancestors(
        &self,
        slot_leader: Pubkey,
        shred: &Shred,
        fanout: usize,
        rcvr_pubkey: Pubkey,
    ) -> ((bool, Option<Pubkey>, usize), Option<Pubkey>) {
        let shred_seed = shred.seed2(slot_leader);
        let mut weighted_shuffle = self.weighted_shuffle.clone();
        // Exclude slot leader from list of nodes.
        if slot_leader == rcvr_pubkey {
            error!("retransmit from slot leader: {}", slot_leader);
        } else if let Some(index) = self.index.get(&slot_leader) {
            debug!("Removed slot leader at index {}", *index);
            weighted_shuffle.remove_index(*index);
        };
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == rcvr_pubkey)
            .unwrap();

        debug!(
            "turbine shred verifier,
            leader: {},
            slot: {}, shred_index: {},
            self neighborhood index: {}",
            slot_leader,
            shred.slot(),
            shred.index(),
            self_index,
        );
        trace!(
            "all cluster pubkeys: {:?}",
            nodes.iter().map(|n| n.id()).collect::<Vec<Pubkey>>()
        );

        (
            get_parent(fanout, self_index, &nodes),
            get_anchor(fanout, self_index, &nodes),
        )
    }
}

pub fn new_cluster_nodes<T: 'static>(
    cluster_info: &ClusterInfo,
    stakes: &HashMap<Pubkey, u64>,
) -> ClusterNodes<T> {
    let self_pubkey = cluster_info.id();
    let nodes = get_nodes(cluster_info, stakes);
    let index: HashMap<_, _> = nodes
        .iter()
        .enumerate()
        .map(|(ix, node)| (node.pubkey(), ix))
        .collect();
    let broadcast = TypeId::of::<T>() == TypeId::of::<BroadcastStage>();
    let stakes: Vec<u64> = nodes.iter().map(|node| node.stake).collect();
    let mut weighted_shuffle = WeightedShuffle::new("cluster-nodes", &stakes);
    if broadcast {
        weighted_shuffle.remove_index(index[&self_pubkey]);
    }
    ClusterNodes {
        pubkey: self_pubkey,
        nodes,
        index,
        weighted_shuffle,
        _phantom: PhantomData::default(),
    }
}

// All staked nodes + other known tvu-peers + the node itself;
// sorted by (stake, pubkey) in descending order.
fn get_nodes(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Vec<Node> {
    let self_pubkey = cluster_info.id();
    // The local node itself.
    std::iter::once({
        let stake = stakes.get(&self_pubkey).copied().unwrap_or_default();
        let node = NodeId::from(cluster_info.my_contact_info());
        Node { node, stake }
    })
    // All known tvu-peers from gossip.
    .chain(cluster_info.tvu_peers().into_iter().map(|node| {
        let stake = stakes.get(&node.id).copied().unwrap_or_default();
        let node = NodeId::from(node);
        Node { node, stake }
    }))
    // All staked nodes.
    .chain(
        stakes
            .iter()
            .filter(|(_, stake)| **stake > 0)
            .map(|(&pubkey, &stake)| Node {
                node: NodeId::from(pubkey),
                stake,
            }),
    )
    .sorted_by_key(|node| Reverse((node.stake, node.pubkey())))
    // Since sorted_by_key is stable, in case of duplicates, this
    // will keep nodes with contact-info.
    .dedup_by(|a, b| a.pubkey() == b.pubkey())
    .collect()
}

// root     : [0]
// 1st layer: [1, 2, ..., fanout]
// 2nd layer: [[fanout + 1, ..., fanout * 2],
//             [fanout * 2 + 1, ..., fanout * 3],
//             ...
//             [fanout * fanout + 1, ..., fanout * (fanout + 1)]]
// 3rd layer: ...
// ...
// The leader node broadcasts shreds to the root node.
// The root node retransmits the shreds to all nodes in the 1st layer.
// Each other node retransmits shreds to fanout many nodes in the next layer.
// For example the node k in the 1st layer will retransmit to nodes:
// fanout + k, 2*fanout + k, ..., fanout*fanout + k
fn get_retransmit_peers<T: Copy>(
    fanout: usize,
    index: usize, // Local node's index withing the nodes slice.
    nodes: &[T],
) -> impl Iterator<Item = T> + '_ {
    // Node's index within its neighborhood.
    let offset = index.saturating_sub(1) % fanout;
    // First node in the neighborhood.
    let anchor = index - offset;
    let step = if index == 0 { 1 } else { fanout };
    (anchor * fanout + offset + 1..)
        .step_by(step)
        .take(fanout)
        .map(|i| nodes.get(i))
        .while_some()
        .copied()
}

fn get_anchor(
    fanout: usize,
    index: usize, // Local node's index withing the nodes slice.
    nodes: &[&Node],
) -> Option<Pubkey> {
    // Node's index within its neighborhood.
    let offset = index.saturating_sub(1) % fanout;
    // First node in the neighborhood.
    let anchor = index - offset;
    if anchor == index {
        // Anchor of anchor is anchor. Don't fall for this trap.
        None
    } else {
        debug!(
            "Anchor is {}, offset is {}",
            nodes.get(anchor).unwrap().pubkey(),
            anchor
        );
        Some(nodes.get(anchor).unwrap().pubkey())
    }
}

fn get_parent(
    fanout: usize,
    index: usize, // Local node's index within the nodes vector
    nodes: &[&Node],
) -> (bool, Option<Pubkey>, usize) {
    // Node's index within its neighborhood.
    if index == 0 {
        debug!("I am root");
        return (true, None, 0);
    }

    let mut temp = index - 1;
    let mut layer = 0;
    loop {
        temp /= fanout;
        layer += 1;
        if temp == 0 {
            break;
        }
    }
    debug!(
        "My index is {}, layer is {}, offset is {}",
        index,
        layer,
        index.saturating_sub(1) % fanout
    );
    if layer == 1 {
        debug!("Parent is root {}", nodes.get(0).unwrap().pubkey());
        (true, Some(nodes.get(0).unwrap().pubkey()), layer)
    } else {
        let mut parent = index % fanout;
        if parent == 0 {
            parent = fanout;
        }
        debug!(
            "Parent is {}, layer is {}, offset is {}",
            nodes.get(parent).unwrap().pubkey(),
            (parent - 1) / fanout + 1,
            parent - 1
        );
        (false, Some(nodes.get(parent).unwrap().pubkey()), layer)
    }
}

impl<T> ClusterNodesCache<T> {
    pub fn new(
        // Capacity of underlying LRU-cache in terms of number of epochs.
        cap: usize,
        // A time-to-live eviction policy is enforced to refresh entries in
        // case gossip contact-infos are updated.
        ttl: Duration,
    ) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            ttl,
        }
    }
}

impl<T: 'static> ClusterNodesCache<T> {
    fn get_cache_entry(&self, epoch: Epoch) -> Arc<Mutex<CacheEntry<T>>> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(&epoch) {
            Some(entry) => Arc::clone(entry),
            None => {
                let entry = Arc::default();
                cache.put(epoch, Arc::clone(&entry));
                entry
            }
        }
    }

    pub(crate) fn get(
        &self,
        shred_slot: Slot,
        root_bank: &Bank,
        working_bank: &Bank,
        cluster_info: &ClusterInfo,
    ) -> Arc<ClusterNodes<T>> {
        let epoch = root_bank.get_leader_schedule_epoch(shred_slot);
        let entry = self.get_cache_entry(epoch);
        // Hold the lock on the entry here so that, if needed, only
        // one thread recomputes cluster-nodes for this epoch.
        let mut entry = entry.lock().unwrap();
        if let Some((asof, nodes)) = entry.deref() {
            if asof.elapsed() < self.ttl {
                return Arc::clone(nodes);
            }
        }
        let epoch_staked_nodes = [root_bank, working_bank]
            .iter()
            .find_map(|bank| bank.epoch_staked_nodes(epoch));
        if epoch_staked_nodes.is_none() {
            inc_new_counter_info!("cluster_nodes-unknown_epoch_staked_nodes", 1);
            if epoch != root_bank.get_leader_schedule_epoch(root_bank.slot()) {
                return self.get(root_bank.slot(), root_bank, working_bank, cluster_info);
            }
            inc_new_counter_info!("cluster_nodes-unknown_epoch_staked_nodes_root", 1);
        }
        let nodes = Arc::new(new_cluster_nodes::<T>(
            cluster_info,
            &epoch_staked_nodes.unwrap_or_default(),
        ));
        *entry = Some((Instant::now(), Arc::clone(&nodes)));
        nodes
    }
}

impl From<ContactInfo> for NodeId {
    fn from(node: ContactInfo) -> Self {
        NodeId::ContactInfo(node)
    }
}

impl From<Pubkey> for NodeId {
    fn from(pubkey: Pubkey) -> Self {
        NodeId::Pubkey(pubkey)
    }
}

pub fn make_test_cluster<R: Rng>(
    rng: &mut R,
    num_nodes: usize,
    unstaked_ratio: Option<(u32, u32)>,
) -> (
    Vec<ContactInfo>,
    HashMap<Pubkey, u64>, // stakes
    ClusterInfo,
) {
    let (unstaked_numerator, unstaked_denominator) = unstaked_ratio.unwrap_or((1, 7));
    let mut nodes: Vec<_> = repeat_with(|| ContactInfo::new_rand(rng, None))
        .take(num_nodes)
        .collect();
    nodes.shuffle(rng);
    let this_node = nodes[0].clone();
    let mut stakes: HashMap<Pubkey, u64> = nodes
        .iter()
        .filter_map(|node| {
            if rng.gen_ratio(unstaked_numerator, unstaked_denominator) {
                None // No stake for some of the nodes.
            } else {
                Some((node.id, rng.gen_range(0, 20)))
            }
        })
        .collect();
    // Add some staked nodes with no contact-info.
    stakes.extend(repeat_with(|| (Pubkey::new_unique(), rng.gen_range(0, 20))).take(100));
    let cluster_info = ClusterInfo::new(
        this_node,
        Arc::new(Keypair::new()),
        SocketAddrSpace::Unspecified,
    );
    {
        let now = timestamp();
        let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
        // First node is pushed to crds table by ClusterInfo constructor.
        for node in nodes.iter().skip(1) {
            let node = CrdsData::ContactInfo(node.clone());
            let node = CrdsValue::new_unsigned(node);
            assert_eq!(
                gossip_crds.insert(node, now, GossipRoute::LocalMessage),
                Ok(())
            );
        }
    }
    (nodes, stakes, cluster_info)
}

fn drop_redundant_turbine_path(shred_slot: Slot, root_bank: &Bank) -> bool {
    let feature_slot = root_bank
        .feature_set
        .activated_slot(&feature_set::drop_redundant_turbine_path::id());
    match feature_slot {
        None => false,
        Some(feature_slot) => {
            let epoch_schedule = root_bank.epoch_schedule();
            let feature_epoch = epoch_schedule.get_epoch(feature_slot);
            let shred_epoch = epoch_schedule.get_epoch(shred_slot);
            feature_epoch < shred_epoch
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        lazy_static::lazy_static,
        queues::*,
        regex::Regex,
        solana_gossip::cluster_info::DATA_PLANE_FANOUT,
        solana_ledger::{leader_schedule::LeaderSchedule, shred::ShredFlags},
        solana_runtime::{
            bank::BankFieldsToDeserialize, snapshot_utils::bank_fields_from_snapshot_archives,
        },
        solana_sdk::epoch_schedule::EpochSchedule,
        std::{
            fs::File,
            io::{BufRead, BufReader},
            ops::Index,
            path::Path,
            str::FromStr,
        },
        substring::Substring,
    };

    #[macro_export]
    macro_rules! socketaddr {
        ($ip:expr, $port:expr) => {
            std::net::SocketAddr::from((std::net::Ipv4Addr::from($ip), $port))
        };
        ($str:expr) => {{
            $str.parse::<std::net::SocketAddr>().unwrap()
        }};
    }

    fn load_slot_shred_map_from_file(slot_to_repair_shred_map: &mut HashMap<u64, Vec<u32>>) {
        let filename = "tv3";
        debug!("Repair log file = {}", filename);
        let file = File::open(filename).expect("Something went wrong reading the file");
        let reader = BufReader::new(file);
        static SHRED_REPAIR_STRING: &'static str = "all shred repairs requested:";
        for line in reader.lines() {
            let line = line.unwrap();
            if line.find(SHRED_REPAIR_STRING).is_some() {
                trace!("FOUND: {}", line);
                lazy_static! {
                    static ref PARSING_SLOTS_SHREDS_REGEX: Regex =
                        Regex::new(r"\{[0-9a-zA-Z,:{} ]*}}").unwrap();
                    static ref PARSING_SLOT_SHRED_REGEX: Regex =
                        Regex::new(r"[0-9][0-9a-zA-Z,:{ ]*}").unwrap();
                    static ref PARSING_SLOT_REGEX: Regex = Regex::new(r"[0-9]*:").unwrap();
                    static ref PARSING_SHRED_REGEX: Regex = Regex::new(r"[0-9]*[,}]").unwrap();
                }

                PARSING_SLOTS_SHREDS_REGEX
                    .find_iter(&line)
                    .for_each(|slots_shreds| {
                        let slots_shreds = slots_shreds.as_str();
                        trace!("SLOTS SHREDS: {}", slots_shreds);

                        PARSING_SLOT_SHRED_REGEX
                            .find_iter(&slots_shreds)
                            .for_each(|slot_shred| {
                                let slot_shred = slot_shred.as_str();
                                trace!("SLOT SHRED: {}", slot_shred);

                                let slot: Vec<_> = PARSING_SLOT_REGEX
                                    .find_iter(&slot_shred)
                                    .map(|mat| {
                                        let slot = mat.as_str();
                                        let slot: u64 =
                                            slot.substring(0, slot.len() - 1).parse().unwrap();
                                        trace!("SLOT: {}", slot);
                                        slot
                                    })
                                    .collect();

                                let shred_vec: Vec<_> = PARSING_SHRED_REGEX
                                    .find_iter(&slot_shred)
                                    .map(|mat| {
                                        let shred = mat.as_str();
                                        let shred: u32 =
                                            shred.substring(0, shred.len() - 1).parse().unwrap();
                                        trace!("SHRED: {}", shred);
                                        shred
                                    })
                                    .collect();

                                slot_to_repair_shred_map.insert(slot[0], shred_vec);
                            });
                    });
            }
        }
    }

    fn get_bank_info() -> BankFieldsToDeserialize {
        let snapshot_path = Path::new("./ledger");
        bank_fields_from_snapshot_archives(snapshot_path).expect("Failed to read snapshot")
    }

    fn get_cluster_info(
        bank_info: &BankFieldsToDeserialize,
    ) -> (Pubkey, ClusterInfo, EpochSchedule) {
        let my_pubkey = Pubkey::from_str("5D1fNXzvv5NjV1ysLjirC4WY92RNsVH18vjmcszZd8on").unwrap();
        let this_node =
            ContactInfo::new_with_pubkey_socketaddr(&my_pubkey, &socketaddr!("127.0.0.1:1234"));
        let cluster_info = ClusterInfo::new(
            this_node,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );

        (my_pubkey, cluster_info, bank_info.epoch_schedule)
    }

    fn get_cluster_nodes(
        cluster_info: &ClusterInfo,
        bank_info: &BankFieldsToDeserialize,
        epoch: Epoch,
    ) -> ClusterNodes<RetransmitStage> {
        let staked_nodes = bank_info.epoch_stakes.index(&epoch).stakes().staked_nodes();
        debug!("Staked nodes for epoch {}: {:?}", epoch, staked_nodes);
        new_cluster_nodes::<RetransmitStage>(&cluster_info, staked_nodes.as_ref())
    }

    fn get_leader_schedule(bank_info: &BankFieldsToDeserialize, epoch: Epoch) -> LeaderSchedule {
        let leader_stake_nodes = bank_info.epoch_stakes.index(&epoch).stakes().staked_nodes();
        solana_ledger::leader_schedule_utils::leader_schedule2(
            epoch,
            Some(leader_stake_nodes.as_ref().clone()),
        )
        .unwrap()
    }

    #[test]
    fn trace_turbine_paths() {
        solana_logger::setup();
        const NUM_LAYERS: usize = 3;
        let bank_info = get_bank_info();
        let (my_pubkey, cluster_info, epoch_schedule) = get_cluster_info(&bank_info);
        let mut slot_to_repair_shred_map: HashMap<u64, Vec<u32>> = HashMap::new();
        load_slot_shred_map_from_file(&mut slot_to_repair_shred_map);
        let mut node_to_shred_count: HashMap<Pubkey, u64> = HashMap::new(); // Number of shreds each node would have been somewhere in the path for for our node where we had to repair that shred
        let mut node_to_shred_count_1hop: HashMap<Pubkey, u64> = HashMap::new(); // Number of shreds each node would have been directly responsible for sending to our node where we had to repair that shred
        let mut node_to_shred_count_total: HashMap<Pubkey, u64> = HashMap::new(); // Number of shreds each node would have been directly responsible for sending to our node
        let mut layer_counts = vec![0; NUM_LAYERS];
        let mut layer_counts_total = vec![0; NUM_LAYERS];
        let mut layer_counts_prev = vec![0; NUM_LAYERS];
        let mut layer_counts_total_prev = vec![0; NUM_LAYERS];
        for (slot, shreds) in slot_to_repair_shred_map {
            let stake_epoch = epoch_schedule.get_leader_schedule_epoch(slot);
            let cluster_nodes = get_cluster_nodes(&cluster_info, &bank_info, stake_epoch);
            let (epoch, slot_offset) = epoch_schedule.get_epoch_and_slot_index(slot);
            let leader_schedule = get_leader_schedule(&bank_info, epoch);
            let leader_pubkey = leader_schedule.index(slot_offset);
            info!(
                "Leader is {} for slot {} (epoch {}, offset {}): shreds {:?}",
                *leader_pubkey, slot, epoch, slot_offset, shreds
            );
            let mut highest_shred = 0;
            for shred_idx in shreds {
                highest_shred = highest_shred.max(shred_idx);
                let shred = Shred::new_from_data(
                    slot,
                    shred_idx,
                    0,
                    &[],
                    ShredFlags::LAST_SHRED_IN_SLOT,
                    0,
                    0,
                    3,
                );

                let mut q: Queue<Pubkey> = queue![];
                q.add(my_pubkey).expect("Failed to add pubkey to queue");
                while q.size() > 0 {
                    if let Ok(pubkey) = q.remove() {
                        debug!("I am {}. Looking at shred {}", pubkey, shred_idx);
                        let ((is_root, parent, layer), anchor) = cluster_nodes
                            .get_retransmit_ancestors(
                                *leader_pubkey,
                                &shred,
                                DATA_PLANE_FANOUT,
                                pubkey,
                            );

                        if pubkey == my_pubkey {
                            layer_counts[layer] += 1;
                            if let Some(parent) = parent {
                                let values = node_to_shred_count_1hop.entry(parent).or_insert(0);
                                *values += 1;
                            } else {
                                let values =
                                    node_to_shred_count_1hop.entry(*leader_pubkey).or_insert(0);
                                *values += 1;
                            }

                            if let Some(anchor) = anchor {
                                let values = node_to_shred_count_1hop.entry(anchor).or_insert(0);
                                *values += 1;
                            }
                        }

                        if let Some(parent) = parent {
                            let values = node_to_shred_count.entry(parent).or_insert(0);
                            *values += 1;
                            if !is_root {
                                q.add(parent).expect("Failed to add pubkey to queue");
                            }
                        } else {
                            let values = node_to_shred_count.entry(*leader_pubkey).or_insert(0);
                            *values += 1;
                        }

                        if let Some(anchor) = anchor {
                            let values = node_to_shred_count.entry(anchor).or_insert(0);
                            *values += 1;
                            q.add(anchor).expect("Failed to add pubkey to queue");
                        }
                    }
                }
            }

            info!(
                "slot {} highest shred is {} (best guess)",
                slot, highest_shred
            );
            for i in 0..=highest_shred {
                let shred = Shred::new_from_data(
                    slot,
                    i as u32,
                    0,
                    &[],
                    ShredFlags::LAST_SHRED_IN_SLOT,
                    0,
                    0,
                    3,
                );

                let ((_, parent, layer), anchor) = cluster_nodes.get_retransmit_ancestors(
                    *leader_pubkey,
                    &shred,
                    DATA_PLANE_FANOUT,
                    my_pubkey,
                );

                layer_counts_total[layer] += 1;
                if let Some(parent) = parent {
                    let values = node_to_shred_count_total.entry(parent).or_insert(0);
                    *values += 1;
                } else {
                    let values = node_to_shred_count_total.entry(*leader_pubkey).or_insert(0);
                    *values += 1;
                }

                if let Some(anchor) = anchor {
                    let values = node_to_shred_count_total.entry(anchor).or_insert(0);
                    *values += 1;
                }
            }

            println!("{} {} {} {}", leader_pubkey, slot, layer_counts[0]-layer_counts_prev[0], layer_counts_total[0] - layer_counts_total_prev[0]);
            for layer in 0..NUM_LAYERS {
                let percent_failed =
                    (layer_counts[layer]-layer_counts_prev[layer]) as f64 * 100 as f64 / (layer_counts_total[layer] - layer_counts_total_prev[layer]) as f64;
                info!(
                    "Slot {} Layer {} had to repair {} out of {} ({}%) shreds",
                    slot, layer, layer_counts[layer]-layer_counts_prev[layer], layer_counts_total[layer] - layer_counts_total_prev[layer], percent_failed,
                );
                layer_counts_prev[layer] = layer_counts[layer];
                layer_counts_total_prev[layer] = layer_counts_total[layer];
            }
        }

        for (node, shred) in node_to_shred_count {
            println!(
                "Node {} was in the path for transmitting me {} shreds that needed to be repaired",
                node, shred
            );
        }
        for (node, shred) in node_to_shred_count_total {
            let failed = node_to_shred_count_1hop.entry(node).or_default();
            let percent_failed = *failed as f64 * 100 as f64 / shred as f64;
            println!(
                "Connected Node {} possibly failed {} out of {} ({}%) shreds",
                node, failed, shred, percent_failed,
            );
        }
        for layer in 0..NUM_LAYERS {
            let percent_failed =
                layer_counts[layer] as f64 * 100 as f64 / layer_counts_total[layer] as f64;
            println!(
                "Layer {} had to repair {} out of {} ({}%) shreds",
                layer, layer_counts[layer], layer_counts_total[layer], percent_failed,
            );
        }
        println!("*** Note above percentage may be high because total shreds for given slot is not known (guessed based on highest repaired index) ***")
    }

    #[test]
    fn test_cluster_nodes_retransmit() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_test_cluster(&mut rng, 1_000, None);
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = new_cluster_nodes::<RetransmitStage>(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
    }

    #[test]
    fn test_cluster_nodes_broadcast() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_test_cluster(&mut rng, 1_000, None);
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = ClusterNodes::<BroadcastStage>::new(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        // Excluding this node itself.
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
    }

    #[test]
    fn test_get_retransmit_peers() {
        // fanout 2
        let index = vec![
            7, // root
            6, 10, // 1st layer
            // 2nd layer
            5, 19, // 1st neighborhood
            0, 14, // 2nd
            // 3rd layer
            3, 1, // 1st neighborhood
            12, 2, // 2nd
            11, 4, // 3rd
            15, 18, // 4th
            // 4th layer
            13, 16, // 1st neighborhood
            17, 9, // 2nd
            8, // 3rd
        ];
        let peers = vec![
            vec![6, 10],
            vec![5, 0],
            vec![19, 14],
            vec![3, 12],
            vec![1, 2],
            vec![11, 15],
            vec![4, 18],
            vec![13, 17],
            vec![16, 9],
            vec![8],
        ];
        for (k, peers) in peers.into_iter().enumerate() {
            let retransmit_peers = get_retransmit_peers(/*fanout:*/ 2, k, &index);
            assert_eq!(retransmit_peers.collect::<Vec<_>>(), peers);
        }
        for k in 10..=index.len() {
            let mut retransmit_peers = get_retransmit_peers(/*fanout:*/ 2, k, &index);
            assert_eq!(retransmit_peers.next(), None);
        }
        // fanout 3
        let index = vec![
            19, // root
            14, 15, 28, // 1st layer
            // 2nd layer
            29, 4, 5, // 1st neighborhood
            9, 16, 7, // 2nd
            26, 23, 2, // 3rd
            // 3rd layer
            31, 3, 17, // 1st neighborhood
            20, 25, 0, // 2nd
            13, 30, 18, // 3rd
            35, 21, 22, // 4th
            6, 8, 11, // 5th
            27, 1, 10, // 6th
            12, 24, 34, // 7th
            33, 32, // 8th
        ];
        let peers = vec![
            vec![14, 15, 28],
            vec![29, 9, 26],
            vec![4, 16, 23],
            vec![5, 7, 2],
            vec![31, 20, 13],
            vec![3, 25, 30],
            vec![17, 0, 18],
            vec![35, 6, 27],
            vec![21, 8, 1],
            vec![22, 11, 10],
            vec![12, 33],
            vec![24, 32],
            vec![34],
        ];
        for (k, peers) in peers.into_iter().enumerate() {
            let retransmit_peers = get_retransmit_peers(/*fanout:*/ 3, k, &index);
            assert_eq!(retransmit_peers.collect::<Vec<_>>(), peers);
        }
        for k in 13..=index.len() {
            let mut retransmit_peers = get_retransmit_peers(/*fanout:*/ 3, k, &index);
            assert_eq!(retransmit_peers.next(), None);
        }
    }
}

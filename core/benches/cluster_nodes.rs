#![feature(test)]

extern crate test;

use {
    rand::{seq::SliceRandom, Rng},
    solana_core::{
        cluster_nodes::{make_test_cluster, new_cluster_nodes, ClusterNodes},
        retransmit_stage::RetransmitStage,
    },
    solana_gossip::legacy_contact_info::LegacyContactInfo as ContactInfo,
    solana_ledger::{
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
        shred::{Shred, ShredFlags},
    },
    solana_runtime::bank::Bank,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    test::Bencher,
};

const NUM_SIMULATED_SHREDS: usize = 10_000;

fn make_cluster_nodes<R: Rng>(
    rng: &mut R,
    unstaked_ratio: Option<(u32, u32)>,
) -> (Vec<ContactInfo>, ClusterNodes<RetransmitStage>) {
    let (nodes, stakes, cluster_info) = make_test_cluster(rng, 3_000, unstaked_ratio);
    let cluster_nodes = new_cluster_nodes::<RetransmitStage>(&cluster_info, &stakes);
    (nodes, cluster_nodes)
}

fn get_retransmit_peers_deterministic(
    cluster_nodes: &ClusterNodes<RetransmitStage>,
    slot: Slot,
    slot_leader: &Pubkey,
    root_bank: &Bank,
    num_simulated_shreds: usize,
) {
    let parent_offset = u16::from(slot != 0);
    let mut distance = vec![0;4];
    for i in 0..num_simulated_shreds {
        let index = i as u32;
        let shred = Shred::new_from_data(
            slot,
            index,
            parent_offset,
            &[],
            ShredFlags::empty(),
            0,
            0,
            0,
        );
        let retransmit_peers = cluster_nodes.get_retransmit_peers(
            slot_leader,
            &shred.id(),
            root_bank,
            solana_gossip::cluster_info::DATA_PLANE_FANOUT,
        ).unwrap();
        if retransmit_peers.neighbors[0].pubkey() == cluster_nodes.pubkey && retransmit_peers.root_distance > 0 {
            // This is a layer 2 anchor node
            assert_eq!(retransmit_peers.root_distance, 2);
            distance[3] += 1;
        } else {
            distance[retransmit_peers.root_distance] += 1;
        }
    }
    println!("{:?}", distance);
}

fn get_retransmit_peers_deterministic_wrapper(b: &mut Bencher, unstaked_ratio: Option<(u32, u32)>) {
    b.iter(|| {
        let mut rng = rand::thread_rng();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_benches(&genesis_config);
        let (nodes, cluster_nodes) = make_cluster_nodes(&mut rng, unstaked_ratio);
        let mut total_stake = 0;
        let mut my_stake = 0;
        for node in &cluster_nodes.nodes {
            match &node.node {
                solana_core::cluster_nodes::NodeId::ContactInfo(info) => {
                    if info.id == cluster_nodes.pubkey {
                        my_stake = node.stake;
                    }
                    total_stake += node.stake;
                }
                _ => (),
            }
        }
        println!("looking at root distance for {:?} with stake {}/{} = {:.2}%", cluster_nodes.pubkey, my_stake, total_stake, ((my_stake as f64) / (total_stake as f64))*100.0);
        let slot_leader = nodes[1..].choose(&mut rng).unwrap().id;
        let slot = rand::random::<u64>();
        get_retransmit_peers_deterministic(
            &cluster_nodes,
            slot,
            &slot_leader,
            &bank,
            NUM_SIMULATED_SHREDS,
        )
    });
}

#[bench]
fn bench_get_retransmit_peers_deterministic_unstaked_ratio_1_2(b: &mut Bencher) {
    get_retransmit_peers_deterministic_wrapper(b, Some((1, 2)));
}

#[bench]
fn bench_get_retransmit_peers_deterministic_unstaked_ratio_1_32(b: &mut Bencher) {
    get_retransmit_peers_deterministic_wrapper(b, Some((1, 32)));
}

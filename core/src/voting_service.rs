use {
    crate::tower_storage::{SavedTowerVersions, TowerStorage},
    chrono::prelude::*,
    crossbeam_channel::Receiver,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure::Measure,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{clock::Slot, transaction::Transaction},
    std::{
        sync::{Arc, Mutex, RwLock},
        thread::{self, Builder, JoinHandle},
    },
};

pub enum VoteOp {
    PushVote {
        tx: Transaction,
        tower_slots: Vec<Slot>,
        saved_tower: SavedTowerVersions,
    },
    RefreshVote {
        tx: Transaction,
        last_voted_slot: Slot,
    },
}

impl VoteOp {
    fn tx(&self) -> &Transaction {
        match self {
            VoteOp::PushVote { tx, .. } => tx,
            VoteOp::RefreshVote { tx, .. } => tx,
        }
    }
}

pub struct VotingService {
    thread_hdl: JoinHandle<()>,
}

impl VotingService {
    pub fn new(
        vote_receiver: Receiver<VoteOp>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<Mutex<PohRecorder>>,
        tower_storage: Arc<dyn TowerStorage>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("sol-vote-service".to_string())
            .spawn(move || {
                for vote_op in vote_receiver.iter() {
                    let rooted_bank = bank_forks.read().unwrap().root_bank().clone();
                    let send_to_tpu_vote_port = rooted_bank.send_to_tpu_vote_port_enabled();
                    Self::handle_vote(
                        &cluster_info,
                        &poh_recorder,
                        tower_storage.as_ref(),
                        vote_op,
                        send_to_tpu_vote_port,
                    );
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn handle_vote(
        cluster_info: &ClusterInfo,
        poh_recorder: &Mutex<PohRecorder>,
        tower_storage: &dyn TowerStorage,
        vote_op: VoteOp,
        send_to_tpu_vote_port: bool,
    ) {
        if let VoteOp::PushVote { saved_tower, .. } = &vote_op {
            let mut measure = Measure::start("tower_save-ms");
            if let Err(err) = tower_storage.store(saved_tower) {
                error!("Unable to save tower to storage: {:?}", err);
                std::process::exit(1);
            }
            measure.stop();
            inc_new_counter_info!("tower_save-ms", measure.as_ms() as usize);
        }
        let time_now = Utc::now().timestamp_nanos() as u64;
        let message = vote_op.tx().message();
        let poh_recorder_lock = poh_recorder.lock().unwrap();
        warn!("{:?} Voting: hash={:?}, key0={:?}, key1={:?}, leader0={:?}, leader1={:?}, leader2={:?}, send_to_tpu_vote_port={:?}",
            time_now,
            message.recent_blockhash,
            message.account_keys[0],
            message.account_keys[1],
            poh_recorder_lock.leader_after_n_slots(0),
            poh_recorder_lock.leader_after_n_slots(1),
            poh_recorder_lock.leader_after_n_slots(2),
            send_to_tpu_vote_port,
        );
        drop(poh_recorder_lock);
        let target_address = if send_to_tpu_vote_port {
            crate::banking_stage::next_leader_tpu_vote(cluster_info, poh_recorder)
        } else {
            crate::banking_stage::next_leader_tpu(cluster_info, poh_recorder)
        };
        let _ = cluster_info.send_transaction(vote_op.tx(), target_address);

        match vote_op {
            VoteOp::PushVote {
                tx, tower_slots, ..
            } => {
                cluster_info.push_vote(&tower_slots, tx);
            }
            VoteOp::RefreshVote {
                tx,
                last_voted_slot,
            } => {
                cluster_info.refresh_vote(tx, last_voted_slot);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

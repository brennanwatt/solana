//! Simple scheduler that drops network packets and generates transactions.
//!     - this is useful for testing the banking stage without the network
//!       or in creating stress-tests on a local network.

use {
    super::{
        consume_banking_worker::{FinishedWork, ScheduledWork},
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        TransactionGenerator,
    },
    crate::{
        banking_trace::BankingPacketReceiver,
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
    },
    crossbeam_channel::{Receiver, Sender},
    std::sync::{atomic::AtomicBool, Arc},
};

pub struct TestScheduler {
    /// Decision maker - only generate when leader
    decision_maker: DecisionMaker,
    /// From SigVerify - ignored
    _dummy_receiver: BankingPacketReceiver,
    /// To BankingStageWorker
    sender: Sender<ScheduledWork>,
    /// From BankingStageWorker
    _receiver: Receiver<FinishedWork>,
    /// Transaction batch generator
    transaction_generator: Vec<TransactionGenerator>,
}

impl TestScheduler {
    pub fn new(
        decision_maker: DecisionMaker,
        dummy_receiver: BankingPacketReceiver,
        sender: Sender<ScheduledWork>,
        receiver: Receiver<FinishedWork>,
        transaction_generator: Vec<TransactionGenerator>,
    ) -> Self {
        Self {
            decision_maker,
            _dummy_receiver: dummy_receiver,
            sender,
            _receiver: receiver,
            transaction_generator,
        }
    }

    pub fn run(mut self, exit: &Arc<AtomicBool>) {
        let mut slot_metrics_tracker = LeaderSlotMetricsTracker::new(0);
        let mut rng = rand::thread_rng();
        let num_generators = self.transaction_generator.len();
        let mut bank_slot = 0;
        let mut generator_idx = 0;
        loop {
            if exit.load(std::sync::atomic::Ordering::Relaxed) {
                debug!("TestScheduler exiting");
                break;
            }
            let (_action, decision) = self
                .decision_maker
                .make_consume_or_forward_decision(&mut slot_metrics_tracker);
            if let BufferedPacketsDecision::Consume(bank_start) = &decision {
                // Create batches of transactions for consumer threads
                if bank_start.working_bank.slot() != bank_slot {
                    bank_slot = bank_start.working_bank.slot();
                    generator_idx = (generator_idx + 1) % num_generators;
                    trace!(
                        "Generating transactions for slot {bank_slot} w/ generator {generator_idx}"
                    );
                }
                for _ in 0..100 {
                    let transactions = (self.transaction_generator[generator_idx])(
                        &mut rng,
                        &bank_start.working_bank,
                    );
                    let scheduled_work = ScheduledWork {
                        decision: decision.clone(),
                        transactions,
                    };
                    self.sender.send(scheduled_work).unwrap();
                }
            }
        }
    }
}

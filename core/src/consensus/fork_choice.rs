use {
    super::heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
    crate::{
        consensus::{
            latest_validator_votes_for_frozen_banks::LatestValidatorVotesForFrozenBanks,
            progress_map::ProgressMap, SwitchForkDecision, ThresholdDecision, Tower,
            SWITCH_FORK_THRESHOLD,
        },
        replay_stage::HeaviestForkFailures,
    },
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_sdk::clock::Slot,
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, RwLock},
    },
};

pub struct SelectVoteAndResetForkResult {
    pub vote_bank: Option<(Arc<Bank>, SwitchForkDecision)>,
    pub reset_bank: Option<Arc<Bank>>,
    pub heaviest_fork_failures: Vec<HeaviestForkFailures>,
}

struct CandidateVoteAndResetBanks<'a> {
    // A bank that the validator will vote on given it passes all
    // remaining vote checks
    candidate_vote_bank: Option<&'a Arc<Bank>>,
    // A bank that the validator will reset its PoH to regardless
    // of voting behavior
    reset_bank: Option<&'a Arc<Bank>>,
    switch_fork_decision: SwitchForkDecision,
}

pub trait ForkChoice {
    type ForkChoiceKey;
    fn compute_bank_stats(
        &mut self,
        bank: &Bank,
        tower: &Tower,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    );

    // Returns:
    // 1) The heaviest overall bank
    // 2) The heaviest bank on the same fork as the last vote (doesn't require a
    // switching proof to vote for)
    fn select_forks(
        &self,
        frozen_banks: &[Arc<Bank>],
        tower: &Tower,
        progress: &ProgressMap,
        ancestors: &HashMap<u64, HashSet<u64>>,
        bank_forks: &RwLock<BankForks>,
    ) -> (Arc<Bank>, Option<Arc<Bank>>);

    fn mark_fork_invalid_candidate(&mut self, invalid_slot: &Self::ForkChoiceKey);

    /// Returns any newly duplicate confirmed ancestors of `valid_slot` up to and including
    /// `valid_slot` itself
    fn mark_fork_valid_candidate(
        &mut self,
        valid_slot: &Self::ForkChoiceKey,
    ) -> Vec<Self::ForkChoiceKey>;
}

fn last_vote_able_to_land(
    reset_bank: Option<&Bank>,
    progress: &ProgressMap,
    tower: &Tower,
) -> bool {
    let Some(heaviest_bank_on_same_voted_fork) = reset_bank else {
        return true;
    };

    let Some(last_voted_slot) = tower.last_voted_slot() else {
        // No previous vote.
        return true;
    };

    let Some(my_latest_landed_vote_slot) =
        progress.my_latest_landed_vote(heaviest_bank_on_same_voted_fork.slot())
    else {
        return true;
    };

    // Last vote did not land. Now let's check if it's able to land.
    my_latest_landed_vote_slot >= last_voted_slot
        // If we are already voting at the tip, there is nothing we can do.
        || last_voted_slot >= heaviest_bank_on_same_voted_fork.slot()
        // Last vote outside slot hashes of the tip of fork
        || heaviest_bank_on_same_voted_fork
            .is_in_slot_hashes_history(&last_voted_slot)
}

fn select_forks_failed_switch_threshold(
    reset_bank: Option<&Bank>,
    progress: &ProgressMap,
    tower: &Tower,
    heaviest_bank_slot: Slot,
    failure_reasons: &mut Vec<HeaviestForkFailures>,
    switch_proof_stake: u64,
    total_stake: u64,
    switch_fork_decision: SwitchForkDecision,
) -> SwitchForkDecision {
    if !last_vote_able_to_land(reset_bank, progress, tower) {
        // If we reach here, these assumptions are true:
        // 1. We can't switch because of threshold
        // 2. Our last vote was on a non-duplicate/confirmed slot
        // 3. Our last vote is now outside slot hashes history of the tip of fork
        // So, there was no hope of this last vote ever landing again.

        // In this case, we do want to obey threshold, yet try to register our vote on
        // the current fork, so we choose to vote at the tip of current fork instead.
        // This will not cause longer lockout because lockout doesn't double after 512
        // slots, it might be enough to get majority vote.
        return SwitchForkDecision::SameFork;
    }

    // If we can't switch and our last vote was on a non-duplicate/confirmed slot, then
    // reset to the the next votable bank on the same fork as our last vote,
    // but don't vote.

    // We don't just reset to the heaviest fork when switch threshold fails because
    // a situation like this can occur:

    /* Figure 1:
                slot 0
                    |
                slot 1
                /        \
    slot 2 (last vote)     |
                |      slot 8 (10%)
        slot 4 (9%)
    */

    // Imagine 90% of validators voted on slot 4, but only 9% landed. If everybody that fails
    // the switch threshold abandons slot 4 to build on slot 8 (because it's *currently* heavier),
    // then there will be no blocks to include the votes for slot 4, and the network halts
    // because 90% of validators can't vote
    info!(
        "Waiting to switch vote to {},
        resetting to slot {:?} for now,
        switch proof stake: {},
        threshold stake: {},
        total stake: {}",
        heaviest_bank_slot,
        reset_bank.as_ref().map(|b| b.slot()),
        switch_proof_stake,
        total_stake as f64 * SWITCH_FORK_THRESHOLD,
        total_stake
    );
    failure_reasons.push(HeaviestForkFailures::FailedSwitchThreshold(
        heaviest_bank_slot,
        switch_proof_stake,
        total_stake,
    ));
    switch_fork_decision
}

fn select_candidate_vote_and_reset_banks<'a>(
    heaviest_bank: &'a Arc<Bank>,
    heaviest_bank_on_same_voted_fork: Option<&'a Arc<Bank>>,
    progress: &'a ProgressMap,
    tower: &'a Tower,
    failure_reasons: &mut Vec<HeaviestForkFailures>,
    initial_switch_fork_decision: SwitchForkDecision,
) -> CandidateVoteAndResetBanks<'a> {
    match initial_switch_fork_decision {
        SwitchForkDecision::FailedSwitchThreshold(switch_proof_stake, total_stake) => {
            // Even though we think we want to switch to a new, heavier fork, we
            // actually want to continue voting on our current fork if our last
            // vote was unable land.
            let final_switch_fork_decision = select_forks_failed_switch_threshold(
                heaviest_bank_on_same_voted_fork.map(|bank| bank.as_ref()),
                progress,
                tower,
                heaviest_bank.slot(),
                failure_reasons,
                switch_proof_stake,
                total_stake,
                initial_switch_fork_decision,
            );
            let candidate_vote_bank = if final_switch_fork_decision.can_vote() {
                heaviest_bank_on_same_voted_fork
            } else {
                Some(heaviest_bank)
            };
            CandidateVoteAndResetBanks {
                candidate_vote_bank,
                reset_bank: heaviest_bank_on_same_voted_fork,
                switch_fork_decision: final_switch_fork_decision,
            }
        }
        SwitchForkDecision::FailedSwitchDuplicateRollback(latest_duplicate_ancestor) => {
            let reset_bank = Some(heaviest_bank);
            info!(
                "Waiting to switch vote to {}, resetting to slot {:?} for now, latest duplicate ancestor: {:?}",
                heaviest_bank.slot(),
                reset_bank.as_ref().map(|b| b.slot()),
                latest_duplicate_ancestor,
            );
            failure_reasons.push(HeaviestForkFailures::FailedSwitchThreshold(
                heaviest_bank.slot(),
                0, // In this case we never actually performed the switch check, 0 for now
                0,
            ));
            CandidateVoteAndResetBanks {
                candidate_vote_bank: None,
                reset_bank,
                switch_fork_decision: initial_switch_fork_decision,
            }
        }
        _ => CandidateVoteAndResetBanks {
            candidate_vote_bank: Some(heaviest_bank),
            reset_bank: Some(heaviest_bank),
            switch_fork_decision: initial_switch_fork_decision,
        },
    }
}

// Checks for all possible reasons we might not be able to vote on the candidate
// bank. Records any failure reasons, and doesn't early return so we can be sure
// to record all possible reasons.
fn can_vote_on_candidate_bank(
    candidate_vote_bank_slot: Slot,
    progress: &ProgressMap,
    tower: &Tower,
    failure_reasons: &mut Vec<HeaviestForkFailures>,
    switch_fork_decision: &SwitchForkDecision,
) -> bool {
    let (
        is_locked_out,
        vote_thresholds,
        propagated_stake,
        is_leader_slot,
        fork_weight,
        total_threshold_stake,
        total_epoch_stake,
    ) = {
        let fork_stats = progress.get_fork_stats(candidate_vote_bank_slot).unwrap();
        let propagated_stats = &progress
            .get_propagated_stats(candidate_vote_bank_slot)
            .unwrap();
        (
            fork_stats.is_locked_out,
            &fork_stats.vote_threshold,
            propagated_stats.propagated_validators_stake,
            propagated_stats.is_leader_slot,
            fork_stats.fork_weight(),
            fork_stats.total_stake,
            propagated_stats.total_epoch_stake,
        )
    };

    // Check if we are locked out.
    if is_locked_out {
        failure_reasons.push(HeaviestForkFailures::LockedOut(candidate_vote_bank_slot));
    }

    // Check if we failed any of the vote thresholds.
    let mut threshold_passed = true;
    for threshold_failure in vote_thresholds {
        let &ThresholdDecision::FailedThreshold(vote_depth, fork_stake) = threshold_failure else {
            continue;
        };
        failure_reasons.push(HeaviestForkFailures::FailedThreshold(
            candidate_vote_bank_slot,
            vote_depth,
            fork_stake,
            total_threshold_stake,
        ));
        // Ignore shallow checks for voting purposes
        if (vote_depth as usize) >= tower.threshold_depth {
            threshold_passed = false;
        }
    }

    // Check if our last leader slot has been propagated.
    // If we reach here, the candidate_vote_bank exists in the bank_forks, so it isn't
    // dumped and should exist in progress map.
    let propagation_confirmed = is_leader_slot
        || progress
            .get_leader_propagation_slot_must_exist(candidate_vote_bank_slot)
            .0;
    if !propagation_confirmed {
        failure_reasons.push(HeaviestForkFailures::NoPropagatedConfirmation(
            candidate_vote_bank_slot,
            propagated_stake,
            total_epoch_stake,
        ));
    }

    if !is_locked_out
        && threshold_passed
        && propagation_confirmed
        && switch_fork_decision.can_vote()
    {
        info!(
            "voting: {} {:.1}%",
            candidate_vote_bank_slot,
            100.0 * fork_weight
        );
        true
    } else {
        false
    }
}

/// Given a `heaviest_bank` and a `heaviest_bank_on_same_voted_fork`, return
/// a bank to vote on, a bank to reset to, and a list of switch failure
/// reasons.
///
/// If `heaviest_bank_on_same_voted_fork` is `None` due to that fork no
/// longer being valid to vote on, it's possible that a validator will not
/// be able to reset away from the invalid fork that they last voted on. To
/// resolve this scenario, validators need to wait until they can create a
/// switch proof for another fork or until the invalid fork is be marked
/// valid again if it was confirmed by the cluster.
/// Until this is resolved, leaders will build each of their
/// blocks from the last reset bank on the invalid fork.
pub fn select_vote_and_reset_forks(
    heaviest_bank: &Arc<Bank>,
    // Should only be None if there was no previous vote
    heaviest_bank_on_same_voted_fork: Option<&Arc<Bank>>,
    ancestors: &HashMap<u64, HashSet<u64>>,
    descendants: &HashMap<u64, HashSet<u64>>,
    progress: &ProgressMap,
    tower: &mut Tower,
    latest_validator_votes_for_frozen_banks: &LatestValidatorVotesForFrozenBanks,
    fork_choice: &HeaviestSubtreeForkChoice,
) -> SelectVoteAndResetForkResult {
    // Try to vote on the actual heaviest fork. If the heaviest bank is
    // locked out or fails the threshold check, the validator will:
    // 1) Not continue to vote on current fork, waiting for lockouts to expire/
    //    threshold check to pass
    // 2) Will reset PoH to heaviest fork in order to make sure the heaviest
    //    fork is propagated
    // This above behavior should ensure correct voting and resetting PoH
    // behavior under all cases:
    // 1) The best "selected" bank is on same fork
    // 2) The best "selected" bank is on a different fork,
    //    switch_threshold fails
    // 3) The best "selected" bank is on a different fork,
    //    switch_threshold succeeds
    let initial_switch_fork_decision: SwitchForkDecision = tower.check_switch_threshold(
        heaviest_bank.slot(),
        ancestors,
        descendants,
        progress,
        heaviest_bank.total_epoch_stake(),
        heaviest_bank
            .epoch_vote_accounts(heaviest_bank.epoch())
            .expect("Bank epoch vote accounts must contain entry for the bank's own epoch"),
        latest_validator_votes_for_frozen_banks,
        fork_choice,
    );

    let mut failure_reasons = vec![];
    let CandidateVoteAndResetBanks {
        candidate_vote_bank,
        reset_bank,
        switch_fork_decision,
    } = select_candidate_vote_and_reset_banks(
        heaviest_bank,
        heaviest_bank_on_same_voted_fork,
        progress,
        tower,
        &mut failure_reasons,
        initial_switch_fork_decision,
    );

    let Some(candidate_vote_bank) = candidate_vote_bank else {
        // No viable candidate to vote on.
        return SelectVoteAndResetForkResult {
            vote_bank: None,
            reset_bank: reset_bank.cloned(),
            heaviest_fork_failures: failure_reasons,
        };
    };

    if can_vote_on_candidate_bank(
        candidate_vote_bank.slot(),
        progress,
        tower,
        &mut failure_reasons,
        &switch_fork_decision,
    ) {
        // We can vote!
        SelectVoteAndResetForkResult {
            vote_bank: Some((candidate_vote_bank.clone(), switch_fork_decision)),
            reset_bank: Some(candidate_vote_bank.clone()),
            heaviest_fork_failures: failure_reasons,
        }
    } else {
        // Unable to vote on the candidate bank.
        SelectVoteAndResetForkResult {
            vote_bank: None,
            reset_bank: reset_bank.cloned(),
            heaviest_fork_failures: failure_reasons,
        }
    }
}

//! The `sigverify_stage` implements the signature verification stage of the TPU. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next stage whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if perf-libs are available

use {
    crate::{find_packet_sender_stake_stage, sigverify},
    core::time::Duration,
    crossbeam_channel::{RecvTimeoutError, SendError, Sender},
    itertools::Itertools,
    solana_measure::measure::Measure,
    solana_perf::{
        packet::{Packet, PacketBatch},
        sigverify::{count_valid_packets, shrink_batches, Deduper},
    },
    solana_sdk::timing,
    solana_streamer::streamer::{self, StreamerError},
    std::{
        thread::{self, Builder, JoinHandle},
        time::Instant,
    },
    thiserror::Error,
};

// Try to target 50ms, rough timings from mainnet machines
//
// 50ms/(300ns/packet) = 166666 packets ~ 1300 batches
const MAX_DEDUP_BATCH: usize = 165_000;

// 50ms/(25us/packet) = 2000 packets
const MAX_SIGVERIFY_BATCH: usize = 10_000;

#[derive(Error, Debug)]
pub enum SigVerifyServiceError<SendType> {
    #[error("send packets batch error")]
    Send(#[from] SendError<SendType>),

    #[error("streamer error")]
    Streamer(#[from] StreamerError),
}

type Result<T, SendType> = std::result::Result<T, SigVerifyServiceError<SendType>>;

pub struct SigVerifyStage {
    thread_hdl: JoinHandle<()>,
}

pub struct VerifyFilterStage {
    thread_hdl: JoinHandle<()>,
}

pub trait SigVerifier {
    type SendType: std::fmt::Debug;
    fn verify_batches(&self, batches: Vec<PacketBatch>, valid_packets: usize) -> Vec<PacketBatch>;
    fn process_received_packet(
        &mut self,
        _packet: &mut Packet,
        _removed_before_sigverify_stage: bool,
        _is_dup: bool,
    ) {
    }
    fn process_excess_packet(&mut self, _packet: &Packet) {}
    fn process_passed_sigverify_packet(&mut self, _packet: &Packet) {}
    fn send_packets(&mut self, packet_batches: Vec<PacketBatch>) -> Result<(), Self::SendType>;
}

#[derive(Default, Clone)]
pub struct DisabledSigVerifier {}

#[derive(Default)]
struct SigVerifierStats {
    recv_batches_us_hist: histogram::Histogram, // time to call recv_batch
    verify_batches_pp_us_hist: histogram::Histogram, // per-packet time to call verify_batch
    discard_packets_pp_us_hist: histogram::Histogram, // per-packet time to call verify_batch
    dedup_packets_pp_us_hist: histogram::Histogram, // per-packet time to call verify_batch
    batches_hist: histogram::Histogram,         // number of packet batches per verify call
    packets_hist: histogram::Histogram,         // number of packets per verify call
    total_batches: usize,
    total_packets: usize,
    total_dedup: usize,
    total_excess_fail: usize,
    total_valid_packets: usize,
    total_shrinks: usize,
    total_discard_random: usize,
    total_dedup_time_us: usize,
    total_discard_time_us: usize,
    total_discard_random_time_us: usize,
    total_verify_time_us: usize,
    total_shrink_time_us: usize,
}

impl SigVerifierStats {
    fn report(&self, name: &'static str) {
        datapoint_info!(
            name,
            (
                "recv_batches_us_90pct",
                self.recv_batches_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_min",
                self.recv_batches_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_max",
                self.recv_batches_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "recv_batches_us_mean",
                self.recv_batches_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_90pct",
                self.verify_batches_pp_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_min",
                self.verify_batches_pp_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_max",
                self.verify_batches_pp_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_mean",
                self.verify_batches_pp_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_90pct",
                self.discard_packets_pp_us_hist
                    .percentile(90.0)
                    .unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_min",
                self.discard_packets_pp_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_max",
                self.discard_packets_pp_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "discard_packets_pp_us_mean",
                self.discard_packets_pp_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_90pct",
                self.dedup_packets_pp_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_min",
                self.dedup_packets_pp_us_hist.minimum().unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_max",
                self.dedup_packets_pp_us_hist.maximum().unwrap_or(0),
                i64
            ),
            (
                "dedup_packets_pp_us_mean",
                self.dedup_packets_pp_us_hist.mean().unwrap_or(0),
                i64
            ),
            (
                "batches_90pct",
                self.batches_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("batches_min", self.batches_hist.minimum().unwrap_or(0), i64),
            ("batches_max", self.batches_hist.maximum().unwrap_or(0), i64),
            ("batches_mean", self.batches_hist.mean().unwrap_or(0), i64),
            (
                "packets_90pct",
                self.packets_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            ("packets_min", self.packets_hist.minimum().unwrap_or(0), i64),
            ("packets_max", self.packets_hist.maximum().unwrap_or(0), i64),
            ("packets_mean", self.packets_hist.mean().unwrap_or(0), i64),
            ("total_batches", self.total_batches, i64),
            ("total_packets", self.total_packets, i64),
            ("total_dedup", self.total_dedup, i64),
            ("total_excess_fail", self.total_excess_fail, i64),
            ("total_valid_packets", self.total_valid_packets, i64),
            ("total_discard_random", self.total_discard_random, i64),
            ("total_shrinks", self.total_shrinks, i64),
            ("total_dedup_time_us", self.total_dedup_time_us, i64),
            ("total_discard_time_us", self.total_discard_time_us, i64),
            (
                "total_discard_random_time_us",
                self.total_discard_random_time_us,
                i64
            ),
            ("total_verify_time_us", self.total_verify_time_us, i64),
            ("total_shrink_time_us", self.total_shrink_time_us, i64),
        );
    }
}

impl SigVerifier for DisabledSigVerifier {
    type SendType = ();
    fn verify_batches(
        &self,
        mut batches: Vec<PacketBatch>,
        _valid_packets: usize,
    ) -> Vec<PacketBatch> {
        sigverify::ed25519_verify_disabled(&mut batches);
        batches
    }

    fn send_packets(&mut self, _packet_batches: Vec<PacketBatch>) -> Result<(), Self::SendType> {
        Ok(())
    }
}

impl VerifyFilterStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        sender: Sender<Vec<PacketBatch>>,
        name: &'static str,
    ) -> Self {
        let thread_hdl = Self::filter_services(packet_receiver, sender, name);
        Self { thread_hdl }
    }

    pub fn discard_excess_packets(
        batches: &mut [PacketBatch],
        mut max_packets: usize,
    ) {
        // Group packets by their incoming IP address.
        let mut addrs = batches
            .iter_mut()
            .rev()
            .flat_map(|batch| batch.iter_mut().rev())
            .filter(|packet| !packet.meta.discard())
            .map(|packet| (packet.meta.addr, packet))
            .into_group_map();
        // Allocate max_packets evenly across addresses.
        while max_packets > 0 && !addrs.is_empty() {
            let num_addrs = addrs.len();
            addrs.retain(|_, packets| {
                let cap = (max_packets + num_addrs - 1) / num_addrs;
                max_packets -= packets.len().min(cap);
                packets.truncate(packets.len().saturating_sub(cap));
                !packets.is_empty()
            });
        }
        // Discard excess packets from each address.
        for packet in addrs.into_values().flatten() {
            packet.meta.set_discard(true);
        }
    }

    fn filter(
        deduper: &Deduper,
        recvr: &find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        sender: &Sender<Vec<PacketBatch>>,
        stats: &mut SigVerifierStats,
    ) -> Result<()> {
        let (mut batches, num_packets, _recv_duration) = streamer::recv_vec_packet_batches(recvr)?;

        debug!(
            "@{:?} filter: filtering: {} packets",
            timing::timestamp(),
            num_packets,
        );

        let mut discard_random_time = Measure::start("sigverify_discard_random_time");
        let non_discarded_packets = solana_perf::discard::discard_batches_randomly(
            &mut batches,
            MAX_DEDUP_BATCH,
            num_packets,
        );
        let num_discarded_randomly = num_packets.saturating_sub(non_discarded_packets);
        discard_random_time.stop();

        let mut dedup_time = Measure::start("sigverify_dedup_time");
        let discard_or_dedup_fail = deduper.dedup_packets_and_count_discards(
            &mut batches,
        ) as usize;
        dedup_time.stop();
        let num_unique = non_discarded_packets.saturating_sub(discard_or_dedup_fail);

        let mut discard_time = Measure::start("sigverify_discard_time");
        let mut num_valid_packets = num_unique;
        if num_unique > MAX_SIGVERIFY_BATCH {
            Self::discard_excess_packets(
                &mut batches,
                MAX_SIGVERIFY_BATCH,
            );
            num_valid_packets = MAX_SIGVERIFY_BATCH;
        }
        let excess_fail = num_unique.saturating_sub(MAX_SIGVERIFY_BATCH);
        discard_time.stop();

        sender.send(batches)?;

        debug!(
            "\ndiscard random time={:?}ns\ndedup time ={:?}us\ndiscard time={:?}ns",
            discard_random_time.as_ns(),
            dedup_time.as_us(),
            discard_time.as_ns(),
        );

        debug!(
            "\nnum packets={:?}\ndiscard random={:?} packets\ndedup ={:?} packets\ndiscard ={:?} packets",
            num_packets,
            num_discarded_randomly,
            discard_or_dedup_fail,
            excess_fail,
        );

        Ok(())
    }

    fn filter_service(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        sender: Sender<Vec<PacketBatch>>,
        name: &'static str,
    ) -> JoinHandle<()> {
        let mut stats = SigVerifierStats::default();
        const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
        const MAX_DEDUPER_ITEMS: u32 = 1_000_000;
        Builder::new()
            .name("solana-filter".to_string())
            .spawn(move || {
                let mut deduper = Deduper::new(MAX_DEDUPER_ITEMS, MAX_DEDUPER_AGE);
                loop {
                    deduper.reset();
                    Self::filter(&deduper, &packet_receiver, &sender, &mut stats);
                }
            })
            .unwrap()
    }

    fn filter_services(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        sender: Sender<Vec<PacketBatch>>,
        name: &'static str,
    ) -> JoinHandle<()> {
        Self::filter_service(packet_receiver, sender, name)
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

impl SigVerifyStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: T,
        name: &'static str,
    ) -> Self {
        let thread_hdl = Self::verifier_services(packet_receiver, verifier, name);
        Self { thread_hdl }
    }

    fn verifier<T: SigVerifier>(
        deduper: &Deduper,
        recvr: &find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: &mut T,
        stats: &mut SigVerifierStats,
    ) -> Result<(), T::SendType> {
        let (mut batches, num_packets, recv_duration) = streamer::recv_vec_packet_batches(recvr)?;

        let batches_len = batches.len();
        debug!(
            "@{:?} verifier: verifying: {} packets",
            timing::timestamp(),
            num_packets,
        );

        let mut verify_time = Measure::start("sigverify_batch_time");
        let mut batches = verifier.verify_batches(batches, num_packets);
        verify_time.stop();

        let mut shrink_time = Measure::start("sigverify_shrink_time");
        let num_valid_packets = count_valid_packets(
            &batches,
            #[inline(always)]
            |valid_packet| verifier.process_passed_sigverify_packet(valid_packet),
        );
        let start_len = batches.len();
        const MAX_EMPTY_BATCH_RATIO: usize = 4;
        if num_packets > num_valid_packets.saturating_mul(MAX_EMPTY_BATCH_RATIO) {
            let valid = shrink_batches(&mut batches);
            batches.truncate(valid);
        }
        let total_shrinks = start_len.saturating_sub(batches.len());
        shrink_time.stop();

        verifier.send_packets(batches)?;

        debug!(
            "@{:?} verifier done. batches: {}, packets: {}, ver/s: {}",
            timing::timestamp(),
            batches_len,
            num_packets,
            (num_packets as f32 / verify_time.as_s())
        );
        debug!(
            "\nverify time={:?}us\nshrink time={:?}us",
            verify_time.as_us(),
            shrink_time.as_us(),
        );

        debug!(
            "\nverify={:?} packets\nshrink={:?} batches",
            num_valid_packets,
            total_shrinks,
        );

        Ok(())
    }

    fn verifier_service<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        mut verifier: T,
        name: &'static str,
    ) -> JoinHandle<()> {
        let mut stats = SigVerifierStats::default();
        let mut last_print = Instant::now();
        const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
        const MAX_DEDUPER_ITEMS: u32 = 1_000_000;
        Builder::new()
            .name("solana-verifier".to_string())
            .spawn(move || {
                let mut deduper = Deduper::new(MAX_DEDUPER_ITEMS, MAX_DEDUPER_AGE);
                loop {
                    deduper.reset();
                    if let Err(e) =
                        Self::verifier(&deduper, &packet_receiver, &mut verifier, &mut stats)
                    {
                        match e {
                            SigVerifyServiceError::Streamer(StreamerError::RecvTimeout(
                                RecvTimeoutError::Disconnected,
                            )) => break,
                            SigVerifyServiceError::Streamer(StreamerError::RecvTimeout(
                                RecvTimeoutError::Timeout,
                            )) => (),
                            SigVerifyServiceError::Send(_) => {
                                break;
                            }
                            _ => error!("{:?}", e),
                        }
                    }
                    if last_print.elapsed().as_secs() > 2 {
                        stats.report(name);
                        stats = SigVerifierStats::default();
                        last_print = Instant::now();
                    }
                }
            })
            .unwrap()
    }

    fn verifier_services<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: T,
        name: &'static str,
    ) -> JoinHandle<()> {
        Self::verifier_service(packet_receiver, verifier, name)
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{sigverify::TransactionSigVerifier, sigverify_stage::timing::duration_as_ms},
        crossbeam_channel::unbounded,
        solana_perf::{
            packet::{to_packet_batches},
            test_tx::test_tx,
        },
        solana_sdk::packet::PacketFlags,
    };

    fn count_non_discard(packet_batches: &[PacketBatch]) -> usize {
        packet_batches
            .iter()
            .map(|batch| {
                batch
                    .iter()
                    .map(|p| if p.meta.discard() { 0 } else { 1 })
                    .sum::<usize>()
            })
            .sum::<usize>()
    }

    fn gen_batches(
        use_same_tx: bool,
        packets_per_batch: usize,
        total_packets: usize,
    ) -> Vec<PacketBatch> {
        if use_same_tx {
            let tx = test_tx();
            to_packet_batches(&vec![tx; total_packets], packets_per_batch)
        } else {
            let txs: Vec<_> = (0..total_packets).map(|_| test_tx()).collect();
            to_packet_batches(&txs, packets_per_batch)
        }
    }

    #[test]
    fn test_sigverify_stage() {
        solana_logger::setup();
        trace!("start");
        let (packet_s, packet_r) = unbounded();
        let (filter_s, filter_r) = unbounded();
        let (verified_s, verified_r) = unbounded();

        let filter = VerifyFilterStage::new(packet_r, filter_s, "test_filter");
        let verifier = TransactionSigVerifier::new(verified_s);
        let stage = SigVerifyStage::new(filter_r, verifier, "test_sigverify");

        let use_same_tx = false;
        let now = Instant::now();
        let packets_per_batch = 1;
        let total_packets = 200000/packets_per_batch*packets_per_batch;
        
        // This is important so that we don't discard any packets and fail asserts below about
        // `total_excess_tracer_packets`
        //assert!(total_packets <= MAX_SIGVERIFY_BATCH);
        let mut batches = gen_batches(use_same_tx, packets_per_batch, total_packets);
        trace!(
            "starting... generation took: {} ms batches: {}",
            duration_as_ms(&now.elapsed()),
            batches.len()
        );

        let mut sent_len = 0;
        for _ in 0..batches.len() {
            if let Some(mut batch) = batches.pop() {
                sent_len += batch.len();
                batch
                    .iter_mut()
                    .for_each(|packet| packet.meta.flags |= PacketFlags::TRACER_PACKET);
                assert_eq!(batch.len(), packets_per_batch);
                packet_s.send(vec![batch]).unwrap();
            }
        }
        let mut received = 0;
        let mut total_tracer_packets_received_in_sigverify_stage = 0;
        trace!("sent: {}", sent_len);
        loop {
            if let Ok((mut verifieds, tracer_packet_stats_option)) = verified_r.recv() {
                let tracer_packet_stats = tracer_packet_stats_option.unwrap();
                total_tracer_packets_received_in_sigverify_stage +=
                    tracer_packet_stats.total_tracer_packets_received_in_sigverify_stage;
                assert_eq!(
                    tracer_packet_stats.total_tracer_packets_received_in_sigverify_stage
                        % packets_per_batch,
                    0,
                );

                if use_same_tx {
                    // Every transaction other than the very first one in the very first batch
                    // should be deduped.

                    // Also have to account for the fact that deduper could be cleared periodically,
                    // in which case the first transaction in the next batch won't be deduped
                    assert!(
                        (tracer_packet_stats.total_tracer_packets_deduped
                            == tracer_packet_stats
                                .total_tracer_packets_received_in_sigverify_stage
                                - 1)
                            || (tracer_packet_stats.total_tracer_packets_deduped
                                == tracer_packet_stats
                                    .total_tracer_packets_received_in_sigverify_stage)
                    );
                    assert!(
                        (tracer_packet_stats.total_tracker_packets_passed_sigverify == 1)
                            || (tracer_packet_stats.total_tracker_packets_passed_sigverify == 0)
                    );
                } else {
                    assert_eq!(tracer_packet_stats.total_tracer_packets_deduped, 0);
                }
                assert_eq!(tracer_packet_stats.total_excess_tracer_packets, 0);
                while let Some(v) = verifieds.pop() {
                    received += v.len();
                    batches.push(v);
                }
            }

            if total_tracer_packets_received_in_sigverify_stage >= sent_len {
                break;
            }
        }
        trace!("received: {}", received);

        drop(packet_s);
        stage.join().unwrap();
    }
}

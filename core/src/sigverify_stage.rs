//! The `sigverify_stage` implements the signature verification stage of the TPU. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next stage whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if perf-libs are available

use {
    crate::{find_packet_sender_stake_stage, sigverify},
    core::time::Duration,
    crossbeam_channel::{unbounded, RecvTimeoutError, SendError, Sender, Receiver},
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
const MAX_SIGVERIFY_BATCH: usize = 2_000;

#[derive(Error, Debug)]
pub enum SigVerifyServiceError<SendType> {
    #[error("send packets batch error")]
    Send(#[from] SendError<SendType>),

    #[error("streamer error")]
    Streamer(#[from] StreamerError),
}

type Result<T, SendType> = std::result::Result<T, SigVerifyServiceError<SendType>>;

pub struct SigVerifyStage {
    thread_hdls: Vec<JoinHandle<()>>,
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

impl SigVerifyStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: T,
        name: &'static str,
    ) -> Self {
        let thread_hdls = Self::verifier_services(packet_receiver, verifier, name);
        Self { thread_hdls }
    }

    pub fn discard_excess_packets(
        batches: &mut [PacketBatch],
        mut max_packets: usize,
        mut process_excess_packet: impl FnMut(&Packet),
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
            process_excess_packet(packet);
            packet.meta.set_discard(true);
        }
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
            "@{:?} verifier: verifying: {}",
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
            "@{:?} verifier: done. batches: {} total verify time: {:?} verified: {} v/s {}",
            timing::timestamp(),
            batches_len,
            verify_time.as_ms(),
            num_packets,
            (num_packets as f32 / verify_time.as_s())
        );

        stats
            .recv_batches_us_hist
            .increment(recv_duration.as_micros() as u64)
            .unwrap();
        stats
            .verify_batches_pp_us_hist
            .increment(verify_time.as_us() / (num_packets as u64))
            .unwrap();
        stats.batches_hist.increment(batches_len as u64).unwrap();
        stats.packets_hist.increment(num_packets as u64).unwrap();
        stats.total_batches += batches_len;
        stats.total_packets += num_packets;
        stats.total_valid_packets += num_valid_packets;
        stats.total_shrinks += total_shrinks;
        stats.total_verify_time_us += verify_time.as_us() as usize;
        stats.total_shrink_time_us += shrink_time.as_us() as usize;

        Ok(())
    }

    fn verifier_service<T: SigVerifier + 'static + Send + Clone>(
        filter_receiver: Receiver<Vec<PacketBatch>>,
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
                        Self::verifier(&deduper, &filter_receiver, &mut verifier, &mut stats)
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

    fn filter<T: SigVerifier>(
        deduper: &Deduper,
        recvr: &find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        verifier: &mut T,
        sender: &Sender<Vec<PacketBatch>>,
        stats: &mut SigVerifierStats,
    ) -> Result<(), T::SendType> {
        let (mut batches, num_packets, recv_duration) = streamer::recv_vec_packet_batches(recvr)?;

        let batches_len = batches.len();
        debug!(
            "@{:?} filter: filtering: {}",
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
            #[inline(always)]
            |received_packet, removed_before_sigverify_stage, is_dup| {
                verifier.process_received_packet(
                    received_packet,
                    removed_before_sigverify_stage,
                    is_dup,
                );
            },
        ) as usize;
        dedup_time.stop();
        let num_unique = non_discarded_packets.saturating_sub(discard_or_dedup_fail);

        let mut discard_time = Measure::start("sigverify_discard_time");
        let mut num_valid_packets = num_unique;
        if num_unique > MAX_SIGVERIFY_BATCH {
            Self::discard_excess_packets(
                &mut batches,
                MAX_SIGVERIFY_BATCH,
                #[inline(always)]
                |excess_packet| verifier.process_excess_packet(excess_packet),
            );
            num_valid_packets = MAX_SIGVERIFY_BATCH;
        }
        let excess_fail = num_unique.saturating_sub(MAX_SIGVERIFY_BATCH);
        discard_time.stop();

        let mut shrink_time = Measure::start("sigverify_shrink_time");
        let num_valid_packets = count_valid_packets(
            &batches,
            #[inline(always)]
            |valid_packet| verifier.process_passed_sigverify_packet(valid_packet),
        );
        let start_len = batches.len();
        const MAX_EMPTY_BATCH_RATIO: usize = 4;
        if non_discarded_packets > num_valid_packets.saturating_mul(MAX_EMPTY_BATCH_RATIO) {
            let valid = shrink_batches(&mut batches);
            batches.truncate(valid);
        }
        let total_shrinks = start_len.saturating_sub(batches.len());
        shrink_time.stop();

        if let Err(e) = sender.send(batches)
        {
            error!("{:?}", e);
        }

        debug!(
            "@{:?} filter: done. batches: {} packets: {} random discard: {} dedup: {} excess: {}",
            timing::timestamp(),
            batches_len,
            num_packets,
            num_discarded_randomly,
            discard_or_dedup_fail,
            excess_fail
        );

        stats
            .recv_batches_us_hist
            .increment(recv_duration.as_micros() as u64)
            .unwrap();
        stats
            .discard_packets_pp_us_hist
            .increment(discard_time.as_us() / (num_packets as u64))
            .unwrap();
        stats
            .dedup_packets_pp_us_hist
            .increment(dedup_time.as_us() / (num_packets as u64))
            .unwrap();
        stats.batches_hist.increment(batches_len as u64).unwrap();
        stats.packets_hist.increment(num_packets as u64).unwrap();
        stats.total_batches += batches_len;
        stats.total_packets += num_packets;
        stats.total_dedup += discard_or_dedup_fail;
        stats.total_valid_packets += num_valid_packets;
        stats.total_discard_random_time_us += discard_random_time.as_us() as usize;
        stats.total_discard_random += num_discarded_randomly;
        stats.total_excess_fail += excess_fail;
        stats.total_shrinks += total_shrinks;
        stats.total_dedup_time_us += dedup_time.as_us() as usize;
        stats.total_discard_time_us += discard_time.as_us() as usize;
        stats.total_shrink_time_us += shrink_time.as_us() as usize;

        Ok(())
    }

    fn filter_service<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: find_packet_sender_stake_stage::FindPacketSenderStakeReceiver,
        mut verifier: T,
        filter_sender: Sender<Vec<PacketBatch>>,
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
                        Self::filter(&deduper, &packet_receiver, &mut verifier, &filter_sender, &mut stats)
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
    ) -> Vec<JoinHandle<()>> {
        let (filter_sender, filter_receiver) = unbounded();
        let thread_filter = Self::filter_service(packet_receiver, verifier.clone(), filter_sender, name);
        let thread_verifier = Self::verifier_service(filter_receiver, verifier, name);
        vec![thread_filter, thread_verifier]
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{sigverify::TransactionSigVerifier, sigverify_stage::timing::duration_as_ms},
        crossbeam_channel::unbounded,
        solana_perf::{
            packet::{to_packet_batches, Packet},
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

    #[test]
    fn test_packet_discard() {
        solana_logger::setup();
        let batch_size = 10;
        let mut batch = PacketBatch::with_capacity(batch_size);
        let mut tracer_packet = Packet::default();
        tracer_packet.meta.flags |= PacketFlags::TRACER_PACKET;
        batch.resize(batch_size, tracer_packet);
        batch[3].meta.addr = std::net::IpAddr::from([1u16; 8]);
        batch[3].meta.set_discard(true);
        let num_discarded_before_filter = 1;
        batch[4].meta.addr = std::net::IpAddr::from([2u16; 8]);
        let total_num_packets = batch.len();
        let mut batches = vec![batch];
        let max = 3;
        let mut total_tracer_packets_discarded = 0;
        SigVerifyStage::discard_excess_packets(&mut batches, max, |packet| {
            if packet.meta.is_tracer_packet() {
                total_tracer_packets_discarded += 1;
            }
        });
        let total_non_discard = count_non_discard(&batches);
        let total_discarded = total_num_packets - total_non_discard;
        // Every packet except the packets already marked `discard` before the call
        // to `discard_excess_packets()` should count towards the
        // `total_tracer_packets_discarded`
        assert_eq!(
            total_tracer_packets_discarded,
            total_discarded - num_discarded_before_filter
        );
        assert_eq!(total_non_discard, max);
        assert!(!batches[0][0].meta.discard());
        assert!(batches[0][3].meta.discard());
        assert!(!batches[0][4].meta.discard());
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
        let (verified_s, verified_r) = unbounded();
        let verifier = TransactionSigVerifier::new(verified_s);
        let stage = SigVerifyStage::new(packet_r, verifier, "test");

        let use_same_tx = true;
        let now = Instant::now();
        let packets_per_batch = 128;
        let total_packets = 1920;
        let expected_packets = if use_same_tx {
            1
        } else {
            total_packets
        };
        // This is important so that we don't discard any packets and fail asserts below about
        // `total_excess_tracer_packets`
        assert!(total_packets < MAX_SIGVERIFY_BATCH);
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
        trace!("sent: {}", sent_len);
        loop {
            if let Ok((mut verifieds, _tracer_packet_stats)) = verified_r.recv() {
                while let Some(v) = verifieds.pop() {
                    received += v.len();
                    batches.push(v);
                }
            }

            if received >= expected_packets {
                break;
            }
        }
        trace!("received: {}", received);
        drop(packet_s);
        stage.join().unwrap();
    }
}

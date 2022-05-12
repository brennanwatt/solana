//! The `streamer` module defines a set of services for efficiently pulling data from UDP sockets.
//!

use {
    crate::{
        packet::{self, PacketBatch, PacketBatchRecycler, PACKETS_PER_BATCH},
        sendmmsg::{batch_send, SendPktsError},
        socket::SocketAddrSpace,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, SendError, RecvError, Sender},
    histogram::Histogram,
    solana_sdk::{packet::Packet, timing::timestamp},
    std::collections::VecDeque,
    std::sync::RwLock,
    std::{
        cmp::Reverse,
        collections::HashMap,
        net::{IpAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::{Duration, Instant},
    },
    thiserror::Error,
};

struct MyPacketBatchChannelData {
    queue: VecDeque<PacketBatch>,
    packet_count: usize,
    work_unit_packet_size: usize,
    max_queued_batches: usize,
}

#[derive(Clone)]
pub struct MyPacketBatchReceiver {
    receiver: Receiver<()>,
    sender: Sender<()>,
    data: Arc<RwLock<MyPacketBatchChannelData>>,
}

#[derive(Clone)]
pub struct MyPacketBatchSender {
    sender: Sender<()>,
    data: Arc<RwLock<MyPacketBatchChannelData>>,
}

impl MyPacketBatchChannelData {
    fn add_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_add(amount);
    }
    fn sub_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_sub(amount);
    }
}

impl MyPacketBatchReceiver {
    // TODO: can return Ok(no-batches)
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<(Vec<PacketBatch>, usize, Duration), RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        loop {
            return match self.receiver.recv_deadline(deadline) {
                Ok(()) => {
                    if let Some(r) = self.try_recv() {
                        Ok(r)
                    } else {
                        continue;
                    }
                }
                Err(err) => Err(err),
            }
        }
    }

    pub fn recv(&self) -> std::result::Result<(Vec<PacketBatch>, usize, Duration), RecvError> {
        loop {
            return match self.receiver.recv() {
                Ok(()) => {
                    if let Some(r) = self.try_recv() {
                        Ok(r)
                    } else {
                        continue;
                    }
                }
                Err(err) => Err(err),
            }
        }
    }

    fn try_recv(&self) -> Option<(Vec<PacketBatch>, usize, Duration)> {
        let (recv_data, packets, has_more) = {
            let mut locked_data = self.data.write().unwrap();

            let mut batches = 0;
            let mut packets = 0;
            for batch in locked_data.queue.iter() {
                let new_packets = packets + batch.packets.len();
                if new_packets > locked_data.work_unit_packet_size {
                    break;
                }
                packets = new_packets;
                batches += 1;
            }
            if batches == 0 {
                return None;
            }
            locked_data.sub_packet_count(packets);
            let has_more = batches < locked_data.queue.len();
            (
                locked_data.queue.drain(0..batches).collect::<Vec<_>>(),
                packets,
                has_more,
            )
        };

        // if there's more data than this consumer could handle,
        // send the flag to wake another consumer
        if has_more {
            self.sender.send(()); // TODO: handle error?
        }

        Some((recv_data, packets, Duration::ZERO))
    }

    pub fn recv_default_timeout(&self) -> std::result::Result<(Vec<PacketBatch>, usize, Duration), RecvTimeoutError> {
        let timer = Duration::new(1, 0);
        self.recv_timeout(timer)
    }

    pub fn batch_count(&self) -> usize {
        self.data.read().unwrap().queue.len()
    }

    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
    }
}

impl MyPacketBatchSender {
    // Ok(true) means an existing batch was discarded
    pub fn send_batch(&self, batch: PacketBatch) -> std::result::Result<bool, SendError<()>> {
        if batch.packets.len() == 0 {
            return Ok(false);
        }
        let batch_discarded = {
            let mut locked_data = self.data.write().unwrap();
            let discarded = if locked_data.queue.len() >= locked_data.max_queued_batches {
                if let Some(old_batch) = locked_data.queue.pop_front() {
                    locked_data.sub_packet_count(old_batch.packets.len());
                    true
                } else {
                    false
                }
            } else {
                false
            };
            locked_data.add_packet_count(batch.packets.len());
            locked_data.queue.push_back(batch);
            discarded
        };
        if let Err(err) = self.sender.send(()) {
            return Err(err);
        }
        Ok(batch_discarded)
    }

    // Ok(n) means that n batches were discarded
    pub fn send_batches(&self, batches: Vec<PacketBatch>) -> std::result::Result<usize, SendError<()>> {
        if batches.len() == 0 {
            return Ok(0);
        }
        // TODO: this allows adding batches without packets
        let batches_discarded = {
            let mut locked_data = self.data.write().unwrap();
            let max_queued_batches = locked_data.max_queued_batches;
            let batches_to_queue = std::cmp::min(batches.len(), max_queued_batches);
            let discard_new_batches = batches.len() - batches_to_queue;

            let discard_old_batches =
                (locked_data.queue.len() + batches_to_queue).saturating_sub(max_queued_batches);
            let discard_old_packets = locked_data
                .queue
                .drain(0..discard_old_batches)
                .map(|b| b.packets.len())
                .sum();
            locked_data.sub_packet_count(discard_old_packets);

            let new_packets = batches
                .iter()
                .take(max_queued_batches)
                .map(|b| b.packets.len())
                .sum();
            locked_data.add_packet_count(new_packets);
            locked_data
                .queue
                .extend(batches.into_iter().take(max_queued_batches));

            discard_old_batches + discard_new_batches
        };
        if let Err(err) = self.sender.send(()) {
            return Err(err);
        }
        Ok(batches_discarded)
    }

    pub fn batch_count(&self) -> usize {
        self.data.read().unwrap().queue.len()
    }

    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
    }
}

pub fn my_packet_batch_channel(work_unit_packet_size: usize, max_queued_batches: usize) -> (MyPacketBatchSender, MyPacketBatchReceiver) {
    let (sig_sender, sig_receiver) = crossbeam_channel::unbounded::<()>();
    let data = Arc::new(RwLock::new(MyPacketBatchChannelData {
        queue: VecDeque::new(),
        packet_count: 0,
        max_queued_batches,
        work_unit_packet_size,
    }));
    let sender = MyPacketBatchSender {
        sender: sig_sender.clone(),
        data: data.clone(),
    };
    let receiver = MyPacketBatchReceiver {
        sender: sig_sender,
        receiver: sig_receiver,
        data: data,
    };
    (sender, receiver)
}

pub type PacketBatchReceiver = Receiver<PacketBatch>;
pub type PacketBatchSender = Sender<PacketBatch>;

#[derive(Error, Debug)]
pub enum StreamerError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("receive timeout error")]
    RecvTimeout(#[from] RecvTimeoutError),

    #[error("send packets error")]
    Send(#[from] SendError<PacketBatch>),

    #[error(transparent)]
    SendPktsError(#[from] SendPktsError),
}

pub struct StreamerReceiveStats {
    pub name: &'static str,
    pub packets_count: AtomicUsize,
    pub packet_batches_count: AtomicUsize,
    pub full_packet_batches_count: AtomicUsize,
    pub max_channel_len: AtomicUsize,
}

impl StreamerReceiveStats {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            packets_count: AtomicUsize::default(),
            packet_batches_count: AtomicUsize::default(),
            full_packet_batches_count: AtomicUsize::default(),
            max_channel_len: AtomicUsize::default(),
        }
    }

    pub fn report(&self) {
        datapoint_warn!(
            self.name,
            (
                "packets_count",
                self.packets_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "packet_batches_count",
                self.packet_batches_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "full_packet_batches_count",
                self.full_packet_batches_count.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
            (
                "channel_len",
                self.max_channel_len.swap(0, Ordering::Relaxed) as i64,
                i64
            ),
        );
    }
}

pub type Result<T> = std::result::Result<T, StreamerError>;

fn recv_loop(
    socket: &UdpSocket,
    exit: Arc<AtomicBool>,
    packet_batch_sender: &PacketBatchSender,
    recycler: &PacketBatchRecycler,
    stats: &StreamerReceiveStats,
    coalesce_ms: u64,
    use_pinned_memory: bool,
) -> Result<()> {
    loop {
        let mut packet_batch = if use_pinned_memory {
            PacketBatch::new_with_recycler(recycler.clone(), PACKETS_PER_BATCH, stats.name)
        } else {
            PacketBatch::with_capacity(PACKETS_PER_BATCH)
        };
        loop {
            // Check for exit signal, even if socket is busy
            // (for instance the leader transaction socket)
            if exit.load(Ordering::Relaxed) {
                return Ok(());
            }
            if let Ok(len) = packet::recv_from(&mut packet_batch, socket, coalesce_ms) {
                if len > 0 {
                    let StreamerReceiveStats {
                        packets_count,
                        packet_batches_count,
                        full_packet_batches_count,
                        max_channel_len,
                        ..
                    } = stats;

                    packets_count.fetch_add(len, Ordering::Relaxed);
                    packet_batches_count.fetch_add(1, Ordering::Relaxed);
                    max_channel_len.fetch_max(packet_batch_sender.len(), Ordering::Relaxed);
                    if len == PACKETS_PER_BATCH {
                        full_packet_batches_count.fetch_add(1, Ordering::Relaxed);
                    }

                    packet_batch_sender.send(packet_batch)?;
                }
                break;
            }
        }
    }
}

pub fn receiver(
    socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
    packet_batch_sender: PacketBatchSender,
    recycler: PacketBatchRecycler,
    stats: Arc<StreamerReceiveStats>,
    coalesce_ms: u64,
    use_pinned_memory: bool,
) -> JoinHandle<()> {
    let res = socket.set_read_timeout(Some(Duration::new(1, 0)));
    assert!(res.is_ok(), "streamer::receiver set_read_timeout error");
    Builder::new()
        .name("solana-receiver".to_string())
        .spawn(move || {
            let _ = recv_loop(
                &socket,
                exit,
                &packet_batch_sender,
                &recycler,
                &stats,
                coalesce_ms,
                use_pinned_memory,
            );
        })
        .unwrap()
}

#[derive(Debug, Default)]
struct SendStats {
    bytes: u64,
    count: u64,
}

#[derive(Default)]
struct StreamerSendStats {
    host_map: HashMap<IpAddr, SendStats>,
    since: Option<Instant>,
}

impl StreamerSendStats {
    fn report_stats(
        name: &'static str,
        host_map: HashMap<IpAddr, SendStats>,
        sample_duration: Option<Duration>,
    ) {
        const MAX_REPORT_ENTRIES: usize = 5;
        let sample_ms = sample_duration.map(|d| d.as_millis()).unwrap_or_default();
        let mut hist = Histogram::default();
        let mut byte_sum = 0;
        let mut pkt_count = 0;
        host_map.iter().for_each(|(_addr, host_stats)| {
            hist.increment(host_stats.bytes).unwrap();
            byte_sum += host_stats.bytes;
            pkt_count += host_stats.count;
        });

        datapoint_info!(
            name,
            ("streamer-send-sample_duration_ms", sample_ms, i64),
            ("streamer-send-host_count", host_map.len(), i64),
            ("streamer-send-bytes_total", byte_sum, i64),
            ("streamer-send-pkt_count_total", pkt_count, i64),
            (
                "streamer-send-host_bytes_min",
                hist.minimum().unwrap_or_default(),
                i64
            ),
            (
                "streamer-send-host_bytes_max",
                hist.maximum().unwrap_or_default(),
                i64
            ),
            (
                "streamer-send-host_bytes_mean",
                hist.mean().unwrap_or_default(),
                i64
            ),
            (
                "streamer-send-host_bytes_90pct",
                hist.percentile(90.0).unwrap_or_default(),
                i64
            ),
            (
                "streamer-send-host_bytes_50pct",
                hist.percentile(50.0).unwrap_or_default(),
                i64
            ),
            (
                "streamer-send-host_bytes_10pct",
                hist.percentile(10.0).unwrap_or_default(),
                i64
            ),
        );

        let num_entries = host_map.len();
        let mut entries: Vec<_> = host_map.into_iter().collect();
        if entries.len() > MAX_REPORT_ENTRIES {
            entries.select_nth_unstable_by_key(MAX_REPORT_ENTRIES, |(_addr, stats)| {
                Reverse(stats.bytes)
            });
            entries.truncate(MAX_REPORT_ENTRIES);
        }
        info!(
            "streamer send {} hosts: count:{} {:?}",
            name, num_entries, entries,
        );
    }

    fn maybe_submit(&mut self, name: &'static str, sender: &Sender<Box<dyn FnOnce() + Send>>) {
        const SUBMIT_CADENCE: Duration = Duration::from_secs(10);
        const MAP_SIZE_REPORTING_THRESHOLD: usize = 1_000;
        let elapsed = self.since.as_ref().map(Instant::elapsed);
        if elapsed.map(|e| e < SUBMIT_CADENCE).unwrap_or_default()
            && self.host_map.len() < MAP_SIZE_REPORTING_THRESHOLD
        {
            return;
        }

        let host_map = std::mem::take(&mut self.host_map);
        let _ = sender.send(Box::new(move || {
            Self::report_stats(name, host_map, elapsed);
        }));

        *self = Self {
            since: Some(Instant::now()),
            ..Self::default()
        };
    }

    fn record(&mut self, pkt: &Packet) {
        let ent = self.host_map.entry(pkt.meta.addr).or_default();
        ent.count += 1;
        ent.bytes += pkt.data.len() as u64;
    }
}

fn recv_send(
    sock: &UdpSocket,
    r: &PacketBatchReceiver,
    socket_addr_space: &SocketAddrSpace,
    stats: &mut Option<StreamerSendStats>,
) -> Result<()> {
    let timer = Duration::new(1, 0);
    let packet_batch = r.recv_timeout(timer)?;
    if let Some(stats) = stats {
        packet_batch.packets.iter().for_each(|p| stats.record(p));
    }
    let packets = packet_batch.packets.iter().filter_map(|pkt| {
        let addr = pkt.meta.addr();
        socket_addr_space
            .check(&addr)
            .then(|| (&pkt.data[..pkt.meta.size], addr))
    });
    batch_send(sock, &packets.collect::<Vec<_>>())?;
    Ok(())
}

pub fn recv_vec_packet_batches(
    recvr: &Receiver<Vec<PacketBatch>>,
) -> Result<(Vec<PacketBatch>, usize, Duration)> {
    let timer = Duration::new(1, 0);
    let mut packet_batches = recvr.recv_timeout(timer)?;
    let recv_start = Instant::now();
    trace!("got packets");
    let mut num_packets = packet_batches
        .iter()
        .map(|packets| packets.packets.len())
        .sum::<usize>();
    while let Ok(packet_batch) = recvr.try_recv() {
        trace!("got more packets");
        num_packets += packet_batch
            .iter()
            .map(|packets| packets.packets.len())
            .sum::<usize>();
        packet_batches.extend(packet_batch);
    }
    let recv_duration = recv_start.elapsed();
    trace!(
        "packet batches len: {}, num packets: {}",
        packet_batches.len(),
        num_packets
    );
    Ok((packet_batches, num_packets, recv_duration))
}

pub fn recv_packet_batches(
    recvr: &PacketBatchReceiver,
) -> Result<(Vec<PacketBatch>, usize, Duration)> {
    let timer = Duration::new(1, 0);
    let packet_batch = recvr.recv_timeout(timer)?;
    let recv_start = Instant::now();
    trace!("got packets");
    let mut num_packets = packet_batch.packets.len();
    let mut packet_batches = vec![packet_batch];
    while let Ok(packet_batch) = recvr.try_recv() {
        trace!("got more packets");
        num_packets += packet_batch.packets.len();
        packet_batches.push(packet_batch);
    }
    let recv_duration = recv_start.elapsed();
    trace!(
        "packet batches len: {}, num packets: {}",
        packet_batches.len(),
        num_packets
    );
    Ok((packet_batches, num_packets, recv_duration))
}

pub fn responder(
    name: &'static str,
    sock: Arc<UdpSocket>,
    r: PacketBatchReceiver,
    socket_addr_space: SocketAddrSpace,
    stats_reporter_sender: Option<Sender<Box<dyn FnOnce() + Send>>>,
) -> JoinHandle<()> {
    Builder::new()
        .name(format!("solana-responder-{}", name))
        .spawn(move || {
            let mut errors = 0;
            let mut last_error = None;
            let mut last_print = 0;
            let mut stats = None;

            if stats_reporter_sender.is_some() {
                stats = Some(StreamerSendStats::default());
            }

            loop {
                if let Err(e) = recv_send(&sock, &r, &socket_addr_space, &mut stats) {
                    match e {
                        StreamerError::RecvTimeout(RecvTimeoutError::Disconnected) => break,
                        StreamerError::RecvTimeout(RecvTimeoutError::Timeout) => (),
                        _ => {
                            errors += 1;
                            last_error = Some(e);
                        }
                    }
                }
                let now = timestamp();
                if now - last_print > 1000 && errors != 0 {
                    datapoint_info!(name, ("errors", errors, i64),);
                    info!("{} last-error: {:?} count: {}", name, last_error, errors);
                    last_print = now;
                    errors = 0;
                }
                if let Some(ref stats_reporter_sender) = stats_reporter_sender {
                    if let Some(ref mut stats) = stats {
                        stats.maybe_submit(name, stats_reporter_sender);
                    }
                }
            }
        })
        .unwrap()
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::{
            packet::{Packet, PacketBatch, PACKET_DATA_SIZE},
            streamer::{receiver, responder},
        },
        crossbeam_channel::unbounded,
        solana_perf::recycler::Recycler,
        std::{
            io,
            io::Write,
            net::UdpSocket,
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc,
            },
            time::Duration,
        },
    };

    fn get_packet_batches(r: PacketBatchReceiver, num_packets: &mut usize) {
        for _ in 0..10 {
            let packet_batch_res = r.recv_timeout(Duration::new(1, 0));
            if packet_batch_res.is_err() {
                continue;
            }

            *num_packets -= packet_batch_res.unwrap().packets.len();

            if *num_packets == 0 {
                break;
            }
        }
    }

    #[test]
    fn streamer_debug() {
        write!(io::sink(), "{:?}", Packet::default()).unwrap();
        write!(io::sink(), "{:?}", PacketBatch::default()).unwrap();
    }
    #[test]
    fn streamer_send_test() {
        let read = UdpSocket::bind("127.0.0.1:0").expect("bind");
        read.set_read_timeout(Some(Duration::new(1, 0))).unwrap();

        let addr = read.local_addr().unwrap();
        let send = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let exit = Arc::new(AtomicBool::new(false));
        let (s_reader, r_reader) = unbounded();
        let stats = Arc::new(StreamerReceiveStats::new("test"));
        let t_receiver = receiver(
            Arc::new(read),
            exit.clone(),
            s_reader,
            Recycler::default(),
            stats.clone(),
            1,
            true,
        );
        const NUM_PACKETS: usize = 5;
        let t_responder = {
            let (s_responder, r_responder) = unbounded();
            let t_responder = responder(
                "streamer_send_test",
                Arc::new(send),
                r_responder,
                SocketAddrSpace::Unspecified,
                None,
            );
            let mut packet_batch = PacketBatch::default();
            for i in 0..NUM_PACKETS {
                let mut p = Packet::default();
                {
                    p.data[0] = i as u8;
                    p.meta.size = PACKET_DATA_SIZE;
                    p.meta.set_addr(&addr);
                }
                packet_batch.packets.push(p);
            }
            s_responder.send(packet_batch).expect("send");
            t_responder
        };

        let mut packets_remaining = NUM_PACKETS;
        get_packet_batches(r_reader, &mut packets_remaining);
        assert_eq!(packets_remaining, 0);
        exit.store(true, Ordering::Relaxed);
        assert_eq!(stats.packets_count.load(Ordering::Relaxed), NUM_PACKETS);
        assert_eq!(stats.packet_batches_count.load(Ordering::Relaxed), 1);
        assert_eq!(stats.full_packet_batches_count.load(Ordering::Relaxed), 0);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
    }
}

//! The `bounded_streamer` module defines a set of services for efficiently and safely pulling data from UDP sockets.
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

struct BoundedPacketBatchChannelData {
    queue: VecDeque<PacketBatch>,
    packet_count: usize,
    work_unit_packet_size: usize,
    max_queued_batches: usize,
}

#[derive(Clone)]
pub struct BoundedPacketBatchReceiver {
    receiver: Receiver<()>,
    sender: Sender<()>,
    data: Arc<RwLock<BoundedPacketBatchChannelData>>,
}

#[derive(Clone)]
pub struct BoundedPacketBatchSender {
    sender: Sender<()>,
    data: Arc<RwLock<BoundedPacketBatchChannelData>>,
}

impl BoundedPacketBatchChannelData {
    fn add_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_add(amount);
    }
    fn sub_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_sub(amount);
    }
}

impl BoundedPacketBatchReceiver {
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

impl BoundedPacketBatchSender {
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

pub fn create_packet_batch_channel(work_unit_packet_size: usize, max_queued_batches: usize)
-> (BoundedPacketBatchSender, BoundedPacketBatchReceiver) {
    let (sig_sender, sig_receiver) = crossbeam_channel::unbounded::<()>();
    let data = Arc::new(RwLock::new(BoundedPacketBatchChannelData {
        queue: VecDeque::new(),
        packet_count: 0,
        max_queued_batches,
        work_unit_packet_size,
    }));
    let sender = BoundedPacketBatchSender {
        sender: sig_sender.clone(),
        data: data.clone(),
    };
    let receiver = BoundedPacketBatchReceiver {
        sender: sig_sender,
        receiver: sig_receiver,
        data: data,
    };
    (sender, receiver)
}
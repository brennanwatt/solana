//! Generators for testing banking stage
//!

use {
    rand::seq::SliceRandom,
    serde::Deserialize,
    solana_runtime::bank::Bank,
    solana_sdk::{
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
    },
    std::path::Path,
};

pub type TransactionGenerator = Box<dyn Send + FnMut(&Bank) -> Vec<SanitizedTransaction>>;

struct AccountsFile {
    payers: Vec<Keypair>,
    allocated_accounts: Vec<Keypair>,
}

impl From<AccountsFileRaw> for AccountsFile {
    fn from(raw: AccountsFileRaw) -> Self {
        let payers = raw.payers.into_iter().map(|r| r.into()).collect();
        let allocated_accounts = raw
            .allocated_accounts
            .into_iter()
            .map(|r| r.into())
            .collect();

        Self {
            payers,
            allocated_accounts,
        }
    }
}

#[derive(Deserialize)]
struct AccountsFileRaw {
    payers: Vec<KeypairRaw>,
    allocated_accounts: Vec<KeypairRaw>,
}

#[derive(Deserialize)]
struct KeypairRaw {
    pub pubkey: String,
    pub secret_key: Vec<u8>,
}

impl From<KeypairRaw> for Keypair {
    fn from(raw: KeypairRaw) -> Self {
        let pubkey: Pubkey = raw.pubkey.parse().unwrap();
        let secret_key = raw.secret_key;
        let bytes = pubkey
            .as_ref()
            .iter()
            .chain(secret_key.iter())
            .cloned()
            .collect::<Vec<_>>();
        Self::from_bytes(&bytes).unwrap()
    }
}

/// Generates transfers between a set of accounts.
pub fn random_transfer_generator(accounts_path: impl AsRef<Path>) -> TransactionGenerator {
    let accounts_file_string = std::fs::read_to_string(accounts_path).unwrap();
    let accounts: AccountsFile = serde_json::from_str::<AccountsFileRaw>(&accounts_file_string)
        .unwrap()
        .into();

    Box::new(move |bank: &Bank| {
        let mut transactions = vec![];
        for _ in 0..64 {
            let from = accounts.payers.choose(&mut rand::thread_rng()).unwrap();
            let to = accounts
                .allocated_accounts
                .choose(&mut rand::thread_rng())
                .unwrap();
            let transaction = solana_sdk::system_transaction::transfer(
                from,
                &to.pubkey(),
                1,
                bank.last_blockhash(),
            );

            let message_hash = transaction.message().hash();
            let versioned_tx: VersionedTransaction = transaction.into();
            let sanitized_versioned_tx: SanitizedVersionedTransaction =
                versioned_tx.try_into().unwrap();
            let sanitized_tx =
                SanitizedTransaction::try_new(sanitized_versioned_tx, message_hash, false, bank)
                    .unwrap();
            transactions.push(sanitized_tx);
        }
        transactions
    })
}

use solana_sdk::{clock::Slot, instruction::InstructionError, saturating_add_assign};

#[derive(Debug, Default)]
pub struct TransactionErrorMetrics {
    pub total: usize,
    pub account_in_use: usize,
    pub account_loaded_twice: usize,
    pub account_not_found: usize,
    pub blockhash_not_found: usize,
    pub blockhash_too_old: usize,
    pub call_chain_too_deep: usize,
    pub already_processed: usize,
    pub instruction_error: usize,
    pub insufficient_funds: usize,
    pub invalid_account_for_fee: usize,
    pub invalid_account_index: usize,
    pub invalid_program_for_execution: usize,
    pub not_allowed_during_cluster_maintenance: usize,
    pub invalid_writable_account: usize,
    pub invalid_rent_paying_account: usize,
}

#[derive(Debug, Default)]
pub struct InstructionErrorMetrics {
    pub generic_error: usize,
    pub invalid_argument: usize,
    pub invalid_instruction_data: usize,
    pub invalid_account_data: usize,
    pub account_data_too_small: usize,
    pub insufficient_funds: usize,
    pub incorrect_program_id: usize,
    pub missing_required_signature: usize,
    pub account_already_initialized: usize,
    pub uninitialized_account: usize,
    pub unbalanced_instruction: usize,
    pub modified_program_id: usize,
    pub external_account_lamport_spend: usize,
    pub external_account_data_modified: usize,
    pub readonly_lamport_change: usize,
    pub readonly_data_modified: usize,
    pub duplicate_account_index: usize,
    pub executable_modified: usize,
    pub rent_epoch_modified: usize,
    pub not_enough_account_keys: usize,
    pub account_data_size_changed: usize,
    pub account_not_executable: usize,
    pub account_borrow_failed: usize,
    pub account_borrow_outstanding: usize,
    pub duplicate_account_out_of_sync: usize,
    pub custom: usize,
    pub invalid_error: usize,
    pub executable_data_modified: usize,
    pub executable_lamport_change: usize,
    pub executable_account_not_rent_exempt: usize,
    pub unsupported_program_id: usize,
    pub call_depth: usize,
    pub missing_account: usize,
    pub reentrancy_not_allowed: usize,
    pub max_seed_length_exceeded: usize,
    pub invalid_seeds: usize,
    pub invalid_realloc: usize,
    pub computational_budget_exceeded: usize,
    pub privilege_escalation: usize,
    pub program_environment_setup_failure: usize,
    pub program_failed_to_complete: usize,
    pub program_failed_to_compile: usize,
    pub immutable: usize,
    pub incorrect_authority: usize,
    pub borsh_io_error: usize,
    pub account_not_rent_exempt: usize,
    pub invalid_account_owner: usize,
    pub arithmetic_overflow: usize,
    pub unsupported_sysvar: usize,
    pub illegal_owner: usize,
    pub max_accounts_data_size_exceeded: usize,
    pub max_accounts_exceeded: usize,
}

impl TransactionErrorMetrics {
    pub fn new() -> Self {
        Self { ..Self::default() }
    }

    pub fn accumulate(&mut self, other: &TransactionErrorMetrics) {
        saturating_add_assign!(self.total, other.total);
        saturating_add_assign!(self.account_in_use, other.account_in_use);
        saturating_add_assign!(self.account_loaded_twice, other.account_loaded_twice);
        saturating_add_assign!(self.account_not_found, other.account_not_found);
        saturating_add_assign!(self.blockhash_not_found, other.blockhash_not_found);
        saturating_add_assign!(self.blockhash_too_old, other.blockhash_too_old);
        saturating_add_assign!(self.call_chain_too_deep, other.call_chain_too_deep);
        saturating_add_assign!(self.already_processed, other.already_processed);
        saturating_add_assign!(self.instruction_error, other.instruction_error);
        saturating_add_assign!(self.insufficient_funds, other.insufficient_funds);
        saturating_add_assign!(self.invalid_account_for_fee, other.invalid_account_for_fee);
        saturating_add_assign!(self.invalid_account_index, other.invalid_account_index);
        saturating_add_assign!(
            self.invalid_program_for_execution,
            other.invalid_program_for_execution
        );
        saturating_add_assign!(
            self.not_allowed_during_cluster_maintenance,
            other.not_allowed_during_cluster_maintenance
        );
        saturating_add_assign!(
            self.invalid_writable_account,
            other.invalid_writable_account
        );
        saturating_add_assign!(
            self.invalid_rent_paying_account,
            other.invalid_rent_paying_account
        );
    }

    pub fn report(&self, id: u32, slot: Slot) {
        datapoint_info!(
            "banking_stage-leader_slot_transaction_errors",
            ("id", id as i64, i64),
            ("slot", slot as i64, i64),
            ("total", self.total as i64, i64),
            ("account_in_use", self.account_in_use as i64, i64),
            (
                "account_loaded_twice",
                self.account_loaded_twice as i64,
                i64
            ),
            ("account_not_found", self.account_not_found as i64, i64),
            ("blockhash_not_found", self.blockhash_not_found as i64, i64),
            ("blockhash_too_old", self.blockhash_too_old as i64, i64),
            ("call_chain_too_deep", self.call_chain_too_deep as i64, i64),
            ("already_processed", self.already_processed as i64, i64),
            ("instruction_error", self.instruction_error as i64, i64),
            ("insufficient_funds", self.insufficient_funds as i64, i64),
            (
                "invalid_account_for_fee",
                self.invalid_account_for_fee as i64,
                i64
            ),
            (
                "invalid_account_index",
                self.invalid_account_index as i64,
                i64
            ),
            (
                "invalid_program_for_execution",
                self.invalid_program_for_execution as i64,
                i64
            ),
            (
                "not_allowed_during_cluster_maintenance",
                self.not_allowed_during_cluster_maintenance as i64,
                i64
            ),
            (
                "invalid_writable_account",
                self.invalid_writable_account as i64,
                i64
            ),
            (
                "invalid_rent_paying_account",
                self.invalid_rent_paying_account as i64,
                i64
            ),
        );
    }
}

impl InstructionErrorMetrics {
    pub fn increment(&mut self, error_type: &InstructionError) {
        match error_type.clone() {
            InstructionError::GenericError => self.generic_error += 1,
            InstructionError::InvalidArgument => self.generic_error += 1,
            InstructionError::InvalidInstructionData => self.generic_error += 1,
            InstructionError::InvalidAccountData => self.generic_error += 1,
            InstructionError::AccountDataTooSmall => self.generic_error += 1,
            InstructionError::InsufficientFunds => self.generic_error += 1,
            InstructionError::IncorrectProgramId => self.generic_error += 1,
            InstructionError::MissingRequiredSignature => self.generic_error += 1,
            InstructionError::AccountAlreadyInitialized => self.generic_error += 1,
            InstructionError::UninitializedAccount => self.generic_error += 1,
            InstructionError::UnbalancedInstruction => self.generic_error += 1,
            InstructionError::ModifiedProgramId => self.generic_error += 1,
            InstructionError::ExternalAccountLamportSpend => self.generic_error += 1,
            InstructionError::ExternalAccountDataModified => self.generic_error += 1,
            InstructionError::ReadonlyLamportChange => self.generic_error += 1,
            InstructionError::ReadonlyDataModified => self.generic_error += 1,
            InstructionError::DuplicateAccountIndex => self.generic_error += 1,
            InstructionError::ExecutableModified => self.generic_error += 1,
            InstructionError::RentEpochModified => self.generic_error += 1,
            InstructionError::NotEnoughAccountKeys => self.generic_error += 1,
            InstructionError::AccountDataSizeChanged => self.generic_error += 1,
            InstructionError::AccountNotExecutable => self.generic_error += 1,
            InstructionError::AccountBorrowFailed => self.generic_error += 1,
            InstructionError::AccountBorrowOutstanding => self.generic_error += 1,
            InstructionError::DuplicateAccountOutOfSync => self.generic_error += 1,
            InstructionError::Custom(_) => self.generic_error += 1,
            InstructionError::InvalidError => self.generic_error += 1,
            InstructionError::ExecutableDataModified => self.generic_error += 1,
            InstructionError::ExecutableLamportChange => self.generic_error += 1,
            InstructionError::ExecutableAccountNotRentExempt => self.generic_error += 1,
            InstructionError::UnsupportedProgramId => self.generic_error += 1,
            InstructionError::CallDepth => self.generic_error += 1,
            InstructionError::MissingAccount => self.generic_error += 1,
            InstructionError::ReentrancyNotAllowed => self.generic_error += 1,
            InstructionError::MaxSeedLengthExceeded => self.generic_error += 1,
            InstructionError::InvalidSeeds => self.generic_error += 1,
            InstructionError::InvalidRealloc => self.generic_error += 1,
            InstructionError::ComputationalBudgetExceeded => self.generic_error += 1,
            InstructionError::PrivilegeEscalation => self.generic_error += 1,
            InstructionError::ProgramEnvironmentSetupFailure => self.generic_error += 1,
            InstructionError::ProgramFailedToComplete => self.generic_error += 1,
            InstructionError::ProgramFailedToCompile => self.generic_error += 1,
            InstructionError::Immutable => self.generic_error += 1,
            InstructionError::IncorrectAuthority => self.generic_error += 1,
            InstructionError::BorshIoError(_) => self.generic_error += 1,
            InstructionError::AccountNotRentExempt => self.generic_error += 1,
            InstructionError::InvalidAccountOwner => self.generic_error += 1,
            InstructionError::ArithmeticOverflow => self.generic_error += 1,
            InstructionError::UnsupportedSysvar => self.generic_error += 1,
            InstructionError::IllegalOwner => self.generic_error += 1,
            InstructionError::MaxAccountsDataSizeExceeded => self.generic_error += 1,
            InstructionError::MaxAccountsExceeded => self.generic_error += 1,
        }
    }

    pub fn accumulate(&mut self, other: &InstructionErrorMetrics) {
        saturating_add_assign!(self.generic_error, other.generic_error);
        saturating_add_assign!(self.invalid_argument, other.invalid_argument);
        saturating_add_assign!(
            self.invalid_instruction_data,
            other.invalid_instruction_data
        );
        saturating_add_assign!(self.invalid_account_data, other.invalid_account_data);
        saturating_add_assign!(self.account_data_too_small, other.account_data_too_small);
        saturating_add_assign!(self.insufficient_funds, other.insufficient_funds);
        saturating_add_assign!(self.incorrect_program_id, other.incorrect_program_id);
        saturating_add_assign!(
            self.missing_required_signature,
            other.missing_required_signature
        );
        saturating_add_assign!(
            self.account_already_initialized,
            other.account_already_initialized
        );
        saturating_add_assign!(self.uninitialized_account, other.uninitialized_account);
        saturating_add_assign!(self.unbalanced_instruction, other.unbalanced_instruction);
        saturating_add_assign!(self.modified_program_id, other.modified_program_id);
        saturating_add_assign!(
            self.external_account_lamport_spend,
            other.external_account_lamport_spend
        );
        saturating_add_assign!(
            self.external_account_data_modified,
            other.external_account_data_modified
        );
        saturating_add_assign!(self.readonly_lamport_change, other.readonly_lamport_change);
        saturating_add_assign!(self.readonly_data_modified, other.readonly_data_modified);
        saturating_add_assign!(self.duplicate_account_index, other.duplicate_account_index);
        saturating_add_assign!(self.executable_modified, other.executable_modified);
        saturating_add_assign!(self.rent_epoch_modified, other.rent_epoch_modified);
        saturating_add_assign!(self.not_enough_account_keys, other.not_enough_account_keys);
        saturating_add_assign!(
            self.account_data_size_changed,
            other.account_data_size_changed
        );
        saturating_add_assign!(self.account_not_executable, other.account_not_executable);
        saturating_add_assign!(self.account_borrow_failed, other.account_borrow_failed);
        saturating_add_assign!(
            self.account_borrow_outstanding,
            other.account_borrow_outstanding
        );
        saturating_add_assign!(
            self.duplicate_account_out_of_sync,
            other.duplicate_account_out_of_sync
        );
        saturating_add_assign!(self.custom, other.custom);
        saturating_add_assign!(self.invalid_error, other.invalid_error);
        saturating_add_assign!(
            self.executable_data_modified,
            other.executable_data_modified
        );
        saturating_add_assign!(
            self.executable_lamport_change,
            other.executable_lamport_change
        );
        saturating_add_assign!(
            self.executable_account_not_rent_exempt,
            other.executable_account_not_rent_exempt
        );
        saturating_add_assign!(self.unsupported_program_id, other.unsupported_program_id);
        saturating_add_assign!(self.call_depth, other.call_depth);
        saturating_add_assign!(self.missing_account, other.missing_account);
        saturating_add_assign!(self.reentrancy_not_allowed, other.reentrancy_not_allowed);
        saturating_add_assign!(
            self.max_seed_length_exceeded,
            other.max_seed_length_exceeded
        );
        saturating_add_assign!(self.invalid_seeds, other.invalid_seeds);
        saturating_add_assign!(self.invalid_realloc, other.invalid_realloc);
        saturating_add_assign!(
            self.computational_budget_exceeded,
            other.computational_budget_exceeded
        );
        saturating_add_assign!(self.privilege_escalation, other.privilege_escalation);
        saturating_add_assign!(
            self.program_environment_setup_failure,
            other.program_environment_setup_failure
        );
        saturating_add_assign!(
            self.program_failed_to_complete,
            other.program_failed_to_complete
        );
        saturating_add_assign!(
            self.program_failed_to_compile,
            other.program_failed_to_compile
        );
        saturating_add_assign!(self.immutable, other.immutable);
        saturating_add_assign!(self.incorrect_authority, other.incorrect_authority);
        saturating_add_assign!(self.borsh_io_error, other.borsh_io_error);
        saturating_add_assign!(self.account_not_rent_exempt, other.account_not_rent_exempt);
        saturating_add_assign!(self.invalid_account_owner, other.invalid_account_owner);
        saturating_add_assign!(self.arithmetic_overflow, other.arithmetic_overflow);
        saturating_add_assign!(self.unsupported_sysvar, other.unsupported_sysvar);
        saturating_add_assign!(self.illegal_owner, other.illegal_owner);
        saturating_add_assign!(
            self.max_accounts_data_size_exceeded,
            other.max_accounts_data_size_exceeded
        );
        saturating_add_assign!(self.max_accounts_exceeded, other.max_accounts_exceeded);
    }

    pub fn report(&self, id: u32, slot: Slot) {
        datapoint_info!(
            "banking_stage-leader_slot_transaction_errors",
            ("id", id as i64, i64),
            ("slot", slot as i64, i64),
            ("generic", self.generic_error as i64, i64),
            ("invalid_argument", self.invalid_argument as i64, i64),
            (
                "invalid_instruction_data",
                self.invalid_instruction_data as i64,
                i64
            ),
            (
                "invalid_account_data",
                self.invalid_account_data as i64,
                i64
            ),
            (
                "account_data_too_small",
                self.account_data_too_small as i64,
                i64
            ),
            ("insufficient_funds", self.insufficient_funds as i64, i64),
            (
                "incorrect_program_id",
                self.incorrect_program_id as i64,
                i64
            ),
            (
                "missing_required_signature",
                self.missing_required_signature as i64,
                i64
            ),
            (
                "account_already_initialized",
                self.account_already_initialized as i64,
                i64
            ),
            (
                "uninitialized_account",
                self.uninitialized_account as i64,
                i64
            ),
            (
                "unbalanced_instruction",
                self.unbalanced_instruction as i64,
                i64
            ),
            ("modified_program_id", self.modified_program_id as i64, i64),
            (
                "external_account_lamport_spend",
                self.external_account_lamport_spend as i64,
                i64
            ),
            (
                "external_account_data_modified",
                self.external_account_data_modified as i64,
                i64
            ),
            (
                "readonly_lamport_change",
                self.readonly_lamport_change as i64,
                i64
            ),
            (
                "readonly_data_modified",
                self.readonly_data_modified as i64,
                i64
            ),
            (
                "duplicate_account_index",
                self.duplicate_account_index as i64,
                i64
            ),
            ("executable_modified", self.executable_modified as i64, i64),
            ("rent_epoch_modified", self.rent_epoch_modified as i64, i64),
            (
                "not_enough_account_keys",
                self.not_enough_account_keys as i64,
                i64
            ),
            (
                "account_data_size_changed",
                self.account_data_size_changed as i64,
                i64
            ),
            (
                "account_not_executable",
                self.account_not_executable as i64,
                i64
            ),
            (
                "account_borrow_failed",
                self.account_borrow_failed as i64,
                i64
            ),
            (
                "account_borrow_outstanding",
                self.account_borrow_outstanding as i64,
                i64
            ),
            (
                "duplicate_account_out_of_sync",
                self.duplicate_account_out_of_sync as i64,
                i64
            ),
            ("custom", self.custom as i64, i64),
            ("invalid_error", self.invalid_error as i64, i64),
            (
                "executable_data_modified",
                self.executable_data_modified as i64,
                i64
            ),
            (
                "executable_lamport_change",
                self.executable_lamport_change as i64,
                i64
            ),
            (
                "executable_account_not_rent_exempt",
                self.executable_account_not_rent_exempt as i64,
                i64
            ),
            (
                "unsupported_program_id",
                self.unsupported_program_id as i64,
                i64
            ),
            ("call_depth", self.call_depth as i64, i64),
            ("missing_account", self.missing_account as i64, i64),
            (
                "reentrancy_not_allowed",
                self.reentrancy_not_allowed as i64,
                i64
            ),
            (
                "max_seed_length_exceeded",
                self.max_seed_length_exceeded as i64,
                i64
            ),
            ("invalid_seeds", self.invalid_seeds as i64, i64),
            ("invalid_realloc", self.invalid_realloc as i64, i64),
            (
                "computational_budget_exceeded",
                self.computational_budget_exceeded as i64,
                i64
            ),
            (
                "privilege_escalation",
                self.privilege_escalation as i64,
                i64
            ),
            (
                "program_environment_setup_failure",
                self.program_environment_setup_failure as i64,
                i64
            ),
            (
                "program_failed_to_complete",
                self.program_failed_to_complete as i64,
                i64
            ),
            (
                "program_failed_to_compile",
                self.program_failed_to_compile as i64,
                i64
            ),
            ("immutable", self.immutable as i64, i64),
            ("incorrect_authority", self.incorrect_authority as i64, i64),
            ("borsh_io_error", self.borsh_io_error as i64, i64),
            (
                "account_not_rent_exempt",
                self.account_not_rent_exempt as i64,
                i64
            ),
            (
                "invalid_account_owner",
                self.invalid_account_owner as i64,
                i64
            ),
            ("arithmetic_overflow", self.arithmetic_overflow as i64, i64),
            ("unsupported_sysvar", self.unsupported_sysvar as i64, i64),
            ("illegal_owner", self.illegal_owner as i64, i64),
            (
                "max_accounts_data_size_exceeded",
                self.max_accounts_data_size_exceeded as i64,
                i64
            ),
            (
                "max_accounts_exceeded",
                self.max_accounts_exceeded as i64,
                i64
            ),
        );
    }
}

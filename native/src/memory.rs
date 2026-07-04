use crate::*;

/// Managed-memory accounting for one native operator handle: a reservation against a bounded pool
/// sized by the budget the host reserved for the operator, plus the incrementally tracked estimate
/// of the state's heap footprint. Unaccounted (no budget) it is inert — the tracking branches cost
/// one predicted-false test per touch. `account()` resizes the reservation to the tracked bytes; a
/// denial is the budget-exceeded signal, surfaced to the host as a clear failure instead of the
/// container OOM-killing the process (these operators have no runtime spill to fall back to).
pub(crate) struct OperatorMemory {
    reservation: Option<MemoryReservation>,
    pub(crate) state_bytes: usize,
    // A TaskContext sharing the bounded pool, for fragments the operator delegates to DataFusion's
    // execution (the joins' HashJoinExec) — their transient working memory then draws on the same
    // budget as the operator's state.
    task_ctx: Option<Arc<TaskContext>>,
}

impl OperatorMemory {
    pub(crate) fn unaccounted() -> Self {
        OperatorMemory { reservation: None, state_bytes: 0, task_ctx: None }
    }

    /// Attaches a budget (negative = unaccounted), accounting `current_state_bytes` immediately —
    /// the restore path, where state rebuilt from a snapshot must fit the budget up front.
    pub(crate) fn attach(
        &mut self,
        consumer: &str,
        budget_bytes: i64,
        current_state_bytes: usize,
    ) -> Result<(), DataFusionError> {
        if budget_bytes < 0 {
            return Ok(());
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget_bytes as usize));
        self.attach_pool(consumer, &pool, current_state_bytes)
    }

    /// [`attach`](Self::attach) against a caller-owned pool (shared in tests to observe the pool's
    /// balance from outside).
    pub(crate) fn attach_pool(
        &mut self,
        consumer: &str,
        pool: &Arc<dyn MemoryPool>,
        current_state_bytes: usize,
    ) -> Result<(), DataFusionError> {
        self.reservation = Some(MemoryConsumer::new(consumer.to_string()).register(pool));
        let runtime = RuntimeEnvBuilder::new().with_memory_pool(Arc::clone(pool)).build_arc()?;
        self.task_ctx = Some(Arc::new(TaskContext::default().with_runtime(runtime)));
        self.state_bytes = current_state_bytes;
        self.account()
    }

    /// Whether a budget is attached — gate for any per-touch measurement work.
    pub(crate) fn tracking(&self) -> bool {
        self.reservation.is_some()
    }

    /// The TaskContext DataFusion-executed fragments must run under: pool-bounded when a budget is
    /// attached, a plain default (unbounded, as before accounting) otherwise.
    pub(crate) fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx.clone().unwrap_or_default()
    }

    /// Folds a touched entry's footprint change into the tracked total.
    pub(crate) fn record(&mut self, delta: isize) {
        self.state_bytes = self.state_bytes.saturating_add_signed(delta);
    }

    /// Removes a dropped entry's footprint (an eviction or flush).
    pub(crate) fn forget(&mut self, bytes: usize) {
        self.state_bytes = self.state_bytes.saturating_sub(bytes);
    }

    /// Replaces the tracked total — for mutation paths that rebuild whole containers (an eviction
    /// that reslices buffered batches) where recomputing is cheaper than delta bookkeeping.
    pub(crate) fn set(&mut self, bytes: usize) {
        self.state_bytes = bytes;
    }

    pub(crate) fn account(&mut self) -> Result<(), DataFusionError> {
        let Some(reservation) = &mut self.reservation else {
            return Ok(());
        };
        reservation.try_resize(self.state_bytes).map_err(|e| {
            DataFusionError::ResourcesExhausted(format!(
                "native operator state exceeded its managed-memory budget; raise \
                 taskmanager.memory.managed.size or the operator's managed-memory weight ({e})"
            ))
        })
    }

    /// `account()` on a path where the tracked size can only have shrunk.
    pub(crate) fn account_shrink(&mut self) {
        self.account().expect("shrinking a reservation cannot fail");
    }
}

/// Total Arrow buffer footprint of a set of buffered batches.
pub(crate) fn buffered_batches_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::get_array_memory_size).sum()
}

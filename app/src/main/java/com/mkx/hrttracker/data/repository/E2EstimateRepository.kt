package com.mkx.hrttracker.data.repository

import com.mkx.hrttracker.di.AppScope
import com.mkx.hrttracker.di.DefaultDispatcher
import com.mkx.hrttracker.model.pk.E2CalibrationDisposition
import com.mkx.hrttracker.model.pk.PkCalibrationRenderResult
import com.mkx.hrttracker.model.pk.PkCalibrationResult
import com.mkx.hrttracker.model.pk.resultIdsWith
import com.mkx.hrttracker.startup.StartupTiming
import com.mkx.hrttracker.util.AppDiagnosticsLogger
import com.mkx.hrttracker.util.currentMinute
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.dropWhile
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.transformLatest
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Clock
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton

/** Why a newer estimate is being derived: a change the user made, or the clock or the system. */
enum class E2Pending { USER, SYSTEM }

/**
 * An estimate observation. [value] is the last estimate it derived or
 * admitted, null until the first. [pending] is set while the estimate for a
 * newer key is being derived: [E2Pending.USER] exactly when its inputs'
 * generation is newer than the one behind [value].
 */
data class E2State<T>(val value: T?, val pending: E2Pending? = null)

/** The calibration page's estimate section: the fit as the page renders it, and the lab flags beside it. */
data class CalibrationEstimate(
    /** Null when the solve failed or the render threw: the numeric failure, with Try again. */
    val result: PkCalibrationResult?,
    /** Null without a READY fit. */
    val render: PkCalibrationRenderResult?,
    // From the labs and review metadata the fit was solved from, stored
    // beside it, so they always match it and need no Room read.
    val acceptedLabIds: Set<UUID>,
    val excludedLabIds: Set<UUID>,
    /** Results the fit can never use (value ≤ 0). */
    val nonPositiveLabIds: Set<UUID>,
)

/**
 * The E2 estimates, derived from the snapshot's inputs and the clock by key.
 * An estimate stands in for a derivation only under the exact key it was
 * derived for, so nothing expires: when the key moves, the estimate is
 * derived again.
 */
@Singleton
@OptIn(ExperimentalCoroutinesApi::class)
class E2EstimateRepository @Inject constructor(
    private val homeSnapshotRepository: HomeSnapshotRepository,
    private val store: HomeE2EstimateStore,
    private val clock: Clock,
    @param:AppScope private val appScope: CoroutineScope,
    @param:DefaultDispatcher private val defaultDispatcher: CoroutineDispatcher,
    private val diagnosticsLogger: AppDiagnosticsLogger = AppDiagnosticsLogger(),
) {
    // The last keys of each kind derived, done or in flight, Home's with the
    // stored estimate among them: every observer of a kind, and the warm-up,
    // share them, so no key is derived twice. Each guarded by itself.
    private val homeMemo = lastKeys<HomeEstimateKey, Deferred<Derivation<HomeE2Estimate>>>()
    private val calibrationMemo = lastKeys<CalibrationRenderKey, Deferred<Derivation<CalibrationEstimate>>>()

    @Volatile
    private var storedKey: HomeEstimateKey? = null
    private val writeMutex = Mutex()

    /**
     * The newest Home estimate an observation admitted or derived, seeded
     * with the stored one; null until the first. For readers exempt from the
     * skeleton (the Settings row), which keep their last value and update it
     * in place. A warm-up leaves it alone: one for an older key can land
     * after an observation's newer value.
     */
    val latestHome: StateFlow<HomeE2Estimate?>
        get() = latestHomeState
    private val latestHomeState = MutableStateFlow<HomeE2Estimate?>(null)

    // Set while the snapshot's reads fail, so a streak is logged once.
    private var inputsFailing = false

    // The snapshot as observeHomeSnapshot last reported it (null behind the
    // durable generation), wrapped: null until the first read. Held in
    // memory, so a restarted observation's first state needs no disk read.
    private val reported: StateFlow<Reported?> = homeSnapshotRepository.observeHomeSnapshot()
        .map(::Reported)
        .onEach { inputsFailing = false }
        // A failed read (DataStore's own IOException, outside the serializer)
        // reads as no inputs, so Home's first frame still comes, with the
        // skeleton, and is retried until the snapshot reads again. appScope
        // has no handler, so nothing may escape here.
        .retryWhen { cause, _ ->
            if (!inputsFailing) diagnosticsLogger.warning(TAG, "home_e2_estimate_inputs_failed", cause)
            inputsFailing = true
            emit(Reported(null))
            delay(INPUTS_RETRY_DELAY_MILLIS)
            true
        }
        .stateIn(appScope, SharingStarted.Eagerly, null)

    // The stored estimate seeds the memo before any observation starts, so a
    // match is in the first frame.
    private val loaded: Deferred<Unit> = appScope.async {
        val stored = try {
            store.read()
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (exception: Exception) {
            diagnosticsLogger.warning(TAG, "home_e2_estimate_read_failed", exception)
            null
        }
        if (stored != null) {
            synchronized(homeMemo) {
                homeMemo[stored.key] = CompletableDeferred(Derivation(stored.estimate, storable = true))
            }
            storedKey = stored.key
            latestHomeState.value = stored.estimate
        }
        StartupTiming.mark("home_e2_estimate_loaded")
    }

    /**
     * Home's estimate at [now]. Each observation starts cold: its first
     * state, after the stored estimate's load, uses the published inputs and
     * a fresh clock read, not [now]'s cached minute, which can lag on a
     * resume. It admits a value only when those inputs are current and the
     * memo has their key; otherwise it has none until the estimate is
     * derived, and asks for a rebuild while the inputs aren't current. A
     * value, once shown, is kept while a newer one is pending, including
     * while a mutation's snapshot is cleared.
     */
    fun observeHome(now: Flow<LocalDateTime>): Flow<E2State<HomeE2Estimate>> = observe(
        now = now,
        keyOf = E2Derivation::homeEstimateKey,
        derivationOf = { key, record, minute, zoneId -> homeDerivation(key, record.e2Inputs(), minute, zoneId) },
        onAdmitted = { estimate -> latestHomeState.value = estimate },
    )

    /**
     * The calibration page's estimate section at [now], under the same rules
     * as [observeHome]: shared derivations, a cold start, nothing from inputs
     * that aren't current, [E2Pending.USER] for a change the user made. It
     * is never stored, so a process's first observation derives it. A render
     * that throws gives the numeric failure, which is kept like any other
     * value until the key moves.
     */
    fun observeCalibration(now: Flow<LocalDateTime>): Flow<E2State<CalibrationEstimate>> = observe(
        now = now,
        keyOf = E2Derivation::calibrationRenderKey,
        derivationOf = { key, record, minute, zoneId ->
            calibrationMemo.derivation(key) { deriveCalibration(key, record, minute, zoneId) }
        },
    )

    /** One kind of estimate, observed as [observeHome] describes. */
    private fun <K, T : Any> observe(
        now: Flow<LocalDateTime>,
        keyOf: (E2Inputs, LocalDateTime, ZoneId) -> K,
        derivationOf: (K, HomeSnapshotRecord, LocalDateTime, ZoneId) -> Deferred<Derivation<T>>,
        onAdmitted: (T) -> Unit = {},
    ): Flow<E2State<T>> = flow {
        loaded.await()
        val start = currentMinute(clock)
        var state = E2State<T>(null)
        var shownKey: K? = null
        // The generation of the inputs behind the value.
        var shownGeneration = 0L
        val minutes = now
            .dropWhile { minute -> minute < start }
            .onStart { emit(start) }
            .distinctUntilChanged()
        emitAll(
            combine(reported.filterNotNull(), minutes) { reported, minute -> reported.record to minute }
                .transformLatest { (record, minute) ->
                    val zoneId = ZoneId.systemDefault()
                    val current = record?.takeIf { homeSnapshotRepository.isSnapshotUsable(it, minute, zoneId) }
                    if (current == null) {
                        // Behind a mutation, or invalid: nothing to derive
                        // from until a rebuild publishes current inputs.
                        if (!homeSnapshotRepository.rebuildInFlight.first()) {
                            homeSnapshotRepository.refreshHomeSnapshotAsync()
                        }
                        state = state.copy(pending = null)
                        emit(state)
                        return@transformLatest
                    }
                    val key = keyOf(current.e2Inputs(), minute, zoneId)
                    if (key == shownKey) {
                        // A change the estimate doesn't read: nothing pending.
                        shownGeneration = current.generation
                        state = state.copy(pending = null)
                        emit(state)
                        return@transformLatest
                    }
                    val derivation = derivationOf(key, current, minute, zoneId)
                    if (!derivation.isCompleted) {
                        val pending = if (state.value != null && current.generation > shownGeneration) {
                            E2Pending.USER
                        } else {
                            E2Pending.SYSTEM
                        }
                        emit(state.copy(pending = pending))
                    }
                    val value = derivation.await().value
                    // The change behind it is spent either way: the clock
                    // moving after it is the system's.
                    shownGeneration = current.generation
                    state = if (value == null) {
                        // A failed derivation keeps the last value.
                        state.copy(pending = null)
                    } else {
                        shownKey = key
                        onAdmitted(value)
                        E2State(value)
                    }
                    emit(state)
                }
        )
    }.distinctUntilChanged()

    /**
     * Derives and stores Home's estimate for the current inputs and a fresh
     * clock read, so the next open shows it in its first frame. Background
     * jobs that move Home's key call it. Inputs that aren't current are left
     * alone: the rebuild publishing them warms when it lands. Costs a key
     * when the stored estimate has it already.
     */
    suspend fun warmHome() {
        try {
            loaded.await()
            val now = currentMinute(clock)
            val zoneId = ZoneId.systemDefault()
            val record = homeSnapshotRepository.readUsableHomeSnapshot(now) ?: return
            val inputs = record.e2Inputs()
            val key = E2Derivation.homeEstimateKey(inputs, now, zoneId)
            if (key == storedKey) return
            val derived = homeDerivation(key, inputs, now, zoneId).await()
            val estimate = derived.value ?: return
            if (derived.storable) persist(HomeE2EstimateRecord(key, estimate))
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (exception: Exception) {
            diagnosticsLogger.warning(TAG, "home_e2_estimate_warm_up_failed", exception)
        }
    }

    private fun homeDerivation(
        key: HomeEstimateKey,
        inputs: E2Inputs,
        now: LocalDateTime,
        zoneId: ZoneId,
    ): Deferred<Derivation<HomeE2Estimate>> = homeMemo.derivation(key) {
        deriveHome(key, inputs, now, zoneId).also { derived ->
            // Written behind, outside this derivation, so no one waiting for
            // the estimate waits for the disk.
            if (derived.storable) {
                appScope.launch { persist(HomeE2EstimateRecord(key, checkNotNull(derived.value))) }
            }
        }
    }

    /** The memo's derivation of [key], started on [defaultDispatcher] on a miss. */
    private fun <K, T> MutableMap<K, Deferred<Derivation<T>>>.derivation(
        key: K,
        derive: () -> Derivation<T>,
    ): Deferred<Derivation<T>> = synchronized(this) {
        getOrPut(key) { appScope.async(defaultDispatcher) { derive() } }
    }

    private fun deriveHome(
        key: HomeEstimateKey,
        inputs: E2Inputs,
        now: LocalDateTime,
        zoneId: ZoneId,
    ): Derivation<HomeE2Estimate> {
        val startNanos = System.nanoTime()
        var bandFailed = false
        val estimate = try {
            E2Derivation.deriveHomeEstimate(inputs, now, zoneId) { throwable ->
                bandFailed = true
                diagnosticsLogger.warning(TAG, "home_e2_estimate_band_failed", throwable)
            }
        } catch (exception: Exception) {
            diagnosticsLogger.warning(TAG, "home_e2_estimate_failed", exception)
            null
        }
        diagnosticsLogger.info(
            TAG,
            "home_e2_estimate_derived ms=${(System.nanoTime() - startNanos) / 1_000_000L} " +
                    "option=${key.option} windowDate=${key.windowDate} " +
                    "failed=${estimate == null} bandFailed=$bandFailed"
        )
        return Derivation(estimate, storable = estimate != null && !bandFailed)
    }

    private fun deriveCalibration(
        key: CalibrationRenderKey,
        record: HomeSnapshotRecord,
        now: LocalDateTime,
        zoneId: ZoneId,
    ): Derivation<CalibrationEstimate> {
        val startNanos = System.nanoTime()
        val inputs = record.e2Inputs()
        var failed = false
        val render = try {
            E2Derivation.deriveCalibrationRender(inputs, now, zoneId)
        } catch (exception: Exception) {
            // Shown as the numeric failure with Try again; the log is the
            // only trace of why.
            failed = true
            diagnosticsLogger.warning(TAG, "calibration_render_failed", exception)
            null
        }
        diagnosticsLogger.info(
            TAG,
            "calibration_render_derived ms=${(System.nanoTime() - startNanos) / 1_000_000L} " +
                    "option=${key.option} windowDate=${key.windowDate} failed=$failed"
        )
        return Derivation(
            CalibrationEstimate(
                result = (inputs.fit?.outcome as? FitOutcome.Solved)?.result?.takeUnless { failed },
                render = render,
                acceptedLabIds = record.labMetadata.resultIdsWith(E2CalibrationDisposition.REVIEWED),
                excludedLabIds = record.labMetadata.resultIdsWith(E2CalibrationDisposition.EXCLUDED),
                nonPositiveLabIds = record.labs
                    .filter { lab -> lab.valuePgml <= 0.0 }
                    .mapTo(HashSet()) { lab -> lab.resultId },
            ),
        )
    }

    /**
     * Stores [record] unless it is stored already, or its key is no longer
     * the one the current inputs and a fresh clock read give: a slow
     * derivation must not overwrite a newer estimate.
     */
    private suspend fun persist(record: HomeE2EstimateRecord) {
        try {
            writeMutex.withLock {
                if (record.key == storedKey) return
                val now = currentMinute(clock)
                val current = homeSnapshotRepository.readUsableHomeSnapshot(now)
                    ?.let { snapshot -> E2Derivation.homeEstimateKey(snapshot.e2Inputs(), now, ZoneId.systemDefault()) }
                if (current != record.key) {
                    diagnosticsLogger.info(TAG, "home_e2_estimate_write_skipped reason=key_moved")
                    return
                }
                store.write(record)
                storedKey = record.key
            }
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (exception: Exception) {
            diagnosticsLogger.warning(TAG, "home_e2_estimate_write_failed", exception)
        }
    }

    private class Reported(val record: HomeSnapshotRecord?)

    /**
     * One derivation. Home's has a null [value] when it threw; the calibration
     * page's always has one, the numeric failure when its render threw. Not
     * [storable] when Home's, or its band render, threw: such a result is kept
     * in the memo, so it isn't derived again on every tick, but never stored,
     * so a restart tries again. Only Home's estimate is ever stored.
     */
    private class Derivation<T>(val value: T?, val storable: Boolean = false)

    private companion object {
        const val TAG = "E2EstimateRepository"
        const val MEMO_SIZE = 2
        const val INPUTS_RETRY_DELAY_MILLIS = 1_000L

        /** Access-ordered: keeps the [MEMO_SIZE] keys used last. */
        fun <K, V> lastKeys(): MutableMap<K, V> = object : LinkedHashMap<K, V>(4, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean = size > MEMO_SIZE
        }
    }
}

/** What the snapshot's E2 estimates are derived from. */
internal fun HomeSnapshotRecord.e2Inputs() = E2Inputs(
    estradiolEntries = pkEntries,
    plannedSources = activeGroups,
    weightKg = userProfile?.weightKg,
    option = option,
    fit = fit,
)

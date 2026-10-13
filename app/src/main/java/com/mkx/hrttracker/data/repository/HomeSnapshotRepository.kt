package com.mkx.hrttracker.data.repository

import com.mkx.hrttracker.model.bloodtest.BloodAnalyteKey
import com.mkx.hrttracker.model.pk.E2CalibrationMetadata
import com.mkx.hrttracker.model.pk.PkCalibrationEngine
import com.mkx.hrttracker.model.pk.PkCalibrationLab
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import com.mkx.hrttracker.data.local.DatabaseHolder
import com.mkx.hrttracker.di.AppScope
import com.mkx.hrttracker.di.DefaultDispatcher
import com.mkx.hrttracker.model.journal.TrackedDate
import com.mkx.hrttracker.model.medication.MedicationGroup
import com.mkx.hrttracker.model.medication.MedicationLogEntry
import com.mkx.hrttracker.model.medication.Medicine
import com.mkx.hrttracker.model.personalization.UserProfile
import com.mkx.hrttracker.model.pk.HomeE2ChartWindowOption
import com.mkx.hrttracker.model.pk.PkProjectionResult
import com.mkx.hrttracker.model.pk.projectionFutureDays
import com.mkx.hrttracker.startup.StartupTiming
import com.mkx.hrttracker.util.AppDiagnosticsLogger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton

@Singleton
class HomeSnapshotRepository @Inject constructor(
    private val databaseHolder: DatabaseHolder,
    private val homeSnapshotStore: HomeSnapshotStore,
    private val homeSnapshotGenerationStore: HomeSnapshotGenerationStore,
    private val settingsRepository: SettingsRepository,
    // A provider: the estimator reads its inputs from this repository.
    private val e2EstimateRepository: Provider<E2EstimateRepository>,
    @param:AppScope private val appScope: CoroutineScope,
    @param:DefaultDispatcher private val defaultDispatcher: CoroutineDispatcher,
    private val diagnosticsLogger: AppDiagnosticsLogger = AppDiagnosticsLogger(),
) {
    private val refreshMutex = Mutex()
    private val snapshotMutationMutex = Mutex()

    /**
     * The fit epoch Try again asks for. A build re-solves while the stored
     * fit's epoch is below the value it read when it started, and writes a
     * fit it solves with that value (a reused fit keeps its own, never
     * lower), so a retry outlives joined, superseded and dropped builds, and
     * its new epoch changes every estimate key built on the fit. Starts at
     * the stored fit's epoch, read before anything clears the snapshot; null
     * until then. Guarded by [refreshMutex].
     */
    private var requestedFitEpoch: Int? = null

    /**
     * The fit of the last snapshot written, for the next build to reuse. Kept
     * here because a mutation clears the stored snapshot before its build
     * runs. Starts at the stored one, read with [requestedFitEpoch].
     */
    @Volatile
    private var storedFit: E2FitRecord? = null

    /**
     * True while a refresh is pending or building, from a mutation's clear (or
     * an async request) until its build has written, and warmed Home's
     * estimate, or been skipped.
     */
    val rebuildInFlight: Flow<Boolean>
        get() = activeRefreshes.map { count -> count > 0 }.distinctUntilChanged()
    private val activeRefreshes = MutableStateFlow(0)

    /**
     * Waits until no rebuild is pending or building, the warm-up that ends
     * each build included: the "Log now" receiver and the widget's quick log
     * finish only once their mutation's estimate is stored.
     */
    suspend fun awaitRebuild() {
        activeRefreshes.first { count -> count == 0 }
    }

    fun observeHomeSnapshot(): Flow<HomeSnapshotRecord?> {
        return combine(
            homeSnapshotStore.observeSnapshot(),
            homeSnapshotGenerationStore.observeGeneration(),
        ) { snapshot, generation ->
            when {
                snapshot == null -> {
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_observed_missing durableGeneration=$generation"
                    )
                    null
                }

                snapshot.generation < generation -> {
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_observed_rejected reason=generation " +
                                "snapshotGeneration=${snapshot.generation} durableGeneration=$generation"
                    )
                    null
                }

                else -> {
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_observed_loaded ${snapshot.diagnosticSummary()} " +
                                "durableGeneration=$generation"
                    )
                    snapshot
                }
            }
        }
            .distinctUntilChanged()
            .onEach { snapshot ->
                StartupTiming.mark(
                    if (snapshot == null) {
                        "home_snapshot_missing"
                    } else {
                        "home_snapshot_loaded"
                    }
                )
            }
    }

    // All medication group, medication log, and profile writes that affect Home data
    // must go through this gate so stale snapshots are rejected before the DB changes.
    //
    // The work inside the lock runs in NonCancellable: once we bump the generation
    // and clear the snapshot, the new snapshot MUST be rewritten — otherwise a
    // caller whose scope dies mid-mutation (e.g. an OnboardingViewModel's
    // viewModelScope when the user finishes onboarding right after saving weight)
    // leaves the snapshot store empty and the next launch reads no profile until
    // Room finishes opening. Lock acquisition stays cancellable so callers blocked
    // on a long-running peer can still be cancelled cleanly.
    //
    // The rebuild itself runs in the app scope after the lock is released: the
    // caller returns as soon as the DB commit lands instead of waiting through
    // the calibration solve, simulation and encrypted write, and other
    // mutations are not queued behind that build. Observers see the cleared
    // snapshot briefly: Home's lists come from Room and its E2 estimate keeps
    // its value until the rebuild writes.
    suspend fun <T> runHomeDataMutation(block: suspend () -> T): T {
        var generation = 0L
        var mutationResult: Result<T>? = null
        refreshMutex.withLock {
            withContext(NonCancellable) {
                diagnosticsLogger.info(TAG, "home_data_mutation_start")
                // Counted as in flight before the generation bump: the bump
                // alone makes observers reject the stored snapshot, and they
                // must already see its rebuild coming when that happens (the
                // estimator would request another).
                activeRefreshes.update { count -> count + 1 }
                generation = try {
                    homeSnapshotGenerationStore.incrementGeneration()
                } catch (throwable: Throwable) {
                    // No block ran and no refresh will be launched to drop
                    // it, so its readers would wait forever.
                    activeRefreshes.update { count -> count - 1 }
                    // The only trace of a write, or a Try again, that never ran.
                    diagnosticsLogger.warning(TAG, "home_data_mutation_generation_failed", throwable)
                    throw throwable
                }
                // Before the clear below drops the stored fit they start from.
                requestedFitEpochLocked()
                diagnosticsLogger.info(
                    TAG,
                    "home_data_mutation_generation_incremented generation=$generation"
                )
                val result = runCatching { block() }
                mutationResult = result
                result.onSuccess {
                    diagnosticsLogger.info(
                        TAG,
                        "home_data_mutation_committed generation=$generation"
                    )
                }
                // The generation has already been bumped, so observers will reject any
                // stale snapshot. Clear while still serialized with other mutations.
                clearSnapshotBestEffort()
            }
        }
        launchCountedRefresh(now = LocalDateTime.now(), force = true, zoneId = ZoneId.systemDefault())
        diagnosticsLogger.info(
            TAG,
            "home_data_mutation_snapshot_refresh_enqueued generation=$generation"
        )
        return checkNotNull(mutationResult).getOrThrow()
    }

    /**
     * Calibration's Try again: a gated change like any other, so screens hold
     * on it as a user change, that also raises [requestedFitEpoch]. Builds
     * re-solve until one writes its fit with the new epoch, so every estimate
     * built on the fit is derived again, even from unchanged inputs.
     */
    suspend fun retryCalibration() {
        runHomeDataMutation { requestedFitEpoch = requestedFitEpochLocked() + 1 }
    }

    /** Guarded by [refreshMutex]. Reads the stored fit once, into [storedFit] too. */
    private suspend fun requestedFitEpochLocked(): Int = requestedFitEpoch ?: run {
        val stored = try {
            homeSnapshotStore.readSnapshot()?.fit
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (exception: Exception) {
            diagnosticsLogger.warning(TAG, "home_snapshot_fit_epoch_read_failed", exception)
            null
        }
        storedFit = stored
        (stored?.epoch ?: 0).also { epoch -> requestedFitEpoch = epoch }
    }

    /** The snapshot's own validity, apart from the generation: schema, engine, zone and date window. */
    fun isSnapshotUsable(
        snapshot: HomeSnapshotRecord,
        now: LocalDateTime,
        zoneId: ZoneId = ZoneId.systemDefault(),
    ): Boolean {
        return snapshotUsabilityFailure(
            snapshot = snapshot,
            now = now,
            zoneId = zoneId,
        ) == null
    }

    suspend fun readUsableHomeSnapshot(
        now: LocalDateTime = LocalDateTime.now(),
    ): HomeSnapshotRecord? {
        val snapshot = homeSnapshotStore.readSnapshot()
            ?: return null.also {
                diagnosticsLogger.info(TAG, "home_snapshot_read_missing now=$now")
            }
        val generation = homeSnapshotGenerationStore.readGeneration()
        if (snapshot.generation < generation) {
            diagnosticsLogger.info(
                TAG,
                "home_snapshot_read_rejected reason=generation " +
                        "snapshotGeneration=${snapshot.generation} durableGeneration=$generation now=$now"
            )
            return null
        }
        val usabilityFailure = snapshotUsabilityFailure(
            snapshot = snapshot,
            now = now,
            zoneId = ZoneId.systemDefault(),
        )
        if (usabilityFailure != null) {
            diagnosticsLogger.info(
                TAG,
                "home_snapshot_read_rejected reason=$usabilityFailure " +
                        "${snapshot.diagnosticSummary()} now=$now"
            )
            return null
        }
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_read_usable ${snapshot.diagnosticSummary()} " +
                    "durableGeneration=$generation now=$now"
        )
        return snapshot
    }

    fun scheduleEntriesForHome(
        snapshot: HomeSnapshotRecord,
        now: LocalDateTime,
        zoneId: ZoneId = ZoneId.systemDefault(),
    ): List<MedicationLogEntry> {
        val window = HomeSnapshotWindow.forNow(now = now, zoneId = zoneId)
        return snapshot.scheduleEntries.filter { entry ->
            val scheduledFor = entry.scheduledFor
            if (scheduledFor != null) {
                scheduledFor >= window.homeScheduledStart &&
                        scheduledFor <= window.homeScheduledEnd
            } else {
                val appliedAtEpochMillis = entry.appliedAt.toEpochMilli()
                appliedAtEpochMillis >= window.homeManualStartEpochMillis &&
                        appliedAtEpochMillis < window.homeManualEndEpochMillis
            }
        }
    }

    private fun snapshotUsabilityFailure(
        snapshot: HomeSnapshotRecord,
        now: LocalDateTime,
        zoneId: ZoneId,
    ): String? {
        if (snapshot.schemaVersion != HOME_SNAPSHOT_SCHEMA_VERSION) {
            return "schema_version expected=$HOME_SNAPSHOT_SCHEMA_VERSION actual=${snapshot.schemaVersion}"
        }
        // The fit was solved, and the stored estimate keyed, by this engine.
        if (snapshot.engineVersion != PK_ENGINE_VERSION) {
            return "engine_version expected=$PK_ENGINE_VERSION actual=${snapshot.engineVersion}"
        }
        if (snapshot.zoneId != zoneId.id) {
            return "zone_id expected=${zoneId.id} actual=${snapshot.zoneId}"
        }
        val anchorDate = LocalDate.ofEpochDay(snapshot.anchorDateEpochDay)
        val today = now.toLocalDate()
        val validUntil = anchorDate.plusDays(HOME_SNAPSHOT_VALIDITY_DAYS)
        if (today.isBefore(anchorDate) || today.isAfter(validUntil)) {
            return "date_window anchorDate=$anchorDate validUntil=$validUntil today=$today"
        }
        return null
    }

    /**
     * [force] bypasses the skip-check so the snapshot is rebuilt even when the
     * stored one looks usable. System events (boot, clock / zone change,
     * package update, the midnight alarm) pass [silent], which no longer
     * changes anything: the screens' indicators follow their estimates'
     * [E2Pending.USER]. It goes with [force] when the rebuild worker replaces
     * these requests.
     */
    fun refreshHomeSnapshotAsync(
        now: LocalDateTime = LocalDateTime.now(),
        force: Boolean = false,
        zoneId: ZoneId = ZoneId.systemDefault(),
        silent: Boolean = false,
    ) {
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_refresh_async_enqueued force=$force silent=$silent now=$now"
        )
        activeRefreshes.update { count -> count + 1 }
        launchCountedRefresh(now = now, force = force, zoneId = zoneId)
    }

    /** Runs a refresh in the app scope; the caller has already counted it in [activeRefreshes]. */
    private fun launchCountedRefresh(
        now: LocalDateTime,
        force: Boolean,
        zoneId: ZoneId,
    ) {
        // Reserve the request's place in line synchronously: the launched
        // coroutine may reach the refresh mutex after a later request's.
        val sequence = refreshRequestSequence.incrementAndGet()
        appScope.launch {
            try {
                refreshHomeSnapshotIfNeeded(
                    now = now,
                    force = force,
                    zoneId = zoneId,
                    sequence = sequence,
                )
            } catch (throwable: Throwable) {
                if (throwable is CancellationException) {
                    throw throwable
                }
                diagnosticsLogger.warning(
                    TAG,
                    "home_snapshot_refresh_failed force=$force now=$now",
                    throwable
                )
            } finally {
                activeRefreshes.update { count -> count - 1 }
            }
        }
    }

    /**
     * [invalidateHomeSnapshot] followed by a forced rebuild, counted before
     * the generation bump like a mutation, so Home never sees the cleared
     * snapshot without knowing a rebuild is coming.
     */
    suspend fun invalidateAndRebuildHomeSnapshot() {
        activeRefreshes.update { count -> count + 1 }
        try {
            invalidateHomeSnapshot()
        } catch (throwable: Throwable) {
            activeRefreshes.update { count -> count - 1 }
            throw throwable
        }
        launchCountedRefresh(now = LocalDateTime.now(), force = true, zoneId = ZoneId.systemDefault())
    }

    suspend fun invalidateHomeSnapshot() {
        diagnosticsLogger.info(TAG, "home_snapshot_invalidate_start")
        // Before the clear below drops the stored fit they start from.
        refreshMutex.withLock { requestedFitEpochLocked() }
        val generation = homeSnapshotGenerationStore.incrementGeneration()
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_invalidate_generation_incremented generation=$generation"
        )
        snapshotMutationMutex.withLock {
            homeSnapshotStore.clearSnapshot()
            diagnosticsLogger.info(TAG, "home_snapshot_invalidated generation=$generation")
        }
    }

    /** See [refreshHomeSnapshotAsync] for [force] and [silent]. */
    suspend fun refreshHomeSnapshotIfNeeded(
        now: LocalDateTime = LocalDateTime.now(),
        force: Boolean = false,
        zoneId: ZoneId = ZoneId.systemDefault(),
        silent: Boolean = false,
    ) {
        // Reserved before the first suspension so the sequence is request order.
        refreshHomeSnapshotIfNeeded(
            now = now,
            force = force,
            zoneId = zoneId,
            sequence = refreshRequestSequence.incrementAndGet(),
        )
    }

    private suspend fun refreshHomeSnapshotIfNeeded(
        now: LocalDateTime,
        force: Boolean,
        zoneId: ZoneId,
        sequence: Long,
    ) {
        diagnosticsLogger.info(TAG, "home_snapshot_refresh_start force=$force now=$now")
        // Suspends on first call until the observer has captured the raw
        // DataStore value; subsequent calls return immediately.
        val option = settingsRepository.homeE2ChartWindowOptionFlow.first()
        val cacheWindow = HomePkProjectionWindow.forNow(now = now, zoneId = zoneId, option = option)
        val snapshotWindow = HomeSnapshotWindow.forNow(now = now, zoneId = zoneId)

        // refreshMutex is held only across the skip-check. The simulation and
        // write phases run outside the lock so mutations never stall behind a
        // PK simulation + JSON encode + encrypted DataStore write; correctness
        // relies on the generation recheck inside snapshotMutationMutex below —
        // any racing mutation bumps the durable generation and the writeSnapshot
        // call is dropped. A build in flight for the same generation, minute,
        // zone and chart option is shared: a second request (forced or not)
        // joins it instead of running its own. The boot / time-set / timezone
        // broadcasts reach two receivers that each force a refresh, which used
        // to run two full builds. A request whose context differs (the
        // midnight alarm arriving during a build started before midnight, a
        // zone change, an option change) runs its own build so the only
        // corrective request is never consumed by a stale one. Requests are
        // sequenced in creation order; a request that reaches this point after
        // a newer one has already started a build is superseded (it joins that
        // build, or returns if it has finished), and the write guard drops a
        // build once a newer request has started, so a slow older-context
        // build can never overwrite the newer snapshot.
        val minute = now.truncatedTo(ChronoUnit.MINUTES)
        val build = refreshMutex.withLock {
            val generation = homeSnapshotGenerationStore.readGeneration()
            val inFlight = inFlightRefresh
            val superseded = sequence < latestStartedRefreshSequence
            if (inFlight != null && inFlight.build.isActive && (
                        superseded || (
                                inFlight.generation == generation &&
                                        inFlight.minute == minute &&
                                        inFlight.zoneId == zoneId &&
                                        inFlight.option == option
                                )
                        )
            ) {
                diagnosticsLogger.info(
                    TAG,
                    "home_snapshot_refresh_joined_in_flight generation=$generation " +
                            "force=$force now=$now superseded=$superseded"
                )
                return@withLock inFlight.build
            }
            if (superseded) {
                diagnosticsLogger.info(
                    TAG,
                    "home_snapshot_refresh_skipped reason=superseded sequence=$sequence " +
                            "latestStartedSequence=$latestStartedRefreshSequence now=$now"
                )
                return@withLock null
            }
            val refreshGeneration = refreshGenerationAfterSkipCheckLocked(
                generation = generation,
                now = now,
                zoneId = zoneId,
                force = force,
            ) ?: return@withLock null
            latestStartedRefreshSequence = sequence
            val fitEpoch = requestedFitEpochLocked()
            // The build deliberately outlives its requester: a receiver's
            // timeout cancels only its await, and later requests join the
            // still-running build instead of starting another.
            appScope.async {
                // Logged here, not by the awaiter: a receiver's timeout cancels
                // only its await, leaving nobody to observe a later failure.
                try {
                    buildAndWriteHomeSnapshot(
                        refreshGeneration = refreshGeneration,
                        refreshSequence = sequence,
                        fitEpoch = fitEpoch,
                        now = now,
                        zoneId = zoneId,
                        option = option,
                        cacheWindow = cacheWindow,
                        snapshotWindow = snapshotWindow,
                    )
                } catch (throwable: Throwable) {
                    if (throwable is CancellationException) throw throwable
                    diagnosticsLogger.warning(
                        TAG,
                        "home_snapshot_refresh_failed force=$force now=$now",
                        throwable
                    )
                }
            }.also { started ->
                inFlightRefresh = InFlightRefresh(
                    generation = refreshGeneration,
                    minute = minute,
                    zoneId = zoneId,
                    option = option,
                    build = started,
                )
            }
        } ?: return
        build.await()
    }

    /** The build owned by the last refresh to pass the skip-check; guarded by [refreshMutex]. */
    private var inFlightRefresh: InFlightRefresh? = null

    /** Request-order sequence, reserved synchronously when a refresh is requested. */
    private val refreshRequestSequence = AtomicLong(0L)

    /** Highest sequence whose build has started; set under [refreshMutex], read at write time. */
    @Volatile
    private var latestStartedRefreshSequence: Long = 0L

    private class InFlightRefresh(
        val generation: Long,
        val minute: LocalDateTime,
        val zoneId: ZoneId,
        val option: HomeE2ChartWindowOption,
        val build: Deferred<Unit>,
    )

    private suspend fun refreshGenerationAfterSkipCheckLocked(
        generation: Long,
        now: LocalDateTime,
        zoneId: ZoneId,
        force: Boolean,
    ): Long? {
        val gen = generation
        val existingSnapshot = homeSnapshotStore.readSnapshot()
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_refresh_verify_existing force=$force " +
                    "generation=$gen " +
                    "existing=${existingSnapshot?.diagnosticSummary() ?: "none"}"
        )
        if (
            !force &&
            existingSnapshot != null &&
            existingSnapshot.generation >= gen &&
            isSnapshotUsable(existingSnapshot, now, zoneId)
        ) {
            diagnosticsLogger.info(
                TAG,
                "home_snapshot_refresh_skipped reason=existing_usable " +
                        "${existingSnapshot.diagnosticSummary()} now=$now"
            )
            return null
        }
        return gen
    }

    private suspend fun buildAndWriteHomeSnapshot(
        refreshGeneration: Long,
        /** Request-order sequence; a build is dropped once a newer request has started one. */
        refreshSequence: Long,
        /** [requestedFitEpoch] when the build started: a fit it solves is written with it. */
        fitEpoch: Int,
        now: LocalDateTime,
        zoneId: ZoneId,
        option: HomeE2ChartWindowOption,
        cacheWindow: HomePkProjectionWindow,
        snapshotWindow: HomeSnapshotWindow,
    ) {
        val inputs = withContext(Dispatchers.IO) {
            // awaitOpen, not get(): this runs inside WidgetDateReceiver's bounded goAsync
            // window, and the blocking get() is uncancellable — a stalled SQLCipher open
            // would hold the broadcast past its budget with no way to time out.
            val database = databaseHolder.awaitOpen()
            val homeDao = database.homeDao()
            val medicineDao = database.medicineDao()
            val medicationLogDao = database.medicationLogDao()
            val activeGroupEntities = homeDao.getActiveGroups()
            val archivedGroupEntities = homeDao.getArchivedGroups()
            val scheduleEntryEntities = homeDao.getScheduleEntries(
                scheduledStartIso = snapshotWindow.bufferedScheduledStart.toString(),
                scheduledEndIso = snapshotWindow.bufferedScheduledEnd.toString(),
                manualStartEpochMillis = snapshotWindow.bufferedManualStartEpochMillis,
                manualEndEpochMillis = snapshotWindow.bufferedManualEndEpochMillis,
            )
            val antiandrogenHistoryEntities = homeDao.getLatestAntiandrogenEntriesOnOrBefore(
                onOrBeforeEpochMillis = snapshotWindow.onOrBeforeEpochMillis,
            )
            val pkEntries = homeDao.getEstradiolPkEntries(
                startEpochMillis = cacheWindow.inputStartEpochMillis,
                endEpochMillis = cacheWindow.windowEndEpochMillis,
            )
            val latestEstradiolEntryEntity = homeDao.getLatestEstradiolEntryOnOrBefore(
                onOrBeforeEpochMillis = cacheWindow.generatedAtEpochMillis,
            )
            val stockMedicineEntities = medicineDao.getAllActiveTrackedEntities()
            val stockWindowStartIso = now.toLocalDate()
                .minusDays(1)
                .atStartOfDay()
                .toString()
            val stockWindowEndIso = now.toLocalDate()
                .plusDays(ScheduledRunwayCalculator.HORIZON_DAYS)
                .atTime(23, 59, 59)
                .toString()
            val stockFulfillmentEntities = medicationLogDao.getScheduledEntriesInWindow(
                scheduledStartIso = stockWindowStartIso,
                scheduledEndIso = stockWindowEndIso,
            )
            val homeAnchor = database.journalDao().getFirstPinnedTrackedDate()?.toModel()
            // Calibration inputs: every E2 lab, the estradiol doses that can
            // still shape a lab (see calibrationDoseWindowStartEpochMillis),
            // and the user's review metadata. None of the reads follows the
            // clock, so neither does the fit's identity.
            val calibrationLabs = database.bloodTestDao().getPanels().flatMap { panel ->
                panel.results
                    .filter { result ->
                        result.builtinAnalyteKey == BloodAnalyteKey.E2.storageValue
                    }
                    .map { result ->
                        PkCalibrationLab(
                            resultId = UUID.fromString(result.uuid),
                            collectedAtEpochMillis = panel.panel.collectedAtInstantEpochMillis,
                            valuePgml = result.canonicalValue,
                        )
                    }
            }
            val calibrationEntryEntities = calibrationDoseWindowStartEpochMillis(calibrationLabs)
                ?.let { startEpochMillis ->
                    homeDao.getEstradiolPkEntries(
                        startEpochMillis = startEpochMillis,
                        endEpochMillis = Long.MAX_VALUE,
                    )
                }
                .orEmpty()
            // Whenever it is logged for: buildPkCalibrationInput keeps it when
            // every dose predates the lab window. Without labs it is the only
            // dose read, since the solve then only tells no dose history from
            // no labs; a latest dose the model can't place reads as none.
            val latestCalibrationEntryEntity = homeDao.getLatestEstradiolEntryOnOrBefore(
                onOrBeforeEpochMillis = Long.MAX_VALUE,
            )
            val calibrationMetadata = database.pkCalibrationDao().getAllMetadata()
                .map { entity -> entity.toModel() }

            val groupMedicinesByUuid = database.resolveMedicinesForGroups(
                activeGroupEntities + archivedGroupEntities
            )
            val entryMedicinesByUuid = database.resolveMedicinesForEntries(
                scheduleEntryEntities + antiandrogenHistoryEntities + pkEntries +
                        listOfNotNull(latestEstradiolEntryEntity) + stockFulfillmentEntities +
                        calibrationEntryEntities + listOfNotNull(latestCalibrationEntryEntity)
            )
            val activeGroups = activeGroupEntities.map { group ->
                group.toMedicationGroupModel(groupMedicinesByUuid)
            }
            val archivedGroups = archivedGroupEntities.map { group ->
                group.toMedicationGroupModel(groupMedicinesByUuid)
            }
            val scheduleEntries = scheduleEntryEntities.map { entry ->
                entry.toMedicationLogEntryModel(entryMedicinesByUuid)
            }
            val antiandrogenHistoryEntries = antiandrogenHistoryEntities.map { entry ->
                entry.toMedicationLogEntryModel(entryMedicinesByUuid)
            }
            val pkEntryModels = pkEntries.map { entry ->
                entry.toMedicationLogEntryModel(entryMedicinesByUuid)
            }
            val latestEstradiolEntry = latestEstradiolEntryEntity?.toMedicationLogEntryModel(
                entryMedicinesByUuid
            )
            val stockMedicines = stockMedicineEntities.map { entity -> entity.toMedicineModel() }
            val stockFulfillmentEntries = stockFulfillmentEntities.map { entry ->
                entry.toMedicationLogEntryModel(entryMedicinesByUuid)
            }
            HomeSnapshotBuildInputs(
                calibrationLabs = calibrationLabs,
                // Plus the latest dose.
                calibrationEntries = (calibrationEntryEntities + listOfNotNull(latestCalibrationEntryEntity))
                    .distinctBy { entry -> entry.uuid }
                    .map { entry -> entry.toMedicationLogEntryModel(entryMedicinesByUuid) },
                calibrationMetadata = calibrationMetadata,
                activeGroups = activeGroups,
                archivedGroups = archivedGroups,
                scheduleEntries = scheduleEntries,
                antiandrogenHistoryEntries = antiandrogenHistoryEntries,
                pkEntries = pkEntryModels,
                latestEstradiolEntry = latestEstradiolEntry,
                profile = database.userProfileDao()
                    .getProfile()
                    ?.toUserProfileModel()
                    ?: UserProfile(),
                stockMedicines = stockMedicines,
                stockFulfillmentEntries = stockFulfillmentEntries,
                homeAnchor = homeAnchor,
            )
        }
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_refresh_inputs_loaded generation=$refreshGeneration " +
                    "activeGroups=${inputs.activeGroups.size} " +
                    "archivedGroups=${inputs.archivedGroups.size} " +
                    "scheduleEntries=${inputs.scheduleEntries.size} " +
                    "antiandrogenEntries=${inputs.antiandrogenHistoryEntries.size} " +
                    "pkEntries=${inputs.pkEntries.size} " +
                    "hasLatestEstradiol=${inputs.latestEstradiolEntry != null}"
        )

        // Lab calibration is part of the snapshot so the calibrated curve is
        // what Home and the widget show first, not a population curve swapped
        // out later.
        val phaseStartNanos = System.nanoTime()
        fun phaseMillis(): Long = (System.nanoTime() - phaseStartNanos) / 1_000_000L
        val calibrationInput = buildPkCalibrationInput(
            labs = inputs.calibrationLabs,
            entries = inputs.calibrationEntries,
            weightKg = inputs.profile.weightKg,
            metadata = inputs.calibrationMetadata,
        )
        val fitId = E2Derivation.fitId(calibrationInput)
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_phase calibration_input_built ms=${phaseMillis()} " +
                    "labs=${calibrationInput.labs.size} doseEvents=${calibrationInput.doseEvents.size}"
        )
        // The solve reads only what fitId digests, so the stored fit stands in
        // for it: a stock edit, an antiandrogen dose, midnight or a 7/30
        // switch never re-solves. Not a failed one, which the next build tries
        // again, nor one older than a pending Try again. The labs and review
        // metadata written beside it then flag what it was solved from: fitId
        // digests every value the calibration page's flags read.
        val reused = storedFit?.takeIf { stored ->
            stored.fitId == fitId && stored.outcome != FitOutcome.Failed && stored.epoch >= fitEpoch
        }
        val fit = reused ?: E2FitRecord(
            fitId = fitId,
            epoch = fitEpoch,
            // A throwing solve must not drop the snapshot write: Home would
            // fall back to population. Treat it as no calibration and still
            // write the snapshot.
            outcome = runCatching {
                withContext(defaultDispatcher) { PkCalibrationEngine.evaluate(calibrationInput) }
            }.onFailure { throwable ->
                if (throwable is CancellationException) throw throwable
                diagnosticsLogger.warning(
                    TAG,
                    "home_snapshot_calibration_failed generation=$refreshGeneration",
                    throwable,
                )
            }.getOrNull()
                ?.let { evaluation -> FitOutcome.Solved(evaluation.result, calibrationInput.originEpochMillis) }
                ?: FitOutcome.Failed,
        )
        val result = (fit.outcome as? FitOutcome.Solved)?.result
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_phase calibration_${if (reused != null) "reused" else "solved"} ms=${phaseMillis()} " +
                    "epoch=${fit.epoch} state=${result?.globalState} promoted=${result?.promotedRoutes}"
        )
        val e2Inputs = E2Inputs(
            estradiolEntries = inputs.pkEntries,
            plannedSources = inputs.activeGroups,
            weightKg = inputs.profile.weightKg,
            option = option,
            fit = fit,
        )
        // Home's estimate is derived from the record by E2EstimateRepository;
        // the widget still reads its projection from here.
        val widgetProjection = withContext(defaultDispatcher) {
            E2Derivation.deriveWidgetEstimate(e2Inputs, now, zoneId)
        }
        diagnosticsLogger.info(
            TAG,
            "home_snapshot_phase widget_estimate_derived ms=${phaseMillis()} " +
                    "real=${inputs.pkEntries.size} samples=${widgetProjection.timeH.size}"
        )
        val generatedAt = now.atZone(zoneId).toInstant()
        val snapshotRecord = HomeSnapshotRecord(
            schemaVersion = HOME_SNAPSHOT_SCHEMA_VERSION,
            generation = refreshGeneration,
            generatedAtEpochMillis = generatedAt.toEpochMilli(),
            anchorDateEpochDay = now.toLocalDate().toEpochDay(),
            zoneId = zoneId.id,
            // Logged doses only: nothing goes stale before the window end.
            widgetPkProjection = widgetProjection.toRecord(
                generatedAt = generatedAt,
                expiresAt = widgetProjection.windowEnd,
            ),
            activeGroups = inputs.activeGroups,
            archivedGroups = inputs.archivedGroups,
            scheduleEntries = inputs.scheduleEntries,
            antiandrogenHistoryEntries = inputs.antiandrogenHistoryEntries,
            userProfile = inputs.profile,
            stockMedicines = inputs.stockMedicines,
            stockFulfillmentEntries = inputs.stockFulfillmentEntries,
            pkEntries = inputs.pkEntries,
            homeAnchor = inputs.homeAnchor,
            latestEstradiolEntry = inputs.latestEstradiolEntry,
            option = option,
            fit = e2Inputs.fit,
            labs = inputs.calibrationLabs,
            labMetadata = inputs.calibrationMetadata,
        )

        val written = withContext(Dispatchers.IO) {
            // Avoid restoring stale snapshot data from a refresh that raced with a write.
            snapshotMutationMutex.withLock {
                if (refreshSequence < latestStartedRefreshSequence) {
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_write_skipped reason=newer_refresh_started " +
                                "refreshSequence=$refreshSequence " +
                                "latestStartedSequence=$latestStartedRefreshSequence"
                    )
                    false
                } else if (homeSnapshotGenerationStore.readGeneration() == refreshGeneration) {
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_write_start ${snapshotRecord.diagnosticSummary()} " +
                                "pkEntries=${inputs.pkEntries.size}"
                    )
                    homeSnapshotStore.writeSnapshot(snapshotRecord)
                    storedFit = snapshotRecord.fit
                    StartupTiming.mark("home_snapshot_rebuilt")
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_refreshed ${snapshotRecord.diagnosticSummary()}"
                    )
                    true
                } else {
                    diagnosticsLogger.info(
                        TAG,
                        "home_snapshot_write_skipped reason=generation_changed " +
                                "refreshGeneration=$refreshGeneration " +
                                "currentGeneration=${homeSnapshotGenerationStore.readGeneration()}"
                    )
                    false
                }
            }
        }
        // Home's estimate for the record just written, so the next open finds
        // it stored. Inside the build, so whoever awaits it awaits this too.
        if (written) e2EstimateRepository.get().warmHome()
    }

    private data class HomeSnapshotBuildInputs(
        val calibrationLabs: List<PkCalibrationLab>,
        val calibrationEntries: List<MedicationLogEntry>,
        val calibrationMetadata: List<E2CalibrationMetadata>,
        val activeGroups: List<MedicationGroup>,
        val archivedGroups: List<MedicationGroup>,
        val scheduleEntries: List<MedicationLogEntry>,
        val antiandrogenHistoryEntries: List<MedicationLogEntry>,
        val pkEntries: List<MedicationLogEntry>,
        val latestEstradiolEntry: MedicationLogEntry?,
        val profile: UserProfile,
        val stockMedicines: List<Medicine>,
        val stockFulfillmentEntries: List<MedicationLogEntry>,
        val homeAnchor: TrackedDate? = null,
    )

    // The logged E2 the snapshot carries: from every estimate's PK lookback,
    // the calibration page's included, whatever the option, through the
    // option's projection end (option.futureDays +
    // MainChartProjectionFutureBufferDays past today's midnight).
    private data class HomePkProjectionWindow(
        val generatedAtEpochMillis: Long,
        val windowEndEpochMillis: Long,
        val inputStartEpochMillis: Long,
    ) {
        companion object {
            fun forNow(
                now: LocalDateTime,
                zoneId: ZoneId,
                option: HomeE2ChartWindowOption,
            ): HomePkProjectionWindow {
                val generatedAt = now.atZone(zoneId).toInstant()
                val windowEnd = now
                    .toLocalDate()
                    .plusDays(option.projectionFutureDays())
                    .atStartOfDay()
                    .atZone(zoneId)
                    .toInstant()
                return HomePkProjectionWindow(
                    generatedAtEpochMillis = generatedAt.toEpochMilli(),
                    windowEndEpochMillis = windowEnd.toEpochMilli(),
                    inputStartEpochMillis = E2Derivation.loggedEntriesStart(now, zoneId).toEpochMilli(),
                )
            }
        }
    }

    private data class HomeSnapshotWindow(
        val homeScheduledStart: LocalDateTime,
        val homeScheduledEnd: LocalDateTime,
        val homeManualStartEpochMillis: Long,
        val homeManualEndEpochMillis: Long,
        val bufferedScheduledStart: LocalDateTime,
        val bufferedScheduledEnd: LocalDateTime,
        val bufferedManualStartEpochMillis: Long,
        val bufferedManualEndEpochMillis: Long,
        val onOrBeforeEpochMillis: Long,
    ) {
        companion object {
            fun forNow(
                now: LocalDateTime,
                zoneId: ZoneId,
            ): HomeSnapshotWindow {
                val today = now.toLocalDate()
                return HomeSnapshotWindow(
                    homeScheduledStart = today.minusDays(1).atStartOfDay(),
                    homeScheduledEnd = today.plusDays(HOME_SCHEDULE_LOOKAHEAD_DAYS)
                        .atTime(23, 59, 59),
                    homeManualStartEpochMillis = today.minusDays(1)
                        .atStartOfDay(zoneId)
                        .toInstant()
                        .toEpochMilli(),
                    homeManualEndEpochMillis = today.plusDays(1)
                        .atStartOfDay(zoneId)
                        .toInstant()
                        .toEpochMilli(),
                    bufferedScheduledStart = today.minusDays(HOME_SNAPSHOT_PAST_BUFFER_DAYS)
                        .atStartOfDay(),
                    bufferedScheduledEnd = today.plusDays(HOME_SCHEDULE_LOOKAHEAD_DAYS + HOME_SNAPSHOT_VALIDITY_DAYS)
                        .atTime(23, 59, 59),
                    bufferedManualStartEpochMillis = today.minusDays(HOME_SNAPSHOT_PAST_BUFFER_DAYS)
                        .atStartOfDay(zoneId)
                        .toInstant()
                        .toEpochMilli(),
                    bufferedManualEndEpochMillis = today.plusDays(HOME_SNAPSHOT_VALIDITY_DAYS + 1)
                        .atStartOfDay(zoneId)
                        .toInstant()
                        .toEpochMilli(),
                    onOrBeforeEpochMillis = now
                        .atZone(zoneId)
                        .toInstant()
                        .toEpochMilli(),
                )
            }
        }
    }

    private suspend fun clearSnapshotBestEffort() {
        snapshotMutationMutex.withLock {
            clearSnapshotBestEffortLocked()
        }
    }

    private suspend fun clearSnapshotBestEffortLocked() {
        runCatching {
            homeSnapshotStore.clearSnapshot()
            diagnosticsLogger.info(TAG, "home_snapshot_cleared_best_effort")
        }.onFailure { throwable ->
            diagnosticsLogger.warning(TAG, "home_snapshot_clear_failed", throwable)
        }
    }

    private suspend fun refreshHomeSnapshotBestEffort(
        now: LocalDateTime = LocalDateTime.now(),
        force: Boolean = false,
        zoneId: ZoneId = ZoneId.systemDefault(),
    ) {
        runCatching {
            refreshHomeSnapshotIfNeeded(now = now, force = force, zoneId = zoneId)
        }.onFailure { throwable ->
            if (throwable is CancellationException) {
                throw throwable
            }
            diagnosticsLogger.warning(
                TAG,
                "home_snapshot_refresh_failed force=$force now=$now",
                throwable
            )
        }
    }
}

/** A derived projection as the snapshot stores it: stamped with the build's time and its expiry. */
private fun PkProjectionResult.toRecord(
    generatedAt: Instant,
    expiresAt: Instant,
) = HomePkProjectionRecord(
    generatedAtEpochMillis = generatedAt.toEpochMilli(),
    windowStartEpochMillis = windowStart.toEpochMilli(),
    windowEndEpochMillis = windowEnd.toEpochMilli(),
    pkProjectionExpiresAtEpochMillis = expiresAt.toEpochMilli(),
    concentrationUnit = concentrationUnit.name,
    timeH = timeH,
    concentrations = concentrations,
    doseMarkers = doseMarkers.map { marker ->
        HomePkProjectionDoseMarkerRecord(
            timeH = marker.timeH,
            concentration = marker.concentration,
            isPlanned = marker.isPlanned,
        )
    },
)

private fun HomeSnapshotRecord.diagnosticSummary(): String {
    return "schema=$schemaVersion " +
            "generation=$generation " +
            "anchorDate=${LocalDate.ofEpochDay(anchorDateEpochDay)} " +
            "zone=$zoneId " +
            "groups=${activeGroups.size} " +
            "archivedGroups=${archivedGroups.size} " +
            "scheduleEntries=${scheduleEntries.size} " +
            "antiandrogenEntries=${antiandrogenHistoryEntries.size} " +
            "engine=$engineVersion " +
            "option=$option " +
            "fitEpoch=${fit?.epoch}"
}

// Bumped to 9 with the Oct 2026 estradiol PK refit so every install rebuilt its cached
// projections and calibration on the first open after the update. A PK refit now bumps
// PK_ENGINE_VERSION instead: a schema bump alone rebuilds the snapshot but reuses the
// stored fit, whose fitId only the engine version moves.
internal const val HOME_SNAPSHOT_SCHEMA_VERSION = 9

private const val TAG = "HomeSnapshotRepository"
private const val HOME_SCHEDULE_LOOKAHEAD_DAYS = 90L
private const val HOME_SNAPSHOT_VALIDITY_DAYS = 10L
private const val HOME_SNAPSHOT_PAST_BUFFER_DAYS = 1L

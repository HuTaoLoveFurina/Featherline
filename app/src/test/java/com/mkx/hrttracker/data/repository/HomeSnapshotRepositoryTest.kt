package com.mkx.hrttracker.data.repository

import com.mkx.hrttracker.data.local.DatabaseHolder
import com.mkx.hrttracker.data.local.HomeDao
import com.mkx.hrttracker.data.local.PkCalibrationDao
import com.mkx.hrttracker.data.local.BloodTestDao
import com.mkx.hrttracker.data.local.BloodTestPanelEntity
import com.mkx.hrttracker.data.local.BloodTestPanelWithResultsEntity
import com.mkx.hrttracker.data.local.BloodTestResultEntity
import com.mkx.hrttracker.data.local.E2CalibrationMetadataEntity
import com.mkx.hrttracker.data.local.HrtTrackerDatabase
import com.mkx.hrttracker.data.local.JournalDao
import com.mkx.hrttracker.data.local.MedicationGroupEntity
import com.mkx.hrttracker.data.local.MedicationGroupItemEntity
import com.mkx.hrttracker.data.local.MedicationGroupScheduleTimeEntity
import com.mkx.hrttracker.data.local.MedicationGroupWithItemsEntity
import com.mkx.hrttracker.data.local.MedicationLogDao
import com.mkx.hrttracker.data.local.MedicationLogEntryEntity
import com.mkx.hrttracker.data.local.MedicineDao
import com.mkx.hrttracker.data.local.MedicineEntity
import com.mkx.hrttracker.data.local.TrackedDateEntity
import com.mkx.hrttracker.data.local.UserProfileDao
import com.mkx.hrttracker.model.bloodtest.BloodAnalyteKey
import com.mkx.hrttracker.model.medication.DoseInstructionKind
import com.mkx.hrttracker.model.medication.MedicationApplicationType
import com.mkx.hrttracker.model.medication.MedicationCategory
import com.mkx.hrttracker.model.medication.MedicationGroupColorKey
import com.mkx.hrttracker.model.medication.MedicationGroupScheduleType
import com.mkx.hrttracker.model.medication.MedicationKey
import com.mkx.hrttracker.model.pk.E2CalibrationDisposition
import com.mkx.hrttracker.model.pk.HomeE2ChartWindowOption
import com.mkx.hrttracker.model.pk.PkCalibrationEngine
import com.mkx.hrttracker.model.pk.PkCalibrationEvaluation
import com.mkx.hrttracker.model.pk.PkCalibrationGlobalState
import com.mkx.hrttracker.model.pk.PkCalibrationResult
import com.mkx.hrttracker.model.pk.PkCalibrationRoute
import com.mkx.hrttracker.model.pk.PkRouteCalibrationDisplayState
import com.mkx.hrttracker.model.pk.PkRouteCalibrationResult
import com.mkx.hrttracker.model.pk.resultIdsWith
import com.mkx.hrttracker.model.personalization.UserProfile
import com.mkx.hrttracker.model.settings.SettingsState
import com.mkx.hrttracker.util.AppDiagnosticsLogger
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.slot
import io.mockk.unmockkObject
import io.mockk.verify
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.IOException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.TimeZone
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class HomeSnapshotRepositoryTest {
    private val databaseHolder: DatabaseHolder = mockk()
    private val homeSnapshotStore: HomeSnapshotStore = mockk()
    private val homeSnapshotGenerationStore: HomeSnapshotGenerationStore = mockk()
    private val settingsRepository: SettingsRepository = mockk()
    private val diagnosticsLogger: AppDiagnosticsLogger = mockk(relaxed = true)
    private val e2EstimateRepository: E2EstimateRepository = mockk(relaxed = true)
    private val generationState = MutableStateFlow(0L)
    private val chartWindowOptionState = MutableStateFlow(HomeE2ChartWindowOption.SEVEN_DAYS)
    private val settingsState = MutableStateFlow(SettingsState())

    @Before
    fun setUp() {
        generationState.value = 0L
        chartWindowOptionState.value = HomeE2ChartWindowOption.SEVEN_DAYS
        every { homeSnapshotGenerationStore.observeGeneration() } returns generationState
        coEvery { homeSnapshotGenerationStore.readGeneration() } coAnswers {
            generationState.value
        }
        coEvery { homeSnapshotGenerationStore.incrementGeneration() } coAnswers {
            val nextGeneration = generationState.value + 1L
            generationState.value = nextGeneration
            nextGeneration
        }
        every { settingsRepository.homeE2ChartWindowOptionFlow } returns chartWindowOptionState
        every { settingsRepository.settingsState } returns settingsState
        coEvery { settingsRepository.getCurrentSettings() } returns SettingsState()
    }

    @Test
    fun invalidateHomeSnapshot_clearsStoredSnapshot() = runTest {
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        val dispatcher = StandardTestDispatcher(testScheduler)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )

        repository.invalidateHomeSnapshot()

        coVerify(exactly = 1) { homeSnapshotStore.clearSnapshot() }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun runHomeDataMutation_rebuildContainsUpdatedDisplayNameForEveryGroupSlot() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val writtenSnapshot = slot<HomeSnapshotRecord>()
        val medicineUuid = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001")
        var displayName: String? = null

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns listOf(
            groupWithTwoSlotsReferencing(medicineUuid)
        )
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null
        coEvery { medicineDao.getByUuids(listOf(medicineUuid.toString())) } coAnswers {
            listOf(medicineEntity(uuid = medicineUuid, displayName = displayName))
        }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
        )

        repository.runHomeDataMutation {
            displayName = "Renamed"
        }
        awaitRebuild(testScheduler) { writtenSnapshot.isCaptured }

        assertTrue(writtenSnapshot.isCaptured)
        assertEquals(
            listOf("Renamed", "Renamed"),
            writtenSnapshot.captured.activeGroups
                .single()
                .medications
                .map { medication -> medication.medicine?.displayName },
        )
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun concurrentForcedRefreshes_shareOneBuild() = runTest {
        // Boot, time-set and timezone broadcasts reach two receivers that each
        // force a refresh ~40 ms apart. Both used to pass the skip-check and run
        // a full build (PK simulation, calibration solve, encrypted write).
        val dispatcher = StandardTestDispatcher(testScheduler)
        val builds = stubSlowEmptyRefreshInputs()
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )

        val first = launch(dispatcher) { repository.refreshHomeSnapshotIfNeeded(force = true) }
        val second = launch(dispatcher) { repository.refreshHomeSnapshotIfNeeded(force = true) }
        first.join()
        second.join()

        assertEquals(1, builds.get())
        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(any()) }

        // Once the shared build has completed, a new forced request builds again.
        repository.refreshHomeSnapshotIfNeeded(force = true)
        assertEquals(2, builds.get())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun concurrentRefreshes_withDifferentAnchorDates_doNotShareABuild() = runTest {
        // The midnight alarm can arrive while a build started at 23:59:59 is
        // still running. Joining it would write yesterday's anchor date and
        // consume the only corrective request; the widget would then show
        // yesterday until the next mutation, app open or alarm.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val builds = stubSlowEmptyRefreshInputs()
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        val beforeMidnight = LocalDateTime.of(2026, 5, 7, 23, 59, 59)
        val midnight = LocalDateTime.of(2026, 5, 8, 0, 0)

        val first = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = beforeMidnight, force = true)
        }
        val second = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = midnight, force = true)
        }
        first.join()
        second.join()

        // Two builds ran; only the latest-started one (midnight) is written, the
        // pre-midnight build's record is dropped even though it finished first.
        assertEquals(2, builds.get())
        val written = slot<HomeSnapshotRecord>()
        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(capture(written)) }
        assertEquals(midnight.toLocalDate().toEpochDay(), written.captured.anchorDateEpochDay)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun olderContextBuildFinishingLast_doesNotOverwriteTheNewerSnapshot() = runTest {
        // Two different-context builds share a generation, so the generation
        // guard alone lets whichever finishes last win. A slow pre-midnight
        // build finishing after the midnight one would put yesterday back.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val builds = stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(200L, 50L))
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        val beforeMidnight = LocalDateTime.of(2026, 5, 7, 23, 59, 59)
        val midnight = LocalDateTime.of(2026, 5, 8, 0, 0)

        val first = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = beforeMidnight, force = true)
        }
        val second = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = midnight, force = true)
        }
        first.join()
        second.join()

        assertEquals(2, builds.get())
        val written = slot<HomeSnapshotRecord>()
        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(capture(written)) }
        assertEquals(midnight.toLocalDate().toEpochDay(), written.captured.anchorDateEpochDay)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun forcedRequest_joinsTheBuildInFlight() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val builds = stubSlowEmptyRefreshInputs()
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        val now = LocalDateTime.of(2026, 5, 7, 10, 0)

        val launch = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = now, force = false)
        }
        runCurrent() // the launch request passes the skip-check and starts its build
        val retry = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = now, force = true)
        }
        launch.join()
        retry.join()

        // One build, joined by the forced request.
        assertEquals(1, builds.get())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun olderRequestReachingTheMutexLast_isSupersededByTheNewerBuild() = runTest {
        // The sequence must follow request order, not the order coroutines
        // reach the refresh mutex: a pre-midnight request whose coroutine is
        // delayed past the midnight request must not take the higher sequence,
        // suppress the midnight build and write yesterday's snapshot.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val builds = stubSlowEmptyRefreshInputs()
        // The option flow is the first suspension before the mutex. Call 0
        // (the first request) is held back so the second request reaches the
        // mutex first.
        var optionFlowReads = 0
        every { settingsRepository.homeE2ChartWindowOptionFlow } answers {
            val read = optionFlowReads++
            flow {
                if (read == 0) delay(50)
                emit(HomeE2ChartWindowOption.SEVEN_DAYS)
            }
        }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        val beforeMidnight = LocalDateTime.of(2026, 5, 7, 23, 59, 59)
        val midnight = LocalDateTime.of(2026, 5, 8, 0, 0)

        val first = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = beforeMidnight, force = true)
        }
        val second = launch(dispatcher) {
            repository.refreshHomeSnapshotIfNeeded(now = midnight, force = true)
        }
        first.join()
        second.join()

        // The delayed older request joins the newer build instead of building.
        assertEquals(1, builds.get())
        val written = slot<HomeSnapshotRecord>()
        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(capture(written)) }
        assertEquals(midnight.toLocalDate().toEpochDay(), written.captured.anchorDateEpochDay)
    }

    /**
     * Empty Home inputs whose first DAO read suspends, so a second refresh
     * request arrives while the first build is still loading. Builds load on
     * Dispatchers.IO, so the counter is read and bumped from real threads.
     */

    /**
     * The mutation returns once the DB commit lands; the rebuild runs in the
     * app scope and hops through Dispatchers.IO, so advance the test scheduler
     * until [condition] holds (or fail after a real-time budget).
     */
    private fun awaitRebuild(
        scheduler: kotlinx.coroutines.test.TestCoroutineScheduler,
        condition: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 10_000L
        while (!condition()) {
            check(System.currentTimeMillis() < deadline) { "rebuild did not finish in time" }
            scheduler.advanceUntilIdle()
            Thread.sleep(2L)
        }
    }

    private fun stubSlowEmptyRefreshInputs(
        /** Per-build delay of the first DAO read, in scheduler ms; the last entry repeats. */
        buildDelaysMs: List<Long> = listOf(100L),
    ): AtomicInteger {
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val builds = AtomicInteger()

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.writeSnapshot(any()) } returns Unit
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } coAnswers {
            val delayMs = buildDelaysMs[minOf(builds.getAndIncrement(), buildDelaysMs.lastIndex)]
            delay(delayMs)
            emptyList()
        }
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicineDao.getByUuids(any()) } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null
        return builds
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun runHomeDataMutation_writesSnapshotWithoutCalibrationWhenSolveThrows() = runTest {
        // A throwing solve used to abort the whole refresh: no snapshot was
        // written, Home fell back to population, and the live calibration
        // surface (keyed on snapshot generation) kept the pre-mutation fit.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val writtenSnapshot = slot<HomeSnapshotRecord>()
        stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )

        mockkObject(PkCalibrationEngine)
        try {
            every { PkCalibrationEngine.evaluate(any()) } throws IllegalStateException("solver")
            repository.runHomeDataMutation { }
            awaitRebuild(testScheduler) { writtenSnapshot.isCaptured }
        } finally {
            unmockkObject(PkCalibrationEngine)
        }

        assertTrue(writtenSnapshot.isCaptured)
        assertEquals(FitOutcome.Failed, writtenSnapshot.captured.fit?.outcome)
        assertTrue(writtenSnapshot.captured.widgetPkProjection != null)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun retryCalibration_reSolvesWithARaisedEpoch_fromUnchangedInputs() = runTest {
        // Try again after a failed solve: the inputs are the same, so only
        // the epoch tells the estimates the fit is new. It is a gated change.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val written = mutableListOf<HomeSnapshotRecord>()
        stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        var solves = 0
        mockkObject(PkCalibrationEngine)
        try {
            every { PkCalibrationEngine.evaluate(any()) } answers {
                if (solves++ == 0) throw IllegalStateException("solver") else readyEvaluation()
            }
            repository.runHomeDataMutation { }
            awaitRebuild(testScheduler) { written.size == 1 }

            repository.retryCalibration()
            assertEquals(2L, generationState.value)
            assertTrue(repository.rebuildInFlight.first())
            awaitRebuild(testScheduler) { written.size == 2 }
        } finally {
            unmockkObject(PkCalibrationEngine)
        }

        assertEquals(FitOutcome.Failed, written[0].fit?.outcome)
        assertEquals(0, written[0].fit?.epoch)
        assertTrue(written[1].fit?.outcome is FitOutcome.Solved)
        assertEquals(1, written[1].fit?.epoch)
        assertEquals(written[0].fit?.fitId, written[1].fit?.fitId)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun retryCalibration_survivesTheBuildItInterrupts() = runTest {
        // The build in flight when Try again lands read the old epoch and is
        // dropped by the generation bump; the retry's own build writes the new
        // one, and only a written build warms Home's estimate.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val written = mutableListOf<HomeSnapshotRecord>()
        val builds = stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(100L, 0L))
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )

        val interrupted = launch(dispatcher) { repository.refreshHomeSnapshotIfNeeded(force = true) }
        runCurrent() // its build has read the epoch and is loading
        repository.retryCalibration()
        interrupted.join()
        awaitRebuild(testScheduler) { written.isNotEmpty() && builds.get() == 2 }
        testScheduler.advanceUntilIdle()

        assertEquals(1, written.size)
        assertEquals(1, written.single().fit?.epoch)
        coVerify(exactly = 1) { e2EstimateRepository.warmHome() }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun requestedFitEpoch_startsAtTheStoredFits_andOutlivesAMutationsClear() = runTest {
        // Try again in an earlier process left epoch 3. An ordinary build must
        // keep it, or a stock edit would move every estimate key.
        val dispatcher = StandardTestDispatcher(testScheduler)
        val written = mutableListOf<HomeSnapshotRecord>()
        stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        var stored: HomeSnapshotRecord? = homeSnapshotRecord(generation = 0L, now = LocalDateTime.of(2026, 5, 7, 9, 0))
            .copy(fit = E2FitRecord(Digest("00"), epoch = 3, outcome = FitOutcome.Failed))
        coEvery { homeSnapshotStore.readSnapshot() } answers { stored }
        coEvery { homeSnapshotStore.clearSnapshot() } answers { stored = null }
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )

        // The first thing this process does is a mutation, which clears the
        // snapshot before any build has read the epoch.
        repository.runHomeDataMutation { }
        awaitRebuild(testScheduler) { written.size == 1 }
        repository.retryCalibration()
        awaitRebuild(testScheduler) { written.size == 2 }

        assertEquals(listOf(3, 4), written.map { record -> record.fit?.epoch })
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun aBuildEndsWithTheWarmUp_andAwaitRebuildWaitsForIt() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val events = java.util.Collections.synchronizedList(mutableListOf<String>())
        val allowWarmUp = CompletableDeferred<Unit>()
        stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { events += "write" }
        coEvery { e2EstimateRepository.warmHome() } coAnswers {
            events += "warm"
            allowWarmUp.await()
        }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )

        repository.runHomeDataMutation { }
        val awaiting = async(dispatcher) { repository.awaitRebuild() }
        awaitRebuild(testScheduler) { "warm" in events }

        assertEquals(listOf("write", "warm"), events.toList())
        assertFalse(awaiting.isCompleted)
        allowWarmUp.complete(Unit)
        awaitRebuild(testScheduler) { awaiting.isCompleted }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun aBuildsWarmUp_storesHomesKey_soAColdStartRightAfterShowsIt() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        val records = MutableStateFlow<HomeSnapshotRecord?>(null)
        every { homeSnapshotStore.observeSnapshot() } returns records
        coEvery { homeSnapshotStore.readSnapshot() } answers { records.value }
        coEvery { homeSnapshotStore.clearSnapshot() } answers { records.value = null }
        coEvery { homeSnapshotStore.writeSnapshot(any()) } answers { records.value = firstArg() }
        var stored: HomeE2EstimateRecord? = null
        val estimateStore = mockk<HomeE2EstimateStore> {
            coEvery { read() } answers { stored }
            coEvery { write(any()) } answers { stored = firstArg() }
        }
        val clock = java.time.Clock.systemDefaultZone()
        lateinit var estimates: E2EstimateRepository
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { estimates },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        estimates = E2EstimateRepository(
            homeSnapshotRepository = repository,
            store = estimateStore,
            clock = clock,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = dispatcher,
        )

        repository.runHomeDataMutation { }
        awaitRebuild(testScheduler) { stored != null }

        val record = checkNotNull(records.value)
        val now = LocalDateTime.now(clock).truncatedTo(java.time.temporal.ChronoUnit.MINUTES)
        assertEquals(E2Derivation.homeEstimateKey(record.e2Inputs(), now, ZoneId.systemDefault()), stored?.key)
        val firstStates = mutableListOf<E2State<HomeE2Estimate>>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            estimates.observeHome(MutableStateFlow(now)).collect { state -> firstStates += state }
        }
        assertEquals(stored?.estimate, firstStates.first().value)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun runHomeDataMutation_runsCleanupAndRefreshEvenWhenBlockThrows() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val events = mutableListOf<String>()
        coEvery { homeSnapshotStore.clearSnapshot() } coAnswers {
            events += "clear"
            Unit
        }
        coEvery { homeSnapshotStore.readSnapshot() } coAnswers {
            events += "refresh_read"
            null
        }
        // Refresh path will hit the database; minimal stubs so it returns quickly.
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getByUuids(any()) } returns emptyList()
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null
        coEvery { homeSnapshotStore.writeSnapshot(any()) } returns Unit
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
        )

        var caught: Throwable? = null
        try {
            repository.runHomeDataMutation {
                events += "throw"
                throw IllegalStateException("simulated mutation failure")
            }
        } catch (t: IllegalStateException) {
            caught = t
        }
        advanceUntilIdle()

        // Failure must propagate (we don't silently swallow), AND cleanup must have run.
        assertEquals("simulated mutation failure", caught.message)
        assertTrue("clearSnapshot should run on the throwing path", events.contains("clear"))
        coVerify(atLeast = 1) { homeSnapshotStore.clearSnapshot() }
        // The async refresh should have been enqueued; readSnapshot is the first
        // action of the refresh worker.
        assertTrue("refresh should be enqueued", events.contains("refresh_read"))
    }

    @Test
    fun runHomeDataMutation_doesNotBlockWriteWhenClearSnapshotFails() = runTest {
        val dispatcher = StandardTestDispatcher()
        val events = mutableListOf<String>()
        coEvery { homeSnapshotStore.clearSnapshot() } coAnswers {
            events += "clear"
            throw IOException("clear failed")
        }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )

        val result = repository.runHomeDataMutation {
            events += "write"
            "saved"
        }

        assertEquals("saved", result)
        assertEquals(listOf("write", "clear"), events)
    }

    @Test
    fun runHomeDataMutation_releasesRebuildCountWhenGenerationBumpThrows() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        coEvery { homeSnapshotGenerationStore.incrementGeneration() } throws IOException("bump failed")
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )
        var blockRan = false

        val failure = runCatching {
            repository.runHomeDataMutation { blockRan = true }
        }.exceptionOrNull()

        assertTrue(failure is IOException)
        assertFalse(blockRan)
        assertFalse(repository.rebuildInFlight.first())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun runHomeDataMutation_rebuildsAfterMutationCommit() = runTest {
        val appScheduler = kotlinx.coroutines.test.TestCoroutineScheduler()
        val appDispatcher = StandardTestDispatcher(appScheduler)
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val mutationCommitted = CompletableDeferred<Unit>()

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers {
            assertTrue(mutationCommitted.isCompleted)
            Unit
        }
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getByUuids(any()) } returns emptyList()
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(appDispatcher),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
        )

        var written = false
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers {
            assertTrue(mutationCommitted.isCompleted)
            written = true
        }

        repository.runHomeDataMutation {
            mutationCommitted.complete(Unit)
        }
        // The caller is back before the rebuild, which is already counted.
        assertTrue(repository.rebuildInFlight.first())
        awaitRebuild(appScheduler) { written }
        // The in-flight count drops once the refresh coroutine unwinds.
        appScheduler.advanceUntilIdle()

        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(any()) }
        assertFalse(repository.rebuildInFlight.first())

        // Home's launch request and a system event's forced rebuild count too.
        repository.refreshHomeSnapshotAsync(force = false)
        assertTrue(repository.rebuildInFlight.first())
        appScheduler.advanceUntilIdle()
        repository.refreshHomeSnapshotAsync(force = true, silent = true)
        assertTrue(repository.rebuildInFlight.first())
    }

    @Test
    fun invalidateAndRebuildHomeSnapshot_countsBeforeGenerationBump() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )
        var countedAtClear: Boolean? = null
        coEvery { homeSnapshotStore.clearSnapshot() } coAnswers {
            countedAtClear = repository.rebuildInFlight.first()
        }
        coEvery { databaseHolder.awaitOpen() } coAnswers { throw IOException("stop at inputs") }

        repository.invalidateAndRebuildHomeSnapshot()

        // The generation bump makes observers reject the stored snapshot, so
        // the rebuild is counted before it (no uncounted population frame).
        assertEquals(true, countedAtClear)
        assertEquals(1L, generationState.value)
        advanceUntilIdle()
        assertFalse(repository.rebuildInFlight.first())
    }

    @Test
    fun observeHomeSnapshot_rejectsStoredSnapshotOlderThanDurableGeneration() = runTest {
        val snapshot = homeSnapshotRecord(
            generation = 1L,
            now = LocalDateTime.of(2026, 5, 6, 10, 15),
        )
        generationState.value = 2L
        every { homeSnapshotStore.observeSnapshot() } returns MutableStateFlow(snapshot)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )

        assertNull(repository.observeHomeSnapshot().first())
    }

    @Test
    fun readUsableHomeSnapshot_rejectsStoredSnapshotOlderThanDurableGeneration() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        coEvery { homeSnapshotStore.readSnapshot() } returns homeSnapshotRecord(
            generation = 1L,
            now = now,
        )
        generationState.value = 2L
        val dispatcher = StandardTestDispatcher(testScheduler)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )

        assertNull(repository.readUsableHomeSnapshot(now = now))
    }

    @Test
    fun readUsableHomeSnapshot_logsVerificationRejectionReason() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        coEvery { homeSnapshotStore.readSnapshot() } returns homeSnapshotRecord(
            generation = 1L,
            now = now,
        ).copy(schemaVersion = HOME_SNAPSHOT_SCHEMA_VERSION - 1)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
            diagnosticsLogger = diagnosticsLogger,
        )

        assertNull(repository.readUsableHomeSnapshot(now = now))

        verify {
            diagnosticsLogger.info(
                "HomeSnapshotRepository",
                match { message ->
                    "home_snapshot_read_rejected" in message &&
                            "reason=schema_version" in message &&
                            "actual=${HOME_SNAPSHOT_SCHEMA_VERSION - 1}" in message
                }
            )
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun refreshHomeSnapshotAsync_swallowsNonCriticalRefreshFailure() = runTest {
        val failures = mutableListOf<Throwable>()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            appScope = CoroutineScope(
                SupervisorJob() + dispatcher + CoroutineExceptionHandler { _, throwable ->
                    failures += throwable
                }
            ),
            settingsRepository = settingsRepository,
            defaultDispatcher = dispatcher,
        )
        coEvery { homeSnapshotStore.readSnapshot() } throws IOException("read failed")

        repository.refreshHomeSnapshotAsync()
        advanceUntilIdle()

        assertTrue(failures.isEmpty())
    }

    @Test
    fun isSnapshotUsable_isTheSchemaEngineZoneAndDateWindow_withoutPkOrChartOption() = runTest {
        val anchorDate = LocalDate.of(2026, 5, 6)
        val zoneId = ZoneId.systemDefault()
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(StandardTestDispatcher(testScheduler)),
            defaultDispatcher = StandardTestDispatcher(testScheduler),
        )
        // A 30-day record with no widget projection: neither matters any more.
        val snapshot = homeSnapshotRecord(generation = 0L, now = anchorDate.atTime(10, 15))
            .copy(option = HomeE2ChartWindowOption.THIRTY_DAYS)

        assertTrue(repository.isSnapshotUsable(snapshot, anchorDate.plusDays(10).atTime(23, 59), zoneId))
        assertFalse(repository.isSnapshotUsable(snapshot, anchorDate.plusDays(11).atTime(0, 0), zoneId))
        assertFalse(repository.isSnapshotUsable(snapshot, anchorDate.minusDays(1).atTime(23, 59), zoneId))
        assertFalse(
            repository.isSnapshotUsable(snapshot, anchorDate.atTime(12, 0), ZoneId.of(if (zoneId.id == "UTC") "Asia/Tokyo" else "UTC"))
        )
        // The fit, and every estimate keyed on it, are another engine's.
        assertFalse(
            repository.isSnapshotUsable(snapshot.copy(engineVersion = PK_ENGINE_VERSION + 1), anchorDate.atTime(12, 0), zoneId)
        )
        assertFalse(
            repository.isSnapshotUsable(
                snapshot.copy(schemaVersion = HOME_SNAPSHOT_SCHEMA_VERSION - 1),
                anchorDate.atTime(12, 0),
                zoneId,
            )
        )
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun aStoredRecordFromAnotherEngine_isRebuiltByTheLaunchRequest() = runTest {
        // An app update with a PK refit keeps the snapshot otherwise usable;
        // the launch request skips a usable one, so the engine check is what
        // makes it rebuild instead of showing the old engine's fit.
        val builds = stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        val now = LocalDateTime.of(2026, 5, 7, 7, 0)
        val current = homeSnapshotRecord(generation = 0L, now = now)
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )

        coEvery { homeSnapshotStore.readSnapshot() } returns current
        repository.refreshHomeSnapshotIfNeeded(now = now, force = false)
        assertEquals(0, builds.get())

        coEvery { homeSnapshotStore.readSnapshot() } returns current.copy(engineVersion = PK_ENGINE_VERSION + 1)
        repository.refreshHomeSnapshotIfNeeded(now = now, force = false)
        assertEquals(1, builds.get())
        val written = slot<HomeSnapshotRecord>()
        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(capture(written)) }
        assertEquals(PK_ENGINE_VERSION, written.captured.engineVersion)
    }

    @Test
    fun refreshHomeSnapshotIfNeeded_capturesRowsForTenDaySnapshotValidity() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        val anchorDate = now.toLocalDate()
        val zoneId = ZoneId.systemDefault()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val writtenSnapshot = slot<HomeSnapshotRecord>()
        val scheduledStartIso = slot<String>()
        val scheduledEndIso = slot<String>()
        val manualStartEpochMillis = slot<Long>()
        val manualEndEpochMillis = slot<Long>()
        val expectedProjectionWindowEnd = anchorDate.plusDays(14)
            .atStartOfDay(zoneId)
            .toInstant()
            .toEpochMilli()

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery {
            homeDao.getScheduleEntries(
                capture(scheduledStartIso),
                capture(scheduledEndIso),
                capture(manualStartEpochMillis),
                capture(manualEndEpochMillis),
            )
        } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getByUuids(any()) } returns emptyList()
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null

        HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        ).refreshHomeSnapshotIfNeeded(now = now, force = true)

        assertTrue(writtenSnapshot.isCaptured)
        assertEquals(
            anchorDate.minusDays(1).atStartOfDay().toString(),
            scheduledStartIso.captured,
        )
        assertEquals(
            anchorDate.plusDays(100).atTime(23, 59, 59).toString(),
            scheduledEndIso.captured,
        )
        assertEquals(
            anchorDate.minusDays(1).atStartOfDay(zoneId).toInstant().toEpochMilli(),
            manualStartEpochMillis.captured,
        )
        assertEquals(
            anchorDate.plusDays(11).atStartOfDay(zoneId).toInstant().toEpochMilli(),
            manualEndEpochMillis.captured,
        )
        assertEquals(
            expectedProjectionWindowEnd,
            checkNotNull(writtenSnapshot.captured.widgetPkProjection).windowEndEpochMillis,
        )
    }

    @Test
    fun buildSnapshot_storesWhatHomesEstimateIsDerivedFrom_andTheWidgetsSevenDays() = runTest {
        // Home's estimate is no longer in the record: the record carries the
        // option, the fit, the labs and the real profile it is derived from.
        // Home's 30-day option does not reach the widget.
        chartWindowOptionState.value = HomeE2ChartWindowOption.THIRTY_DAYS
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        val zoneId = ZoneId.systemDefault()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val writtenSnapshot = slot<HomeSnapshotRecord>()
        stubSlowEmptyRefreshInputs(buildDelaysMs = listOf(0L))
        coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit

        HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        ).refreshHomeSnapshotIfNeeded(now = now, force = true, zoneId = zoneId)

        val record = writtenSnapshot.captured
        assertEquals(PK_ENGINE_VERSION, record.engineVersion)
        assertEquals(HomeE2ChartWindowOption.THIRTY_DAYS, record.option)
        assertNotNull(record.fit)
        assertEquals(0, record.fit?.epoch)
        assertEquals(UserProfile(), record.userProfile)
        val widget = checkNotNull(record.widgetPkProjection)
        assertEquals(now.atZone(zoneId).toInstant().toEpochMilli(), widget.generatedAtEpochMillis)
        assertEquals(
            now.toLocalDate().minusDays(3).atStartOfDay(zoneId).toInstant().toEpochMilli(),
            widget.windowStartEpochMillis,
        )
        assertEquals(
            now.toLocalDate().plusDays(14).atStartOfDay(zoneId).toInstant().toEpochMilli(),
            widget.windowEndEpochMillis,
        )
        assertEquals(widget.windowEndEpochMillis, widget.pkProjectionExpiresAtEpochMillis)
    }

    @Test
    fun buildSnapshot_storesFirstPinnedTrackedDateAsHomeAnchor() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val writtenSnapshot = slot<HomeSnapshotRecord>()

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getByUuids(any()) } returns emptyList()
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns TrackedDateEntity(
            uuid = "hero-1",
            name = "HRT start",
            iconKey = "favorite",
            dateIso = "2025-03-14",
            paletteKey = null,
            heroBackgroundKey = null,
            pinnedOrder = 0,
            createdAtEpochMillis = 10L,
            updatedAtEpochMillis = 10L,
        )

        HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        ).refreshHomeSnapshotIfNeeded(now = now, force = true)

        assertEquals("hero-1", writtenSnapshot.captured.homeAnchor?.id)
        assertEquals(LocalDate.of(2025, 3, 14), writtenSnapshot.captured.homeAnchor?.date)
    }

    @Test
    fun refreshHomeSnapshotIfNeeded_usesProvidedZoneForSnapshotWindows() = runTest {
        val originalTimeZone = TimeZone.getDefault()
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
        try {
            val now = LocalDateTime.of(2026, 5, 6, 10, 15)
            val anchorDate = now.toLocalDate()
            val zoneId = ZoneId.of("America/New_York")
            val dispatcher = StandardTestDispatcher(testScheduler)
            val database: HrtTrackerDatabase = mockk()
            val homeDao: HomeDao = mockk()
            val medicineDao: MedicineDao = mockk()
            val medicationLogDao: MedicationLogDao = mockk()
            val userProfileDao: UserProfileDao = mockk()
            val journalDao: JournalDao = mockk()
            val writtenSnapshot = slot<HomeSnapshotRecord>()
            val manualStartEpochMillis = slot<Long>()
            val manualEndEpochMillis = slot<Long>()

            coEvery { homeSnapshotStore.readSnapshot() } returns null
            coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit
            coEvery { databaseHolder.awaitOpen() } returns database
            every { database.homeDao() } returns homeDao
            every { database.medicineDao() } returns medicineDao
            every { database.medicationLogDao() } returns medicationLogDao
            every { database.userProfileDao() } returns userProfileDao
            every { database.journalDao() } returns journalDao
            every { database.bloodTestDao() } returns mockk<BloodTestDao> {
                coEvery { getPanels() } returns emptyList()
            }
            every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
                coEvery { getAllMetadata() } returns emptyList()
            }
            coEvery { homeDao.getActiveGroups() } returns emptyList()
            coEvery { homeDao.getArchivedGroups() } returns emptyList()
            coEvery {
                homeDao.getScheduleEntries(
                    any(),
                    any(),
                    capture(manualStartEpochMillis),
                    capture(manualEndEpochMillis),
                )
            } returns emptyList()
            coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
            coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
            coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
            coEvery { medicineDao.getByUuids(any()) } returns emptyList()
            coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
            coEvery {
                medicationLogDao.getScheduledEntriesInWindow(
                    any(),
                    any()
                )
            } returns emptyList()
            coEvery { userProfileDao.getProfile() } returns null
            coEvery { journalDao.getFirstPinnedTrackedDate() } returns null

            HomeSnapshotRepository(
                databaseHolder = databaseHolder,
                homeSnapshotStore = homeSnapshotStore,
                homeSnapshotGenerationStore = homeSnapshotGenerationStore,
                e2EstimateRepository = { e2EstimateRepository },
                settingsRepository = settingsRepository,
                appScope = CoroutineScope(dispatcher),
                defaultDispatcher = dispatcher,
            ).refreshHomeSnapshotIfNeeded(now = now, force = true, zoneId = zoneId)

            assertEquals(
                anchorDate.minusDays(1).atStartOfDay(zoneId).toInstant().toEpochMilli(),
                manualStartEpochMillis.captured,
            )
            assertEquals(
                anchorDate.plusDays(11).atStartOfDay(zoneId).toInstant().toEpochMilli(),
                manualEndEpochMillis.captured,
            )
            assertEquals(zoneId.id, writtenSnapshot.captured.zoneId)
        } finally {
            TimeZone.setDefault(originalTimeZone)
        }
    }

    @Test
    fun refreshHomeSnapshotIfNeeded_persistsStockInputs() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        val anchorDate = now.toLocalDate()
        val dispatcher = StandardTestDispatcher(testScheduler)
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val writtenSnapshot = slot<HomeSnapshotRecord>()
        val stockStartIso = slot<String>()
        val stockEndIso = slot<String>()
        val medicineUuid = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001")
        val stockMedicine = medicineEntity(
            uuid = medicineUuid,
            displayName = "Tracked stock",
            trackingEnabled = true,
            stockUnitsRemaining = 2.0,
            stockUnitsLastTotal = 10.0,
        )
        val stockEntry = medicationLogEntryEntity(
            uuid = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000001"),
            medicineUuid = medicineUuid,
            scheduledFor = anchorDate.plusDays(2).atTime(8, 0),
        )

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.writeSnapshot(capture(writtenSnapshot)) } returns Unit
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns listOf(stockMedicine)
        coEvery { medicineDao.getByUuids(any()) } returns listOf(stockMedicine)
        coEvery {
            medicationLogDao.getScheduledEntriesInWindow(
                capture(stockStartIso),
                capture(stockEndIso),
            )
        } returns listOf(stockEntry)
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null

        HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        ).refreshHomeSnapshotIfNeeded(now = now, force = true)

        assertTrue(writtenSnapshot.isCaptured)
        assertEquals(
            anchorDate.minusDays(1).atStartOfDay().toString(),
            stockStartIso.captured,
        )
        assertEquals(
            anchorDate.plusDays(ScheduledRunwayCalculator.HORIZON_DAYS)
                .atTime(23, 59, 59)
                .toString(),
            stockEndIso.captured,
        )
        assertEquals(
            listOf(medicineUuid),
            writtenSnapshot.captured.stockMedicines.map { medicine -> medicine.uuid },
        )
        assertEquals(
            listOf(stockEntry.uuid),
            writtenSnapshot.captured.stockFulfillmentEntries.map { entry -> entry.uuid.toString() },
        )
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun invalidateHomeSnapshot_waitsForInFlightSnapshotWriteBeforeClearing() = runTest {
        val now = LocalDateTime.of(2026, 5, 6, 10, 15)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        val writeStarted = CompletableDeferred<Unit>()
        val allowWrite = CompletableDeferred<Unit>()

        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers {
            writeStarted.complete(Unit)
            allowWrite.await()
        }
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } returns emptyList()
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } returns emptyList()
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } returns emptyList()
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } returns null
        coEvery { medicineDao.getByUuids(any()) } returns emptyList()
        coEvery { medicineDao.getAllActiveTrackedEntities() } returns emptyList()
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(dispatcher),
            defaultDispatcher = dispatcher,
        )

        val refreshJob = launch {
            repository.refreshHomeSnapshotIfNeeded(now = now, force = true)
        }
        advanceUntilIdle()
        writeStarted.await()

        val invalidateJob = launch {
            repository.invalidateHomeSnapshot()
        }
        advanceUntilIdle()

        assertEquals(false, invalidateJob.isCompleted)

        allowWrite.complete(Unit)
        refreshJob.join()
        invalidateJob.join()

        coVerify(exactly = 1) { homeSnapshotStore.writeSnapshot(any()) }
        coVerify(exactly = 1) { homeSnapshotStore.clearSnapshot() }
    }


    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun aStoredFit_standsInForTheSolve_whenNothingItReadsChanged() = runTest {
        // Each edit is a mutation, whose clear drops the stored snapshot
        // before its build runs. Mutations' builds read the wall clock.
        val now = LocalDateTime.now()
        val room = calibrationRoom(now)
        stubRoom(room)
        val written = java.util.Collections.synchronizedList(mutableListOf<HomeSnapshotRecord>())
        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        var solves = 0
        mockkObject(PkCalibrationEngine)
        try {
            every { PkCalibrationEngine.evaluate(any()) } answers {
                solves++
                callOriginal()
            }
            repository.refreshHomeSnapshotIfNeeded(now = now, force = true)
            assertEquals(1, solves)

            val edits = listOf<() -> Unit>(
                // A testosterone-only result.
                { room.panels += labPanel(UUID(0L, 7L), now.minusDays(1), 30.0, BloodAnalyteKey.T) },
                // An antiandrogen dose.
                {
                    room.antiandrogens += room.doses.last().copy(
                        uuid = UUID(0L, 8L).toString(),
                        category = MedicationCategory.ANTIANDROGEN.name,
                    )
                },
                // A stock recount of the E2 medicine every dose embeds.
                { room.medicine = room.medicine.copy(stockUnitsRemaining = 3.0, updatedAtEpochMillis = 900L) },
                // Accepting the accepted result again: only its timestamp moves.
                { room.metadata = room.metadata.map { review -> review.copy(updatedAtEpochMillis = 2_000L) } },
                // The 7/30 switch.
                { chartWindowOptionState.value = HomeE2ChartWindowOption.THIRTY_DAYS },
            )
            for (edit in edits) {
                val before = written.size
                repository.runHomeDataMutation { edit() }
                awaitRebuild(testScheduler) { written.size == before + 1 }
            }
            // Midnight.
            repository.refreshHomeSnapshotIfNeeded(now = now.plusDays(1), force = true)
        } finally {
            unmockkObject(PkCalibrationEngine)
        }

        assertEquals(1, solves)
        assertEquals(7, written.size)
        val fit = checkNotNull(written.first().fit)
        assertTrue(fit.outcome is FitOutcome.Solved)
        assertTrue(written.all { record -> record.fit === fit })
        // Written beside it, the labs and reviews it was solved from: a
        // review's timestamp, which the fit doesn't read, aside.
        assertTrue(written.all { record -> record.labs == written.first().labs })
        for (disposition in E2CalibrationDisposition.entries) {
            assertEquals(
                setOf(written.first().labMetadata.resultIdsWith(disposition)),
                written.map { record -> record.labMetadata.resultIdsWith(disposition) }.toSet(),
            )
        }
        assertEquals(2_000L, written.last().labMetadata.single().updatedAt.toEpochMilli())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun aStoredFit_isSolvedAgain_whenItFailed_orWhileATryAgainIsPending() = runTest {
        val now = LocalDateTime.now()
        val room = calibrationRoom(now)
        stubRoom(room)
        val written = java.util.Collections.synchronizedList(mutableListOf<HomeSnapshotRecord>())
        coEvery { homeSnapshotStore.readSnapshot() } returns null
        coEvery { homeSnapshotStore.clearSnapshot() } returns Unit
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        var solves = 0
        mockkObject(PkCalibrationEngine)
        try {
            every { PkCalibrationEngine.evaluate(any()) } answers {
                if (solves++ == 0) throw IllegalStateException("solver") else callOriginal()
            }
            // A transient failure: the next build, here an antiandrogen
            // dose's, solves again without the user acting.
            repository.refreshHomeSnapshotIfNeeded(now = now, force = true)
            repository.runHomeDataMutation {
                room.antiandrogens += room.doses.last().copy(
                    uuid = UUID(0L, 8L).toString(),
                    category = MedicationCategory.ANTIANDROGEN.name,
                )
            }
            awaitRebuild(testScheduler) { written.size == 2 }
            // Try again over a solved fit whose inputs are unchanged.
            repository.retryCalibration()
            awaitRebuild(testScheduler) { written.size == 3 }
            // Satisfied: the next build (its own minute, so it doesn't join
            // the retry's) reuses it.
            repository.refreshHomeSnapshotIfNeeded(now = now.plusHours(1), force = true)
        } finally {
            unmockkObject(PkCalibrationEngine)
        }

        assertEquals(3, solves)
        assertEquals(listOf(0, 0, 1, 1), written.map { record -> record.fit?.epoch })
        assertEquals(FitOutcome.Failed, written[0].fit?.outcome)
        assertTrue(written.drop(1).all { record -> record.fit?.outcome is FitOutcome.Solved })
        assertEquals(1, written.map { record -> record.fit?.fitId }.distinct().size)
        assertTrue(written[3].fit === written[2].fit)
    }

    @Test
    fun aLaunchBuild_reusesTheFitAnEarlierProcessStored() = runTest {
        // The earlier process's snapshot has outlived its ten-day window, so
        // the launch request rebuilds; nothing the fit read has changed.
        val solvedAt = LocalDateTime.of(2026, 5, 20, 10, 0)
        stubRoom(calibrationRoom(solvedAt))
        val written = mutableListOf<HomeSnapshotRecord>()
        coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
        fun process() = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { e2EstimateRepository },
            settingsRepository = settingsRepository,
            appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob()),
            defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
            diagnosticsLogger = diagnosticsLogger,
        )
        var solves = 0
        mockkObject(PkCalibrationEngine)
        try {
            every { PkCalibrationEngine.evaluate(any()) } answers {
                solves++
                callOriginal()
            }
            coEvery { homeSnapshotStore.readSnapshot() } returns null
            process().refreshHomeSnapshotIfNeeded(now = solvedAt, force = true)
            val stored = written.single()

            coEvery { homeSnapshotStore.readSnapshot() } returns stored
            process().refreshHomeSnapshotIfNeeded(now = solvedAt.plusDays(12))
        } finally {
            unmockkObject(PkCalibrationEngine)
        }

        assertEquals(2, written.size)
        assertEquals(1, solves)
        assertEquals(written[0].fit, written[1].fit)
    }

    @Test
    fun theFitsIdentity_followsNoClock_soAnotherDayOrOptionNeedsNoSolve() = runTest {
        // Doses go back further than any window. Without labs the read used
        // to start at the option's PK window, which moved with the date.
        for (withLabs in listOf(false, true)) {
            val today = LocalDateTime.of(2026, 5, 20, 10, 0)
            stubRoom(calibrationRoom(today, withLabs = withLabs))
            chartWindowOptionState.value = HomeE2ChartWindowOption.SEVEN_DAYS
            val written = mutableListOf<HomeSnapshotRecord>()
            coEvery { homeSnapshotStore.readSnapshot() } returns null
            coEvery { homeSnapshotStore.writeSnapshot(any()) } coAnswers { written += firstArg<HomeSnapshotRecord>() }
            val repository = HomeSnapshotRepository(
                databaseHolder = databaseHolder,
                homeSnapshotStore = homeSnapshotStore,
                homeSnapshotGenerationStore = homeSnapshotGenerationStore,
                e2EstimateRepository = { e2EstimateRepository },
                settingsRepository = settingsRepository,
                appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob()),
                defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
                diagnosticsLogger = diagnosticsLogger,
            )
            var solves = 0
            mockkObject(PkCalibrationEngine)
            try {
                every { PkCalibrationEngine.evaluate(any()) } answers {
                    solves++
                    callOriginal()
                }
                repository.refreshHomeSnapshotIfNeeded(now = today, force = true)
                chartWindowOptionState.value = HomeE2ChartWindowOption.THIRTY_DAYS
                repository.refreshHomeSnapshotIfNeeded(now = today.plusDays(3).withHour(22), force = true)
            } finally {
                unmockkObject(PkCalibrationEngine)
            }

            assertEquals(1, solves)
            assertEquals(written[0].fit?.fitId, written[1].fit?.fitId)
            assertTrue(written[1].fit === written[0].fit)
            val expected = if (withLabs) PkCalibrationGlobalState.READY else PkCalibrationGlobalState.NO_USABLE_LABS
            assertEquals(expected, (written[1].fit?.outcome as? FitOutcome.Solved)?.result?.globalState)
        }
    }

    @Test
    fun theBuild_readsEveryEstimatesLookback_whateverTheOption_withoutMovingHomesKeys() = runTest {
        val now = LocalDateTime.of(2026, 5, 20, 10, 0)
        val zoneId = ZoneId.systemDefault()
        for (option in HomeE2ChartWindowOption.entries) {
            val room = calibrationRoom(now)
            stubRoom(room)
            chartWindowOptionState.value = option
            val written = slot<HomeSnapshotRecord>()
            coEvery { homeSnapshotStore.readSnapshot() } returns null
            coEvery { homeSnapshotStore.writeSnapshot(capture(written)) } returns Unit
            HomeSnapshotRepository(
                databaseHolder = databaseHolder,
                homeSnapshotStore = homeSnapshotStore,
                homeSnapshotGenerationStore = homeSnapshotGenerationStore,
                e2EstimateRepository = { e2EstimateRepository },
                settingsRepository = settingsRepository,
                appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob()),
                defaultDispatcher = UnconfinedTestDispatcher(testScheduler),
                diagnosticsLogger = diagnosticsLogger,
            ).refreshHomeSnapshotIfNeeded(now = now, force = true)

            // From 180 days before the calibration page's domain, 17 days
            // back, for every option.
            val pageLookback = now.toLocalDate().minusDays(17).atStartOfDay(zoneId).toInstant()
                .minus(PK_CALIBRATION_DOSE_LOOKBACK)
            val pkRead = room.pkReads.first { bounds -> bounds.last != Long.MAX_VALUE }
            assertEquals(pageLookback.toEpochMilli(), pkRead.first)
            val record = written.captured
            assertEquals(
                room.doses.filter { dose -> dose.appliedAtEpochMillis >= pageLookback.toEpochMilli() }.size,
                record.pkEntries.size,
            )
            // Home's and the widget's keys are the ones the entries their own
            // lookbacks read give.
            val homeLookback = now.toLocalDate().minusDays(option.pastDays).atStartOfDay(zoneId).toInstant()
                .minus(PK_CALIBRATION_DOSE_LOOKBACK)
            val homeOwn = record.copy(pkEntries = record.pkEntries.filter { entry -> !entry.appliedAt.isBefore(homeLookback) })
            assertTrue(homeOwn.pkEntries.size < record.pkEntries.size)
            assertEquals(
                E2Derivation.homeEstimateKey(homeOwn.e2Inputs(), now, zoneId),
                E2Derivation.homeEstimateKey(record.e2Inputs(), now, zoneId),
            )
            assertEquals(
                E2Derivation.widgetEstimateKey(homeOwn.e2Inputs(), now, zoneId),
                E2Derivation.widgetEstimateKey(record.e2Inputs(), now, zoneId),
            )
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun aRetry_survivesADiscardedRun_andTheFailedRenderIsntReturned() = runTest {
        val now = LocalDateTime.now()
        val room = calibrationRoom(now)
        stubRoom(room)
        val records = MutableStateFlow<HomeSnapshotRecord?>(null)
        val written = java.util.Collections.synchronizedList(mutableListOf<HomeSnapshotRecord>())
        every { homeSnapshotStore.observeSnapshot() } returns records
        coEvery { homeSnapshotStore.readSnapshot() } answers { records.value }
        coEvery { homeSnapshotStore.clearSnapshot() } answers { records.value = null }
        coEvery { homeSnapshotStore.writeSnapshot(any()) } answers {
            written += firstArg<HomeSnapshotRecord>()
            records.value = firstArg()
        }
        val appScope = CoroutineScope(StandardTestDispatcher(testScheduler) + SupervisorJob())
        lateinit var estimates: E2EstimateRepository
        val repository = HomeSnapshotRepository(
            databaseHolder = databaseHolder,
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = homeSnapshotGenerationStore,
            e2EstimateRepository = { estimates },
            settingsRepository = settingsRepository,
            appScope = appScope,
            // Real threads: the retry's solve blocks one while the
            // mutation's build runs.
            defaultDispatcher = Dispatchers.Default,
            diagnosticsLogger = diagnosticsLogger,
        )
        estimates = E2EstimateRepository(
            homeSnapshotRepository = repository,
            store = mockk(relaxed = true) { coEvery { read() } returns null },
            clock = java.time.Clock.systemDefaultZone(),
            appScope = appScope,
            defaultDispatcher = StandardTestDispatcher(testScheduler),
        )
        val solves = AtomicInteger()
        val retrySolveStarted = java.util.concurrent.CountDownLatch(1)
        val releaseRetrySolve = java.util.concurrent.CountDownLatch(1)
        var renders = 0
        val states = mutableListOf<E2State<CalibrationEstimate>>()
        mockkObject(PkCalibrationEngine, E2Derivation)
        try {
            every { PkCalibrationEngine.evaluate(any()) } answers {
                if (solves.incrementAndGet() == 2) {
                    retrySolveStarted.countDown()
                    releaseRetrySolve.await()
                }
                callOriginal()
            }
            every { E2Derivation.deriveCalibrationRender(any(), any(), any()) } answers {
                if (renders++ == 0) throw IllegalStateException("render") else callOriginal()
            }
            repository.refreshHomeSnapshotIfNeeded(now = now, force = true)
            backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
                estimates.observeCalibration(MutableStateFlow(now)).collect { state -> states += state }
            }
            awaitRebuild(testScheduler) { states.lastOrNull()?.value != null }
            // The page shows the failed render's numeric failure, with Try again.
            assertNull(states.last().value?.result)

            // Try again: its build pauses in the solve it forces, and a non-E2
            // edit commits meanwhile, which drops it.
            repository.retryCalibration()
            awaitRebuild(testScheduler) { retrySolveStarted.count == 0L }
            repository.runHomeDataMutation {
                room.antiandrogens += room.doses.last().copy(
                    uuid = UUID(0L, 8L).toString(),
                    category = MedicationCategory.ANTIANDROGEN.name,
                )
            }
            awaitRebuild(testScheduler) { states.last().value?.result != null }
            // The dropped run finishes its solve and unwinds.
            releaseRetrySolve.countDown()
            repository.awaitRebuild()
        } finally {
            releaseRetrySolve.countDown()
            unmockkObject(PkCalibrationEngine, E2Derivation)
        }

        // The mutation's build solved again with the requested epoch rather
        // than reusing the old fit; the dropped one wrote nothing.
        assertEquals(listOf(0, 1), written.map { record -> record.fit?.epoch })
        assertEquals(3, solves.get())
        assertEquals(2, renders)
        assertNotNull(states.last().value?.render)
    }

    private class CalibrationRoom(
        var doses: List<MedicationLogEntryEntity>,
        var panels: List<BloodTestPanelWithResultsEntity>,
        var metadata: List<E2CalibrationMetadataEntity>,
        var medicine: MedicineEntity,
        var antiandrogens: List<MedicationLogEntryEntity> = emptyList(),
    ) {
        /** Every getEstradiolPkEntries read's bounds. */
        val pkReads: MutableList<LongRange> = java.util.Collections.synchronizedList(mutableListOf())
    }

    /**
     * A morning oral dose on each of the 300 days before [now], and, with
     * labs, three E2 results an hour after one, the first accepted.
     */
    private fun calibrationRoom(now: LocalDateTime, withLabs: Boolean = true): CalibrationRoom {
        val medicineUuid = UUID.fromString("aaaaaaaa-0000-0000-0000-0000000000e2")
        return CalibrationRoom(
            doses = (1L..300L).map { daysAgo ->
                val at = now.toLocalDate().minusDays(daysAgo).atTime(8, 0)
                medicationLogEntryEntity(UUID.nameUUIDFromBytes("e2:$at".toByteArray()), medicineUuid, at)
            },
            panels = if (withLabs) {
                listOf(20L, 12L, 5L).mapIndexed { index, daysAgo ->
                    labPanel(UUID(0L, index + 1L), now.toLocalDate().minusDays(daysAgo).atTime(9, 0), 140.0)
                }
            } else {
                emptyList()
            },
            metadata = if (withLabs) {
                listOf(E2CalibrationMetadataEntity(UUID(0L, 1L).toString(), E2CalibrationDisposition.REVIEWED.name, 1_000L))
            } else {
                emptyList()
            },
            medicine = medicineEntity(uuid = medicineUuid, displayName = "E2"),
        )
    }

    /** Room as [room] has it, read with each query's bounds. */
    private fun stubRoom(room: CalibrationRoom) {
        val database: HrtTrackerDatabase = mockk()
        val homeDao: HomeDao = mockk()
        val medicineDao: MedicineDao = mockk()
        val medicationLogDao: MedicationLogDao = mockk()
        val userProfileDao: UserProfileDao = mockk()
        val journalDao: JournalDao = mockk()
        coEvery { databaseHolder.awaitOpen() } returns database
        every { database.homeDao() } returns homeDao
        every { database.medicineDao() } returns medicineDao
        every { database.medicationLogDao() } returns medicationLogDao
        every { database.userProfileDao() } returns userProfileDao
        every { database.journalDao() } returns journalDao
        every { database.bloodTestDao() } returns mockk<BloodTestDao> {
            coEvery { getPanels() } answers { room.panels }
        }
        every { database.pkCalibrationDao() } returns mockk<PkCalibrationDao> {
            coEvery { getAllMetadata() } answers { room.metadata }
        }
        coEvery { homeDao.getActiveGroups() } returns emptyList()
        coEvery { homeDao.getArchivedGroups() } returns emptyList()
        coEvery { homeDao.getScheduleEntries(any(), any(), any(), any()) } returns emptyList()
        coEvery { homeDao.getLatestAntiandrogenEntriesOnOrBefore(any()) } answers { room.antiandrogens }
        coEvery { homeDao.getEstradiolPkEntries(any(), any()) } answers {
            val bounds = firstArg<Long>()..secondArg<Long>()
            room.pkReads += bounds
            room.doses
                .filter { dose -> dose.appliedAtEpochMillis in bounds }
                .sortedByDescending { dose -> dose.appliedAtEpochMillis }
        }
        coEvery { homeDao.getLatestEstradiolEntryOnOrBefore(any()) } answers {
            room.doses
                .filter { dose -> dose.appliedAtEpochMillis <= firstArg<Long>() }
                .maxByOrNull { dose -> dose.appliedAtEpochMillis }
        }
        coEvery { medicineDao.getAllActiveTrackedEntities() } answers { listOf(room.medicine) }
        coEvery { medicineDao.getByUuids(any()) } answers { listOf(room.medicine) }
        coEvery { medicationLogDao.getScheduledEntriesInWindow(any(), any()) } returns emptyList()
        coEvery { userProfileDao.getProfile() } returns null
        coEvery { journalDao.getFirstPinnedTrackedDate() } returns null
    }

    private fun labPanel(
        resultId: UUID,
        collectedAt: LocalDateTime,
        value: Double,
        analyte: BloodAnalyteKey = BloodAnalyteKey.E2,
    ): BloodTestPanelWithResultsEntity {
        val panelUuid = UUID.nameUUIDFromBytes("panel:$resultId".toByteArray()).toString()
        return BloodTestPanelWithResultsEntity(
            panel = BloodTestPanelEntity(
                uuid = panelUuid,
                collectedAtInstantEpochMillis = collectedAt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                collectedAtTimeZoneId = ZoneId.systemDefault().id,
                notes = null,
                timeSinceLastEstradiolDoseMillis = null,
                timeSinceLastTestosteroneDoseMillis = null,
                createdAtEpochMillis = 0L,
                updatedAtEpochMillis = 0L,
            ),
            results = listOf(
                BloodTestResultEntity(
                    uuid = resultId.toString(),
                    panelUuid = panelUuid,
                    createdAtEpochMillis = 0L,
                    displayOrder = 0,
                    builtinAnalyteKey = analyte.storageValue,
                    customAnalyteUuid = null,
                    value = value,
                    unitSnapshot = "pg_ml",
                    canonicalValue = value,
                )
            ),
        )
    }

    private fun homeSnapshotRecord(
        generation: Long,
        now: LocalDateTime,
    ): HomeSnapshotRecord {
        val zoneId = ZoneId.systemDefault()
        return HomeSnapshotRecord(
            schemaVersion = HOME_SNAPSHOT_SCHEMA_VERSION,
            generation = generation,
            generatedAtEpochMillis = now.atZone(zoneId).toInstant().toEpochMilli(),
            anchorDateEpochDay = now.toLocalDate().toEpochDay(),
            zoneId = zoneId.id,
            activeGroups = emptyList(),
            scheduleEntries = emptyList(),
            antiandrogenHistoryEntries = emptyList(),
        )
    }

    private fun readyEvaluation() = PkCalibrationEvaluation(
        PkCalibrationResult(
            PkCalibrationGlobalState.READY,
            PkCalibrationRoute.entries.map { route ->
                PkRouteCalibrationResult(route, PkRouteCalibrationDisplayState.POPULATION_NO_LAB_SIGNAL)
            },
        ),
        evidence = null,
    )

    private fun groupWithTwoSlotsReferencing(medicineUuid: UUID): MedicationGroupWithItemsEntity {
        val groupUuid = UUID.fromString("44bff3cb-b4dd-4be4-9eda-442cf91185c1").toString()
        return MedicationGroupWithItemsEntity(
            group = MedicationGroupEntity(
                uuid = groupUuid,
                name = "Home estradiol",
                colorKey = MedicationGroupColorKey.PLUM.name,
                scheduleType = MedicationGroupScheduleType.DAILY.name,
                scheduleInterval = 1,
                scheduleSinceEpochDay = LocalDate.of(2026, 5, 1).toEpochDay(),
                createdAtEpochMillis = 0L,
                updatedAtEpochMillis = 0L,
            ),
            items = listOf(
                UUID.fromString("79dfe41c-b684-4e48-858d-d47d369b5b30"),
                UUID.fromString("79dfe41c-b684-4e48-858d-d47d369b5b31"),
            ).mapIndexed { slotIndex, slotUuid ->
                MedicationGroupItemEntity(
                    uuid = slotUuid.toString(),
                    groupUuid = groupUuid,
                    sortOrder = slotIndex,
                    count = 1,
                    medicineUuid = medicineUuid.toString(),
                    applicationType = MedicationApplicationType.ORAL.name,
                    doseInstructionKind = DoseInstructionKind.TABLET_FRACTION.name,
                    tabletFractionNumerator = 1,
                    tabletFractionDenominator = 1,
                    doseVolumeMl = null,
                    doseWeightGrams = null,
                )
            },
            scheduleTimes = listOf(
                MedicationGroupScheduleTimeEntity(
                    uuid = UUID.fromString("52d9acc8-11be-43ff-b6cb-035571ec0371").toString(),
                    groupUuid = groupUuid,
                    sortOrder = 0,
                    hourOfDay = 8,
                    minuteOfHour = 0,
                    effectiveFromLocalIso = LocalDate.of(2026, 5, 1).atStartOfDay().toString(),
                )
            ),
            weeklyDays = emptyList(),
        )
    }

    private fun medicineEntity(
        uuid: UUID,
        displayName: String?,
        trackingEnabled: Boolean = false,
        stockUnitsRemaining: Double? = null,
        stockUnitsLastTotal: Double? = null,
    ): MedicineEntity {
        return MedicineEntity(
            uuid = uuid.toString(),
            selectionKind = "CATALOG",
            medicationKey = MedicationKey.ESTRADIOL.name,
            customMedicationName = null,
            customMedicationNameNormalized = null,
            category = MedicationCategory.ESTRADIOL.name,
            preparationType = "PILL",
            strengthMgPerTablet = 2.0,
            strengthMgPerVial = null,
            concentrationMgPerMl = null,
            vialVolumeMl = null,
            concentrationPercent = null,
            sachetWeightGrams = null,
            containerWeightGrams = null,
            patchTotalMg = null,
            patchReleaseRateMcgPerDay = null,
            displayName = displayName,
            identityKey = "C|ESTRADIOL|PILL|strengthMgPerTablet=2",
            createdAtEpochMillis = 0L,
            updatedAtEpochMillis = 500L,
            archivedAtEpochMillis = null,
            trackingEnabled = trackingEnabled,
            stockUnitsRemaining = stockUnitsRemaining,
            stockUnitsLastTotal = stockUnitsLastTotal,
        )
    }

    private fun medicationLogEntryEntity(
        uuid: UUID,
        medicineUuid: UUID,
        scheduledFor: LocalDateTime,
    ): MedicationLogEntryEntity {
        return MedicationLogEntryEntity(
            uuid = uuid.toString(),
            category = MedicationCategory.ESTRADIOL.name,
            medicineUuid = medicineUuid.toString(),
            applicationType = MedicationApplicationType.ORAL.name,
            doseInstructionKind = DoseInstructionKind.TABLET_FRACTION.name,
            tabletFractionNumerator = 1,
            tabletFractionDenominator = 1,
            doseVolumeMl = null,
            doseWeightGrams = null,
            equivalentE2Mg = 2.0,
            sourceGroupUuid = UUID.fromString("cccccccc-0000-0000-0000-000000000001").toString(),
            scheduleTimeUuid = UUID.fromString("dddddddd-0000-0000-0000-000000000001").toString(),
            appliedAtEpochMillis = scheduledFor.atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli(),
            scheduledForIso = scheduledFor.toString(),
        )
    }
}

package com.mkx.hrttracker.data.repository

import com.mkx.hrttracker.model.medication.DoseInstruction
import com.mkx.hrttracker.model.medication.MedicationGroup
import com.mkx.hrttracker.model.medication.MedicationGroupSchedule
import com.mkx.hrttracker.model.medication.MedicationGroupScheduleType
import com.mkx.hrttracker.model.medication.MedicationLogEntry
import com.mkx.hrttracker.model.medication.MedicinePreparation
import com.mkx.hrttracker.model.medication.testMedicationGroupMedication
import com.mkx.hrttracker.model.medication.testMedicationLogEntry
import com.mkx.hrttracker.model.medication.testMedicine
import com.mkx.hrttracker.model.personalization.UserProfile
import com.mkx.hrttracker.model.pk.HomeE2ChartWindowOption
import com.mkx.hrttracker.model.pk.PkCalibrationEngine
import com.mkx.hrttracker.model.pk.PkCalibrationLab
import com.mkx.hrttracker.model.pk.PkCalibrationRenderer
import com.mkx.hrttracker.util.AppDiagnosticsLogger
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkAll
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.IOException
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
class E2EstimateRepositoryTest {
    private val zoneId: ZoneId = ZoneId.systemDefault()
    private val today = LocalDate.of(2026, 5, 20)

    // Before and after today's 08:00 slot, which is still planned at the first.
    private val beforeSlot = today.atTime(7, 30)
    private val afterSlot = today.atTime(8, 5)

    private val estradiol = testMedicine(
        uuid = UUID.fromString("e2e2e2e2-0000-0000-0000-000000000000"),
        preparation = MedicinePreparation.Pill(strengthMgPerTablet = 2.0),
    )
    private val group = MedicationGroup(
        uuid = UUID.fromString("11111111-2222-3333-4444-555555555555"),
        name = "E2",
        schedule = MedicationGroupSchedule(
            type = MedicationGroupScheduleType.DAILY,
            interval = 1,
            since = today.minusDays(40),
            weeklyDaysOfWeek = emptySet(),
            times = listOf(LocalTime.of(8, 0), LocalTime.of(20, 0)),
        ),
        medications = listOf(
            testMedicationGroupMedication(
                uuid = UUID.fromString("66666666-7777-8888-9999-aaaaaaaaaaaa"),
                medicine = estradiol,
                doseInstruction = DoseInstruction.TabletFraction(1, 1),
            ),
        ),
        createdAt = Instant.parse("2026-03-01T00:00:00Z"),
        updatedAt = Instant.parse("2026-03-01T00:00:00Z"),
    )

    /** Every slot logged on time until today's 08:00 one. */
    private val logged: List<MedicationLogEntry> =
        (0..ChronoUnit.DAYS.between(group.schedule.since, today))
            .flatMap { day ->
                group.schedule.timeSlots.map { slot ->
                    val scheduledFor = group.schedule.since.plusDays(day).atTime(slot.time)
                    testMedicationLogEntry(
                        uuid = UUID.nameUUIDFromBytes("dose:$scheduledFor".toByteArray()),
                        medicine = estradiol,
                        doseInstruction = DoseInstruction.TabletFraction(1, 1),
                        sourceGroupUuid = group.uuid,
                        appliedAt = scheduledFor.atZone(zoneId).toInstant(),
                        appliedAtTimeZoneId = zoneId.id,
                        scheduledFor = scheduledFor,
                        scheduleTimeUuid = slot.uuid,
                    )
                }
            }
            .filter { entry -> entry.scheduledFor!!.isBefore(beforeSlot) }

    private val records = MutableStateFlow<HomeSnapshotRecord?>(null)
    private val generation = MutableStateFlow(0L)
    private val homeSnapshotStore: HomeSnapshotStore = mockk {
        every { observeSnapshot() } returns records
        coEvery { readSnapshot() } answers { records.value }
    }
    private val generationStore: HomeSnapshotGenerationStore = mockk {
        every { observeGeneration() } returns generation
        coEvery { readGeneration() } answers { generation.value }
    }
    private val snapshotLogger: AppDiagnosticsLogger = mockk(relaxed = true)

    private var stored: HomeE2EstimateRecord? = null
    private var writes = 0
    private val estimateStore: HomeE2EstimateStore = mockk {
        coEvery { read() } answers { stored }
        coEvery { write(any()) } answers {
            stored = firstArg()
            writes++
        }
    }
    private val clock = MutableClock(beforeSlot.atZone(zoneId).toInstant())
    private val minutes = MutableStateFlow(beforeSlot)
    private var derivations = 0

    @Before
    fun setUp() {
        mockkObject(E2Derivation)
        every { E2Derivation.deriveHomeEstimate(any(), any(), any(), any()) } answers {
            derivations++
            callOriginal()
        }
    }

    @After
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun coldStart_withAStoredMatch_showsItInTheFirstState_withoutDeriving() = runTest {
        records.value = record()
        stored = storedFor(record(), beforeSlot)
        val estimates = estimates()

        val states = observe(estimates)
        runCurrent()

        assertSame(stored?.estimate, states.first().value)
        assertNull(states.first().pending)
        assertEquals(0, derivations)
        assertEquals(0, writes)
    }

    @Test
    fun coldStart_withoutAMatch_hasNoValueUntilDerived_thenStoresIt() = runTest {
        records.value = record()
        // Stored before the slot passed: its planned doses no longer match.
        stored = storedFor(record(), beforeSlot)
        setNow(afterSlot)
        val estimates = estimates()

        val states = observe(estimates)
        runCurrent()

        assertNull(states.first().value)
        assertEquals(1, derivations)
        val shown = checkNotNull(states.last().value)
        assertNull(states.last().pending)
        assertEquals(keyAt(record(), afterSlot), stored?.key)
        assertSame(shown, stored?.estimate)
        assertEquals(HomeE2ChartWindowOption.SEVEN_DAYS, shown.option)
    }

    @Test
    fun coldStart_withNoInputs_hasNoValue_andAsksForARebuild() = runTest {
        stored = storedFor(record(), beforeSlot)
        val estimates = estimates()

        val states = observe(estimates)
        runCurrent()

        assertEquals(listOf(E2State<HomeE2Estimate>(null)), states.distinct())
        assertEquals(0, derivations)
        verify {
            snapshotLogger.info("HomeSnapshotRepository", match { it.startsWith("home_snapshot_refresh_async_enqueued") })
        }
    }

    @Test
    fun theFirstState_readsTheWallClock_notTheCachedMinute() = runTest {
        // On a resume the minute flow can still hold the time the app left
        // at, from before the slot passed, when the stored estimate matched.
        records.value = record()
        stored = storedFor(record(), beforeSlot)
        clock.instant = afterSlot.atZone(zoneId).toInstant()
        val estimates = estimates()

        val states = observe(estimates) // minutes still says 07:30
        runCurrent()

        assertNull(states.first().value)
        assertEquals(keyAt(record(), afterSlot), stored?.key)
        assertTrue(states.none { state -> state.value != null && state.value === storedFor(record(), beforeSlot).estimate })
    }

    @Test
    fun aRestart_withAMovedKeyHasNoValue_andWithAMatchingKeyHasItAtOnce() = runTest {
        records.value = record()
        val estimates = estimates()
        val first = observe(estimates)
        runCurrent()
        val beforeSlotEstimate = checkNotNull(first.last().value)
        stopAll()

        // Away while the slot passed.
        setNow(afterSlot)
        val returned = observe(estimates)
        assertNull(returned.first().value)
        runCurrent()
        val afterSlotEstimate = checkNotNull(returned.last().value)
        assertNotEquals(beforeSlotEstimate, afterSlotEstimate)
        stopAll()

        // Away and back with nothing moved: the value in the first state,
        // before anything else runs.
        val again = observe(estimates)
        assertSame(afterSlotEstimate, again.first().value)
        assertEquals(2, derivations)
    }

    @Test
    fun inputsBehindAMutation_areNeverAdmitted_whileARunningObservationKeepsItsValue() = runTest {
        records.value = record()
        stored = storedFor(record(), beforeSlot)
        val estimates = estimates()
        val running = observe(estimates)
        runCurrent()
        val shown = checkNotNull(running.last().value)

        // A mutation commits while its rebuild is paused (or fails): the old
        // record and the stored estimate still match each other, but the
        // record is behind the durable generation.
        generation.value = 1L
        runCurrent()
        assertSame(shown, running.last().value)
        assertNull(running.last().pending)

        // A screen coming to the foreground meanwhile shows the skeleton.
        val returned = observe(estimates)
        runCurrent()
        assertTrue(returned.all { state -> state.value == null })
        // Its clear doesn't change that either.
        records.value = null
        runCurrent()
        assertSame(shown, running.last().value)
        assertTrue(returned.all { state -> state.value == null })
        assertEquals(0, derivations)
    }

    @Test
    fun pending_isUser_forNewerInputsFromAChange_andSystem_forTheClock() = runTest {
        records.value = record()
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        val initial = checkNotNull(states.last().value)

        // Logging a dose: a gated change, so the next record's generation is newer.
        val withDose = logged + dose(today.minusDays(1).atTime(12, 0))
        generation.value = 1L
        records.value = record(generation = 1L, logged = withDose)
        runCurrent()
        val afterLog = checkNotNull(states.last().value)
        val userTransition = states.dropWhile { state -> state.value !== initial || state.pending == null }
        assertEquals(E2State(initial, E2Pending.USER), userTransition.first())

        // The slot passing while the app is open: the same generation.
        setNow(afterSlot)
        runCurrent()
        val swapped = states.dropWhile { state -> state.value !== afterLog }
        assertEquals(E2State(afterLog, E2Pending.SYSTEM), swapped[1])
        assertNull(swapped.last().pending)
        assertTrue(swapped.last().value !== afterLog)
    }

    @Test
    fun aChangeTheEstimateDoesntRead_hasNothingPending() = runTest {
        records.value = record()
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        val shown = checkNotNull(states.last().value)
        val before = states.size

        // A stock recount: a gated change whose E2 inputs are the same.
        generation.value = 1L
        records.value = record(generation = 1L).copy(stockMedicines = listOf(estradiol))
        runCurrent()
        assertEquals(before, states.size)
        assertSame(shown, states.last().value)

        // And the clock after it is still the system's, not the user's.
        setNow(afterSlot)
        runCurrent()
        assertEquals(E2State(shown, E2Pending.SYSTEM), states[before])
        assertEquals(1 + 1, derivations)
    }

    @Test
    fun observers_shareInFlightDerivations_andTheMemoKeepsTheLastTwoKeys() = runTest {
        records.value = record(weightKg = 60.0)
        val estimates = estimates()
        val home = observe(estimates)
        val settings = observe(estimates)
        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(1, derivations)
        assertSame(home.last().value, settings.last().value)

        records.value = record(weightKg = 61.0)
        runCurrent()
        records.value = record(weightKg = 62.0)
        runCurrent()
        assertEquals(3, derivations)

        // 60 kg was the eldest of three: derived again. 62 kg is kept.
        records.value = record(weightKg = 60.0)
        runCurrent()
        assertEquals(4, derivations)
        records.value = record(weightKg = 62.0)
        runCurrent()
        assertEquals(4, derivations)
    }

    @Test
    fun aDerivationThatThrows_isMemoized_butNeverStored() = runTest {
        every { E2Derivation.deriveHomeEstimate(any(), any(), any(), any()) } answers {
            derivations++
            throw IllegalStateException("simulation")
        }
        records.value = record()
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        // A minute tick with the same key does not derive it again.
        setNow(beforeSlot.plusMinutes(1))
        runCurrent()

        assertTrue(states.all { state -> state.value == null })
        assertNull(states.last().pending)
        assertEquals(1, derivations)
        assertEquals(0, writes)
    }

    @Test
    fun aBandRenderThatThrows_isShownWithoutItsBand_butNeverStored() = runTest {
        mockkObject(PkCalibrationRenderer)
        every {
            PkCalibrationRenderer.render(
                result = any(),
                originEpochMillis = any(),
                weightKg = any(),
                config = any(),
                domain = any(),
                doseEvents = any(),
                statusRange = any(),
            )
        } throws IllegalStateException("render")
        records.value = record(fit = solvedFit(epoch = 0))
        val estimates = estimates()

        val states = observe(estimates)
        runCurrent()

        val shown = checkNotNull(states.last().value)
        assertTrue(shown.bandKnots.isEmpty())
        val status = checkNotNull(shown.calibration)
        assertTrue(status.adjusted)
        assertTrue(status.bandUnavailable)
        assertEquals(0, writes)
    }

    @Test
    fun theWriter_neverStoresAnEstimateWhoseKeyMovedWhileItWasDerived() = runTest {
        records.value = record()
        // A slow derivation: the slot passes before it lands.
        every { E2Derivation.deriveHomeEstimate(any(), any(), any(), any()) } answers {
            derivations++
            callOriginal().also { clock.instant = afterSlot.atZone(zoneId).toInstant() }
        }
        val estimates = estimates()

        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(0, writes)

        every { E2Derivation.deriveHomeEstimate(any(), any(), any(), any()) } answers {
            derivations++
            callOriginal()
        }
        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(1, writes)
        assertEquals(keyAt(record(), afterSlot), stored?.key)
    }

    @Test
    fun tryAgain_afterAFailedSolve_reDerivesFromUnchangedInputs() = runTest {
        // The build's solve failed: the population estimate is shown and
        // stored. Try again re-solves with a new epoch.
        val failed = E2FitRecord(solvedFit(epoch = 0).fitId, epoch = 0, outcome = FitOutcome.Failed)
        records.value = record(fit = failed)
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        assertNull(checkNotNull(states.last().value).calibration)

        generation.value = 1L
        records.value = record(generation = 1L, fit = solvedFit(epoch = 1))
        runCurrent()

        assertEquals(2, derivations)
        assertNotNull(checkNotNull(states.last().value).calibration)
        assertEquals(keyAt(record(fit = solvedFit(epoch = 1)), beforeSlot), stored?.key)
    }

    @Test
    fun tryAgain_afterAThrownRender_reDerives_evenWithTheSameOutcome() = runTest {
        // The render failed, not the solve: the re-solve gives the same
        // outcome, and only the epoch keeps the memo from returning the
        // band-less estimate.
        var renders = 0
        mockkObject(PkCalibrationRenderer)
        every {
            PkCalibrationRenderer.render(
                result = any(),
                originEpochMillis = any(),
                weightKg = any(),
                config = any(),
                domain = any(),
                doseEvents = any(),
                statusRange = any(),
            )
        } answers {
            if (renders++ == 0) throw IllegalStateException("render") else callOriginal()
        }
        records.value = record(fit = solvedFit(epoch = 0))
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        assertTrue(checkNotNull(states.last().value).bandKnots.isEmpty())
        assertEquals(0, writes)

        generation.value = 1L
        records.value = record(generation = 1L, fit = solvedFit(epoch = 1))
        runCurrent()

        assertEquals(2, derivations)
        assertTrue(checkNotNull(states.last().value).bandKnots.isNotEmpty())
        assertEquals(1, writes)
    }

    @Test
    fun warmHome_storesHomesKey_andAColdStartRightAfterShowsIt() = runTest {
        records.value = record()
        val estimates = estimates()

        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(keyAt(record(), beforeSlot), stored?.key)

        val states = observe(estimates)
        assertSame(stored?.estimate, states.first().value)
        assertEquals(1, derivations)
    }

    @Test
    fun warmHome_leavesInputsThatArentCurrent_alone() = runTest {
        records.value = record()
        generation.value = 1L
        val estimates = estimates()

        backgroundScope.launch { estimates.warmHome() }
        runCurrent()

        assertEquals(0, derivations)
        assertEquals(0, writes)
    }

    @Test
    fun warmHome_withAnUnchangedKey_onlyComputesTheKey_andAfterASlotPassesDerivesOnce() = runTest {
        records.value = record()
        stored = storedFor(record(), beforeSlot)
        val estimates = estimates()

        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(0, derivations)
        assertEquals(0, writes)

        clock.instant = afterSlot.atZone(zoneId).toInstant()
        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(1, derivations)
        assertEquals(1, writes)
        assertEquals(keyAt(record(), afterSlot), stored?.key)
    }

    @Test
    fun aFailedUserDerivation_leavesTheNextClockChangeToTheSystem() = runTest {
        val extra = dose(today.minusDays(1).atTime(12, 0))
        every { E2Derivation.deriveHomeEstimate(any(), any(), any(), any()) } answers {
            derivations++
            if (extra in firstArg<E2Inputs>().estradiolEntries) throw IllegalStateException("simulation")
            callOriginal()
        }
        records.value = record()
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        val shown = checkNotNull(states.last().value)

        // A logged dose whose estimate fails: the value stays, nothing pending.
        generation.value = 1L
        records.value = record(generation = 1L, logged = logged + extra)
        runCurrent()
        assertEquals(E2State(shown, null), states.last())
        assertTrue(E2State(shown, E2Pending.USER) in states)

        // The slot passing after it is the clock's change, not the user's.
        val before = states.size
        setNow(afterSlot)
        runCurrent()
        assertEquals(E2State(shown, E2Pending.SYSTEM), states[before])
    }

    @Test
    fun aSnapshotReadThatFails_isRetried_andLaterRecordsReachTheObservation() = runTest {
        // DataStore's IOException comes from outside the serializer, and would
        // otherwise end the inputs for the process's lifetime.
        var reads = 0
        every { homeSnapshotStore.observeSnapshot() } returns flow {
            if (reads++ == 0) throw IOException("read")
            emitAll(records)
        }
        records.value = record()
        val estimates = estimates()

        val states = observe(estimates)
        runCurrent()
        assertTrue(states.all { state -> state.value == null })

        advanceTimeBy(1_001L)
        runCurrent()
        assertNotNull(states.last().value)
        assertEquals(1, derivations)
    }

    @Test
    fun latestHome_isSeededFromTheStoredEstimate_andFollowsWhatAnObservationAdmits() = runTest {
        records.value = record()
        stored = storedFor(record(), beforeSlot)
        val estimates = estimates()
        val seed = checkNotNull(stored).estimate
        assertSame(seed, estimates.latestHome.value)

        // A warm-up derives and stores the moved key but leaves latestHome
        // alone; the observation admitting it from the memo updates it.
        clock.instant = afterSlot.atZone(zoneId).toInstant()
        backgroundScope.launch { estimates.warmHome() }
        runCurrent()
        assertEquals(keyAt(record(), afterSlot), stored?.key)
        assertSame(seed, estimates.latestHome.value)
        setNow(afterSlot)
        val states = observe(estimates)
        assertSame(states.first().value, estimates.latestHome.value)
        assertEquals(1, derivations)

        // A derivation updates it too.
        generation.value = 1L
        records.value = record(generation = 1L, weightKg = 70.0)
        runCurrent()
        assertSame(states.last().value, estimates.latestHome.value)
        assertEquals(2, derivations)
    }

    @Test
    fun latestHome_isNotTakenBackByAWarmUpForAnOlderKey() = runTest {
        records.value = record()
        setNow(afterSlot)
        val estimates = estimates()
        val states = observe(estimates)
        runCurrent()
        val newer = checkNotNull(states.last().value)

        // A warm-up that read the clock before the slot passed and lands now.
        clock.instant = beforeSlot.atZone(zoneId).toInstant()
        backgroundScope.launch { estimates.warmHome() }
        runCurrent()

        assertEquals(2, derivations)
        assertSame(newer, estimates.latestHome.value)
    }

    private val observers = mutableListOf<Job>()

    private fun TestScope.estimates(): E2EstimateRepository {
        val dispatcher = StandardTestDispatcher(testScheduler)
        lateinit var estimates: E2EstimateRepository
        val snapshots = HomeSnapshotRepository(
            databaseHolder = mockk(),
            homeSnapshotStore = homeSnapshotStore,
            homeSnapshotGenerationStore = generationStore,
            settingsRepository = mockk(relaxed = true),
            e2EstimateRepository = { estimates },
            appScope = backgroundScope,
            defaultDispatcher = dispatcher,
            diagnosticsLogger = snapshotLogger,
        )
        estimates = E2EstimateRepository(
            homeSnapshotRepository = snapshots,
            store = estimateStore,
            clock = clock,
            appScope = backgroundScope,
            defaultDispatcher = dispatcher,
        )
        // The stored estimate's load.
        runCurrent()
        return estimates
    }

    private fun TestScope.observe(estimates: E2EstimateRepository): List<E2State<HomeE2Estimate>> {
        val states = mutableListOf<E2State<HomeE2Estimate>>()
        observers += backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            estimates.observeHome(minutes).collect { state -> states += state }
        }
        return states
    }

    private fun stopAll() {
        observers.forEach(Job::cancel)
        observers.clear()
    }

    private fun setNow(now: LocalDateTime) {
        clock.instant = now.atZone(zoneId).toInstant()
        minutes.value = now
    }

    private fun record(
        generation: Long = 0L,
        weightKg: Double = 62.0,
        logged: List<MedicationLogEntry> = this.logged,
        fit: E2FitRecord? = null,
    ) = HomeSnapshotRecord(
        schemaVersion = HOME_SNAPSHOT_SCHEMA_VERSION,
        generation = generation,
        generatedAtEpochMillis = 0L,
        anchorDateEpochDay = today.toEpochDay(),
        zoneId = zoneId.id,
        activeGroups = listOf(group),
        scheduleEntries = emptyList(),
        antiandrogenHistoryEntries = emptyList(),
        userProfile = UserProfile(weightKg = weightKg),
        pkEntries = logged,
        option = HomeE2ChartWindowOption.SEVEN_DAYS,
        fit = fit,
    )

    private fun keyAt(record: HomeSnapshotRecord, now: LocalDateTime): HomeEstimateKey =
        E2Derivation.homeEstimateKey(record.e2Inputs(), now, zoneId)

    private fun storedFor(record: HomeSnapshotRecord, now: LocalDateTime): HomeE2EstimateRecord {
        val inputs = record.e2Inputs()
        return storedEstimates.getOrPut(keyAt(record, now)) {
            HomeE2EstimateRecord(
                key = keyAt(record, now),
                estimate = E2Derivation.deriveHomeEstimate(inputs, now, zoneId) { throw it }.also { derivations-- },
            )
        }
    }

    private val storedEstimates = mutableMapOf<HomeEstimateKey, HomeE2EstimateRecord>()

    // An hour after a morning dose, on three days: oral is adjusted, with a band.
    private fun solvedFit(epoch: Int): E2FitRecord {
        val labs = listOf(10L, 6L, 2L).mapIndexed { index, daysAgo ->
            PkCalibrationLab(
                resultId = UUID(0L, index + 1L),
                collectedAtEpochMillis = today.minusDays(daysAgo).atTime(9, 0).atZone(zoneId).toInstant().toEpochMilli(),
                valuePgml = 140.0,
            )
        }
        val input = buildPkCalibrationInput(labs = labs, entries = logged, weightKg = 62.0, metadata = emptyList())
        return E2FitRecord(
            fitId = E2Derivation.fitId(input),
            epoch = epoch,
            outcome = FitOutcome.Solved(PkCalibrationEngine.evaluate(input).result, input.originEpochMillis),
        )
    }

    private fun dose(at: LocalDateTime) = testMedicationLogEntry(
        uuid = UUID.nameUUIDFromBytes("extra:$at".toByteArray()),
        medicine = estradiol,
        doseInstruction = DoseInstruction.TabletFraction(1, 1),
        sourceGroupUuid = null,
        appliedAt = at.atZone(zoneId).toInstant(),
        appliedAtTimeZoneId = zoneId.id,
    )

    private class MutableClock(@Volatile var instant: Instant) : Clock() {
        override fun getZone(): ZoneId = ZoneId.systemDefault()
        override fun withZone(zone: ZoneId): Clock = this
        override fun instant(): Instant = instant
    }
}

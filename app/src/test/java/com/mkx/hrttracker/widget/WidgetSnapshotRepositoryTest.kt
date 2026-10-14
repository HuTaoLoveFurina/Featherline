package com.mkx.hrttracker.widget

import com.mkx.hrttracker.data.repository.HOME_SNAPSHOT_SCHEMA_VERSION
import com.mkx.hrttracker.data.repository.HomeCacheState
import com.mkx.hrttracker.data.repository.HomeSnapshotRecord
import com.mkx.hrttracker.data.repository.HomeSnapshotRepository
import io.mockk.coEvery
import io.mockk.confirmVerified
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertFalse
import org.junit.Test

class WidgetSnapshotRepositoryTest {
    private val homeSnapshotRepository = mockk<HomeSnapshotRepository>(relaxed = true)
    private val widgetSnapshotStore = mockk<WidgetSnapshotStore>()
    private val repository = WidgetSnapshotRepository(
        context = mockk(),
        homeSnapshotRepository = homeSnapshotRepository,
        e2EstimateRepository = mockk(),
        settingsRepository = mockk(),
        widgetSnapshotStore = widgetSnapshotStore,
        diagnosticsLogger = mockk(relaxed = true),
    )

    @Test
    fun refreshWidgetSnapshot_withNoCurrentState_asksForARun_andLeavesTheWidgetAlone() = runTest {
        // From a mutation until its run publishes: the widget keeps its last
        // snapshot, and the run's publish rewrites it.
        coEvery { homeSnapshotRepository.currentState() } returns null

        repository.refreshWidgetSnapshot()

        verify(exactly = 1) { homeSnapshotRepository.requestRebuild() }
        confirmVerified(widgetSnapshotStore)
    }

    @Test
    fun aStateWithCurrentHomeDataButNoE2Inputs_isNeverWritten_soTheWidgetKeepsItsEstimate() = runTest {
        // The first start after an engine bump: the load keeps Home data and
        // drops the inputs. Rows without their estimate would blank it.
        val homeDataOnly = HomeCacheState(
            generation = 0L,
            homeData = HomeSnapshotRecord(
                schemaVersion = HOME_SNAPSHOT_SCHEMA_VERSION,
                generatedAtEpochMillis = 0L,
                anchorDateEpochDay = 0L,
                zoneId = "UTC",
                activeGroups = emptyList(),
                scheduleEntries = emptyList(),
                antiandrogenHistoryEntries = emptyList(),
            ),
            e2Inputs = null,
        )
        coEvery { homeSnapshotRepository.currentState() } returns homeDataOnly

        assertFalse(repository.writeWidgetSnapshot(homeDataOnly))
        repository.refreshWidgetSnapshot()

        verify(exactly = 1) { homeSnapshotRepository.requestRebuild() }
        confirmVerified(widgetSnapshotStore)
    }
}

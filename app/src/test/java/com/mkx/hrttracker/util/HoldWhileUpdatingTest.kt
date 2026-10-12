package com.mkx.hrttracker.util

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class HoldWhileUpdatingTest {
    @Test
    fun holdsSettledValueUntilUpdateLandsAndMinHoldPasses() = runTest {
        val source = MutableSharedFlow<Pair<String, Boolean>>()
        val shown = mutableListOf<Triple<String, Boolean, Long>>()
        val job = launch {
            source.holdWhileUpdating(minHoldMillis = 800L) { (_, updating) -> updating }
                .collect { held -> shown += Triple(held.value.first, held.isUpdating, testScheduler.currentTime) }
        }
        runCurrent()

        source.emit("startup" to true) // nothing settled yet: passes through
        runCurrent()
        source.emit("old" to false)
        runCurrent()
        source.emit("old" to true) // hold starts at 0 ms
        runCurrent()
        source.emit("fallback" to true) // never shown
        runCurrent()
        advanceTimeBy(100L)
        source.emit("new" to false) // lands at 100 ms, released at 800 ms
        advanceUntilIdle()
        source.emit("next" to false) // no hold: immediate
        runCurrent()

        assertEquals(
            listOf(
                Triple("startup", true, 0L),
                Triple("old", false, 0L),
                Triple("old", true, 0L),
                Triple("old", true, 0L),
                Triple("new", false, 800L),
                Triple("next", false, 800L),
            ),
            shown,
        )
        job.cancel()
    }
}

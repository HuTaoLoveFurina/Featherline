package com.mkx.hrttracker.util

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
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
            source.holdWhileUpdating(MutableStateFlow(true), minHoldMillis = 800L, merge = { held, _ -> held }) { (_, updating) -> updating }
                .collect { held -> shown += Triple(held.value.first, held.isUpdating, testScheduler.currentTime) }
        }
        runCurrent()

        source.emit("startup" to true) // nothing settled yet: dropped, consumer keeps loading
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

    @Test
    fun seededHold_showsSeedWhileFirstUpdateLands() = runTest {
        val source = MutableSharedFlow<Pair<String, Boolean>>()
        val shown = mutableListOf<Triple<String, Boolean, Long>>()
        val job = launch {
            source.holdWhileUpdating(MutableStateFlow(true), minHoldMillis = 800L, settled = "seed" to false, merge = { held, _ -> held }) { (_, updating) -> updating }
                .collect { held -> shown += Triple(held.value.first, held.isUpdating, testScheduler.currentTime) }
        }
        runCurrent()

        source.emit("fallback" to true) // held on the seed, not passed through
        runCurrent()
        advanceTimeBy(100L)
        source.emit("new" to false)
        advanceUntilIdle()

        assertEquals(
            listOf(
                Triple("seed", true, 0L),
                Triple("new", false, 800L),
            ),
            shown,
        )
        job.cancel()
    }

    @Test
    fun merge_holdsOnlyItsPart_andTheRestShowsAtOnce() = runTest {
        val source = MutableSharedFlow<Screen>()
        val shown = mutableListOf<Pair<Held<Screen>, Long>>()
        val job = launch {
            source.holdWhileUpdating(
                MutableStateFlow(true),
                minHoldMillis = 400L,
                settled = Screen(e2 = "old", doses = "old", updating = false),
                merge = { held, latest -> latest.copy(e2 = held.e2) },
            ) { screen -> screen.updating }
                .collect { held -> shown += held to testScheduler.currentTime }
        }
        runCurrent()

        source.emit(Screen(e2 = "fallback", doses = "new", updating = true)) // the dose shows at once
        runCurrent()
        advanceTimeBy(100L)
        source.emit(Screen(e2 = "new", doses = "new", updating = false)) // e2 released at 400 ms
        advanceUntilIdle()

        assertEquals(
            listOf(
                Held(Screen(e2 = "old", doses = "new", updating = true), isUpdating = true) to 0L,
                Held(Screen(e2 = "new", doses = "new", updating = false), isUpdating = false) to 400L,
            ),
            shown,
        )
        job.cancel()
    }

    private data class Screen(val e2: String, val doses: String, val updating: Boolean)

    @Test
    fun hiddenScreen_minHoldStartsWhenShown_andHiddenLandingReleasesAtOnce() = runTest {
        val source = MutableSharedFlow<Pair<String, Boolean>>()
        val visible = MutableStateFlow(false)
        val shown = mutableListOf<Triple<String, Boolean, Long>>()
        val job = launch {
            source.holdWhileUpdating(visible, minHoldMillis = 400L, settled = "old" to false, merge = { held, _ -> held }) { (_, updating) -> updating }
                .collect { held -> shown += Triple(held.value.first, held.isUpdating, testScheduler.currentTime) }
        }
        runCurrent()

        source.emit("fallback" to true) // hold starts while hidden
        runCurrent()
        advanceTimeBy(100L)
        source.emit("new" to false) // lands while hidden: nobody saw the hold
        runCurrent()
        source.emit("fallback" to true) // a save from a pushed screen
        runCurrent()
        advanceTimeBy(200L)
        visible.value = true // popped back at 300 ms: the minimum starts here
        runCurrent()
        advanceTimeBy(100L)
        source.emit("next" to false) // lands at 400 ms, released at 700 ms
        advanceUntilIdle()

        assertEquals(
            listOf(
                Triple("old", true, 0L),
                Triple("new", false, 100L),
                Triple("new", true, 100L),
                Triple("next", false, 700L),
            ),
            shown,
        )
        job.cancel()
    }
}

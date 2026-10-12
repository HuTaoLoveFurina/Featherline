package com.mkx.hrttracker.util

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

/** A held [value] and whether its screen shows the updating indicator. */
data class Held<T>(val value: T, val isUpdating: Boolean)

/** Long enough for the indicator to slide fully in before it leaves. */
const val UPDATING_MIN_HOLD_MILLIS = 400L

/**
 * Keeps the last settled value on screen while [isUpdating] says a rebuild is
 * landing, then releases the newest value once it settles, no sooner than
 * [minHoldMillis] after the screen was [visible] with the hold. The
 * intermediate values (a cleared snapshot's fallback, a section that has
 * re-solved while the list has not) never show, and the content changes
 * together with the indicator leaving.
 *
 * The consumers collect while their screen is covered, and a save from a
 * pushed screen starts the hold before the pop back. Timed from the start of
 * the hold, it would be spent by the time the screen shows the indicator. A
 * value that settles while the screen is hidden is released at once: the
 * screen comes back to it without an indicator.
 *
 * [settled] is what the consumer already shows, so a rebuild in flight when
 * collection starts holds that instead of passing intermediate values
 * through. Without one there is nothing to hold, so updating values are
 * dropped and the consumer keeps its loading state until the first settled
 * value.
 */
fun <T : Any> Flow<T>.holdWhileUpdating(
    visible: StateFlow<Boolean>,
    minHoldMillis: Long = UPDATING_MIN_HOLD_MILLIS,
    settled: T? = null,
    isUpdating: (T) -> Boolean,
): Flow<Held<T>> = channelFlow {
    var settled: T? = settled
    var minHold: Job? = null
    collectLatest { value ->
        val shown = settled
        if (isUpdating(value)) {
            if (shown != null) {
                if (minHold == null) minHold = launch {
                    visible.first { it }
                    delay(minHoldMillis)
                }
                send(Held(shown, isUpdating = true))
            }
        } else {
            if (!visible.value) minHold?.cancel()
            // A newer value cancels this wait and waits out the same hold.
            minHold?.join()
            minHold = null
            settled = value
            send(Held(value, isUpdating = false))
        }
    }
}

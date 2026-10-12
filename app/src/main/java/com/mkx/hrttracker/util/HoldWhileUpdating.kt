package com.mkx.hrttracker.util

import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch

/** A held [value] and whether its screen shows the updating indicator. */
data class Held<T>(val value: T, val isUpdating: Boolean)

/** Long enough for the indicator to slide fully in before it leaves. */
const val UPDATING_MIN_HOLD_MILLIS = 800L

/**
 * Keeps the last settled value on screen while [isUpdating] says a rebuild is
 * landing, then releases the newest value once it settles, no sooner than
 * [minHoldMillis] after the hold began. The intermediate values (a cleared
 * snapshot's fallback, a section that has re-solved while the list has not)
 * never show, and the content changes together with the indicator leaving.
 *
 * Before anything has settled there is nothing to hold, so values pass
 * through, still flagged as updating.
 */
fun <T : Any> Flow<T>.holdWhileUpdating(
    minHoldMillis: Long = UPDATING_MIN_HOLD_MILLIS,
    isUpdating: (T) -> Boolean,
): Flow<Held<T>> = channelFlow {
    var settled: T? = null
    var minHold: Job? = null
    collectLatest { value ->
        val shown = settled
        if (isUpdating(value)) {
            if (shown == null) {
                send(Held(value, isUpdating = true))
            } else {
                if (minHold == null) minHold = launch { delay(minHoldMillis) }
                send(Held(shown, isUpdating = true))
            }
        } else {
            // A newer value cancels this wait and waits out the same hold.
            minHold?.join()
            minHold = null
            settled = value
            send(Held(value, isUpdating = false))
        }
    }
}

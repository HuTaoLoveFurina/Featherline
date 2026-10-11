package com.mkx.hrttracker.ui.main

import com.mkx.hrttracker.data.repository.PkCalibrationLiveResult
import com.mkx.hrttracker.model.pk.PkCalibrationGlobalState
import com.mkx.hrttracker.model.pk.PkCalibrationResult
import com.mkx.hrttracker.ui.pkcalibrationdebug.PkCalibrationUiFixture
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PkCalibrationSheetUiStateTest {
    @Test
    fun loading_isNull_failedEvaluation_isTheNumericFailureSheet_fixtureWins() {
        assertNull(pkCalibrationSheetUiState(live = null, fixture = null))

        val failed = requireNotNull(pkCalibrationSheetUiState(PkCalibrationLiveResult(null), fixture = null))
        assertTrue(failed.numericFailure)
        assertTrue(failed.routeRows.isEmpty())

        val fixture = PkCalibrationUiFixture(
            result = PkCalibrationResult(PkCalibrationGlobalState.NO_USABLE_LABS),
            render = null,
            excludedResultIds = emptySet(),
        )
        assertEquals(
            PkCalibrationGlobalState.NO_USABLE_LABS,
            requireNotNull(pkCalibrationSheetUiState(PkCalibrationLiveResult(null), fixture)).globalState,
        )
    }
}

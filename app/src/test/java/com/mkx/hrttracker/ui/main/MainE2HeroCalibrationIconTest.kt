package com.mkx.hrttracker.ui.main

import android.content.Context
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.performClick
import androidx.test.core.app.ApplicationProvider
import com.mkx.hrttracker.R
import com.mkx.hrttracker.model.bloodtest.BloodUnitKey
import java.time.LocalDateTime
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.GraphicsMode

/** The hero's calibration icon, in every state, opens the estimate sheet through its callback. */
@RunWith(RobolectricTestRunner::class)
@GraphicsMode(GraphicsMode.Mode.NATIVE)
@Config(sdk = [34])
class MainE2HeroCalibrationIconTest {
    @get:Rule
    val composeRule = createComposeRule()

    private val context: Context get() = ApplicationProvider.getApplicationContext()

    @Test
    fun calibratedIcon_invokesTheCallback() {
        assertEquals(1, clicksOn(adjusted = true, descriptionRes = R.string.calibration_pk_hero_adjusted))
    }

    @Test
    fun populationIcon_invokesTheCallback() {
        assertEquals(1, clicksOn(adjusted = false, descriptionRes = R.string.calibration_pk_hero_population))
    }

    @Test
    fun iconBeforeTheFirstEvaluation_isThePopulationIcon() {
        assertEquals(1, clicksOn(adjusted = null, descriptionRes = R.string.calibration_pk_hero_population))
    }

    private fun clicksOn(adjusted: Boolean?, descriptionRes: Int): Int {
        var clicks = 0
        composeRule.setContent {
            MainE2HeroCard(
                section = MainE2HeroUiState(currentValue = 120.0),
                now = LocalDateTime.of(2026, 10, 11, 12, 0),
                displayUnit = BloodUnitKey.PG_ML,
                pkCalibration = adjusted?.let {
                    MainPkCalibrationUiState(
                        adjusted = it,
                        limitedConfidence = false,
                        renderUnavailable = false,
                        bandUnavailable = false,
                        band = null,
                    )
                },
                onCalibrationClick = { clicks += 1 },
            )
        }
        composeRule.onNodeWithContentDescription(context.getString(descriptionRes)).performClick()
        return clicks
    }
}

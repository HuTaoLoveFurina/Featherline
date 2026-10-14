package com.mkx.hrttracker.widget

import android.content.Context
import android.content.res.Configuration
import com.mkx.hrttracker.data.repository.E2EstimateRepository
import com.mkx.hrttracker.data.repository.HomeCacheState
import com.mkx.hrttracker.data.repository.HomeSnapshotRepository
import com.mkx.hrttracker.data.repository.SettingsRepository
import com.mkx.hrttracker.model.settings.SettingsState
import com.mkx.hrttracker.util.AppDiagnosticsLogger
import com.mkx.hrttracker.util.localizedShortTimeFormatter
import com.mkx.hrttracker.util.uses24HourTimeFormat
import dagger.hilt.android.qualifiers.ApplicationContext
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Locale
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class WidgetSnapshotRepository @Inject constructor(
    @param:ApplicationContext private val context: Context,
    private val homeSnapshotRepository: HomeSnapshotRepository,
    private val e2EstimateRepository: E2EstimateRepository,
    private val settingsRepository: SettingsRepository,
    private val widgetSnapshotStore: WidgetSnapshotStore,
    private val diagnosticsLogger: AppDiagnosticsLogger = AppDiagnosticsLogger(),
) {
    suspend fun refreshWidgetSnapshot(now: LocalDateTime = LocalDateTime.now()) {
        // Re-derives the widget snapshot from the current state — used when widget-only
        // inputs (e.g. theme, scale, alpha, E2 unit) change and Home data itself doesn't
        // need to be rebuilt. Callers that actually need fresh Home data go through
        // HomeSnapshotRepository, whose publish the cache observer fans out to the widget.
        val written = homeSnapshotRepository.currentState()
            ?.let { state -> writeWidgetSnapshot(state, now) } == true
        if (!written) {
            // Never cleared for it: the widget keeps its last snapshot until a run
            // publishes both parts, which the cache observer writes.
            diagnosticsLogger.info(
                TAG,
                "widget_snapshot_refresh_skipped reason=no_current_state now=$now"
            )
            homeSnapshotRepository.requestRebuild()
        }
    }

    /**
     * Writes the widget from [state] when it has both parts: its rows from the
     * Home data, its estimate from the E2 inputs published with it. Returns
     * false, and leaves the widget as it is, when either part is missing.
     */
    suspend fun writeWidgetSnapshot(
        state: HomeCacheState,
        now: LocalDateTime = LocalDateTime.now(),
    ): Boolean {
        val homeData = state.homeData ?: return false
        val e2Inputs = state.e2Inputs ?: return false
        val zoneId = ZoneId.systemDefault()
        val settings = settingsRepository.getCurrentSettings()
        // Wrap with an explicitly-localized context so context.getString() inside the
        // builder resolves against the user's chosen app language even on API levels
        // where the application context lags AppCompatDelegate.setApplicationLocales.
        val localizedContext = context.withAppLocale(settings)
        val timeFormatter = localizedShortTimeFormatter(
            locale = Locale.forLanguageTag(settings.appLanguageOption.languageTag),
            uses24HourFormat = context.uses24HourTimeFormat(),
        )
        val widgetSnapshot = buildWidgetSnapshotRecord(
            context = localizedContext,
            homeSnapshot = homeData,
            widgetEstimate = e2EstimateRepository.widgetEstimate(e2Inputs, now),
            settings = settings,
            now = now,
            zoneId = zoneId,
            timeFormatter = timeFormatter,
        )
        widgetSnapshotStore.writeSnapshot(widgetSnapshot)
        pushHrtWidgets(context, widgetSnapshot)
        diagnosticsLogger.info(
            TAG,
            "widget_snapshot_refreshed rows=${widgetSnapshot.doseRows.size} " +
                    "done=${widgetSnapshot.doneCount} total=${widgetSnapshot.totalCount}",
        )
        return true
    }

    private fun Context.withAppLocale(settings: SettingsState): Context {
        val locale = Locale.forLanguageTag(settings.appLanguageOption.languageTag)
        val config = Configuration(resources.configuration).apply { setLocale(locale) }
        return createConfigurationContext(config)
    }

    companion object {
        private const val TAG = "WidgetSnapshotRepository"
    }
}

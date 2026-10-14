package com.mkx.hrttracker.widget

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import dagger.hilt.android.EntryPointAccessors
import java.time.LocalDateTime
import java.time.ZoneId

class WidgetDailyRefreshWorker(
    private val appContext: Context,
    workerParams: WorkerParameters,
) : CoroutineWorker(appContext, workerParams) {

    override suspend fun doWork(): Result {
        val entryPoint =
            EntryPointAccessors.fromApplication(appContext, WidgetEntryPoint::class.java)
        val widgetSnapshotStore = entryPoint.widgetSnapshotStore()
        val homeSnapshotRepository = entryPoint.homeSnapshotRepository()

        val now = LocalDateTime.now()
        val zoneId = ZoneId.systemDefault()
        val snapshot = widgetSnapshotStore.readSnapshot()

        val projectionExpired = snapshot == null ||
                snapshot.pkProjection?.toPkProjectionResult(now, zoneId) == null

        val anchorDateStale = snapshot?.isAnchoredBefore(now) == true
        val doseStatusStale = snapshot?.doseRows?.any { row ->
            when (row.status) {
                WidgetDoseStatus.UPCOMING ->
                    !now.isBefore(row.scheduledAt.minusHours(1))

                WidgetDoseStatus.DUE_SOON ->
                    !now.isBefore(row.scheduledAt.plusHours(1))

                else -> false
            }
        } == true

        if (projectionExpired || anchorDateStale || doseStatusStale) {
            // HomeWidgetManager's cache observer rebuilds the widget from the run's publish.
            // Waited for, so the job, and the process with it, outlive the run.
            homeSnapshotRepository.rebuildNow()
        } else {
            updateAllHrtWidgets(appContext)
        }
        // A slot can pass without a reminder (reminders off, or Doze delayed
        // the alarm): store Home's estimate without it. Only a key when the
        // stored estimate already matches.
        entryPoint.e2EstimateRepository().warmHome()
        return Result.success()
    }
}

internal fun WidgetSnapshotRecord.isAnchoredBefore(now: LocalDateTime): Boolean =
    anchorDateEpochDay < now.toLocalDate().toEpochDay()

package com.mkx.hrttracker.reminder

import android.content.BroadcastReceiver
import com.mkx.hrttracker.data.repository.E2EstimateRepository
import com.mkx.hrttracker.data.repository.MedicationGroupRepository
import com.mkx.hrttracker.data.repository.MedicationLogRepository
import com.mkx.hrttracker.data.repository.SettingsRepository
import com.mkx.hrttracker.model.medication.DoseInstruction
import com.mkx.hrttracker.model.medication.MedicationApplicationType
import com.mkx.hrttracker.model.medication.MedicationGroup
import com.mkx.hrttracker.model.medication.MedicationGroupSchedule
import com.mkx.hrttracker.model.medication.MedicationGroupScheduleType
import com.mkx.hrttracker.model.medication.MedicationKey
import com.mkx.hrttracker.model.medication.testMedicationGroupMedication
import com.mkx.hrttracker.model.medication.testMedicine
import com.mkx.hrttracker.model.settings.SettingsState
import com.mkx.hrttracker.util.AppDiagnosticsLogger
import io.mockk.coEvery
import io.mockk.coVerifyOrder
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

@OptIn(ExperimentalCoroutinesApi::class)
class MedicationReminderReceiverTest {
    private val settingsRepository: SettingsRepository = mockk()
    private val medicationGroupRepository: MedicationGroupRepository = mockk()
    private val medicationLogRepository: MedicationLogRepository = mockk()
    private val medicationReminderScheduler: MedicationReminderScheduler = mockk(relaxed = true)
    private val reminderNotificationManager: ReminderNotificationManager = mockk(relaxed = true)
    private val e2EstimateRepository: E2EstimateRepository = mockk(relaxed = true)
    private val diagnosticsLogger: AppDiagnosticsLogger = mockk(relaxed = true)
    private val pendingResult: BroadcastReceiver.PendingResult = mockk(relaxed = true)

    @Test
    fun aDoseReminder_warmsHomesEstimate_afterPostingTheNotification() = runTest {
        // At the slot time the slot is no longer planned: Home's estimate
        // moved, and the next open should find the new one stored.
        val scheduledAt = LocalDateTime.of(2026, 4, 20, 9, 0)
        val group = MedicationGroup(
            uuid = UUID.fromString("ea12a7fa-9077-442f-b322-988a48195515"),
            name = "Estradiol",
            schedule = MedicationGroupSchedule(
                type = MedicationGroupScheduleType.DAILY,
                interval = 1,
                since = LocalDate.of(2026, 4, 1),
                weeklyDaysOfWeek = emptySet(),
                times = listOf(LocalTime.of(9, 0)),
            ),
            medications = listOf(
                testMedicationGroupMedication(
                    medicine = testMedicine(key = MedicationKey.ESTRADIOL),
                    applicationType = MedicationApplicationType.ORAL,
                    doseInstruction = DoseInstruction.TabletFraction(1, 1),
                )
            ),
            notificationsEnabled = true,
            createdAt = Instant.parse("2026-04-01T00:00:00Z"),
            updatedAt = Instant.parse("2026-04-01T00:00:00Z"),
        )
        coEvery { settingsRepository.getCurrentSettings() } returns SettingsState()
        coEvery { medicationGroupRepository.getGroups() } returns listOf(group)
        coEvery { medicationLogRepository.getScheduledGroupEntriesSince(scheduledAt) } returns emptyList()

        handleDoseReminder(
            groupUuid = group.uuid,
            scheduledAt = scheduledAt,
            deadline = TimeSource.Monotonic.markNow() + 8.seconds,
            appScope = backgroundScope,
            settingsRepository = settingsRepository,
            medicationGroupRepository = medicationGroupRepository,
            medicationLogRepository = medicationLogRepository,
            medicationReminderScheduler = medicationReminderScheduler,
            reminderNotificationManager = reminderNotificationManager,
            e2EstimateRepository = e2EstimateRepository,
            diagnosticsLogger = diagnosticsLogger,
            pendingResult = pendingResult,
        )
        runCurrent()

        coVerifyOrder {
            reminderNotificationManager.showDoseReminderNotification(any(), any(), any())
            e2EstimateRepository.warmHome()
            pendingResult.finish()
        }
    }
}

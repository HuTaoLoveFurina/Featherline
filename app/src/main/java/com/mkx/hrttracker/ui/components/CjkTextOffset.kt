package com.mkx.hrttracker.ui.components

import androidx.compose.runtime.compositionLocalOf
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import java.util.Locale

val LocalCjkTextOffsetEnabled = compositionLocalOf { false }

private val cjkTextOffsetScripts = setOf(
    Character.UnicodeScript.HAN,
    Character.UnicodeScript.HIRAGANA,
    Character.UnicodeScript.KATAKANA,
    Character.UnicodeScript.HANGUL,
)

internal fun CharSequence.containsCjkCharacters(): Boolean {
    var index = 0
    while (index < length) {
        val codePoint = Character.codePointAt(this, index)
        if (Character.UnicodeScript.of(codePoint) in cjkTextOffsetScripts) {
            return true
        }
        index += Character.charCount(codePoint)
    }
    return false
}

// ponytail: no-op. AppTypography (ui/theme/Type.kt) keeps the baseline still on devices whose CJK
// fallback font is taller than the Latin font, which is what this -1 dp nudge used to paper over.
// The call sites, the Settings toggle and LocalCjkTextOffsetEnabled stay until the on-device
// check of the typography fix; delete all of them together afterwards.
@Suppress("UNUSED_PARAMETER")
internal fun Modifier.cjkTextOffset(
    text: CharSequence,
    enabled: Boolean = true,
    amount: Dp = (-1).dp
): Modifier = this

@Suppress("UNUSED_PARAMETER")
internal fun Modifier.cjkTextOffset(
    locale: Locale,
    enabled: Boolean = true,
    amount: Dp = (-1).dp
): Modifier = this

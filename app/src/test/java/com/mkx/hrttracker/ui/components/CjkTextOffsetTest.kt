package com.mkx.hrttracker.ui.components

import androidx.compose.ui.Modifier
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.Locale

class CjkTextOffsetTest {
    // The nudge is a no-op since the typography fix keeps the baseline still; the modifier only
    // survives until the call sites are deleted.
    @Test
    fun cjkTextOffset_isNoOpForCantoneseLocale() {
        assertSame(Modifier, Modifier.cjkTextOffset(Locale.forLanguageTag("yue-Hant-HK")))
    }

    @Test
    fun cjkTextOffset_isNoOpForChineseText() {
        assertSame(Modifier, Modifier.cjkTextOffset("剂量"))
    }

    @Test
    fun cjkTextOffset_skipsNonChineseLocale() {
        assertSame(Modifier, Modifier.cjkTextOffset(Locale.ENGLISH))
    }

    @Test
    fun containsCjkCharacters_returnsFalse_forLatinText() {
        assertFalse("Dose".containsCjkCharacters())
    }

    @Test
    fun containsCjkCharacters_returnsTrue_forHanText() {
        assertTrue("剂量".containsCjkCharacters())
    }

    @Test
    fun containsCjkCharacters_returnsTrue_forMixedText() {
        assertTrue("Estradiol 雌二醇".containsCjkCharacters())
    }

    @Test
    fun containsCjkCharacters_returnsTrue_forJapaneseKana() {
        assertTrue("カレンダー".containsCjkCharacters())
    }

    @Test
    fun containsCjkCharacters_returnsTrue_forHangul() {
        assertTrue("복용 기록".containsCjkCharacters())
    }
}

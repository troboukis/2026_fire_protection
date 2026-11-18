import type { AppLanguage } from './languages'

type SourceText = {
  originalGreek: string
  englishTranslation?: string | null
}

// A translation must be explicitly supplied by a caller. Never translate,
// transliterate, trim or overwrite original evidence as a side effect of locale.
export function resolveSourceText(source: SourceText, language: AppLanguage) {
  if (language === 'en' && source.englishTranslation?.trim()) {
    return { text: source.englishTranslation, language: 'en' as const, isTranslation: true }
  }
  return { text: source.originalGreek, language: 'el' as const, isTranslation: false }
}

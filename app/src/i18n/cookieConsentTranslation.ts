import type { Translation } from 'vanilla-cookieconsent'
import { getOverviewUi } from './overview'
import type { AppLanguage } from './languages'
import { documentHref, localizeHref } from '../routing/localizedHref'

export function cookieConsentTranslation(language: AppLanguage): Translation {
  const ui = getOverviewUi(language)
  const privacyHref = documentHref(localizeHref('/privacy', language), import.meta.env.BASE_URL)
  return {
    consentModal: {
      title: ui("Επιλογές απορρήτου"),
      description: ui("Χρησιμοποιούμε προαιρετικά cookies της υπηρεσίας Google Analytics, ώστε να κατανοούμε καλύτερα πώς χρησιμοποιείται το FireWatch. Επιλέγοντας «Αποδοχή όλων», ενεργοποιείτε την επιλογή «Στατιστικά». Επιλέγοντας «Απόρριψη όλων», η επιλογή «Στατιστικά» παραμένει απενεργοποιημένη."),
      acceptAllBtn: ui("Αποδοχη ολων"),
      acceptNecessaryBtn: ui("Απορριψη ολων"),
      showPreferencesBtn: ui("Ρυθμισεις"),
      footer: `<a href="${privacyHref}">${ui("Πολιτική απορρήτου και cookies")}</a>`,
    },
    preferencesModal: {
      title: ui("Ρυθμίσεις απορρήτου"),
      acceptAllBtn: ui("Αποδοχη ολων"),
      acceptNecessaryBtn: ui("Απορριψη ολων"),
      savePreferencesBtn: ui("Αποθηκευση επιλογων"),
      closeIconLabel: ui("Κλείσιμο"),
      serviceCounterLabel: ui("Υπηρεσία|Υπηρεσίες"),
      sections: [
        {
          title: ui("Οι επιλογές σας"),
          description: ui("Μπορείτε να αλλάξετε την επιλογή σας οποιαδήποτε στιγμή από τον σύνδεσμο «Ρυθμίσεις cookies» στο κάτω μέρος της εφαρμογής."),
        },
        {
          title: ui("Απαραίτητα"),
          description: ui("Αποθηκεύουν αποκλειστικά την επιλογή σας για τα cookies, ώστε να μη σας ζητείται σε κάθε επίσκεψη. Δεν μπορούν να απενεργοποιηθούν."),
          linkedCategory: 'necessary',
          cookieTable: {
            caption: ui("Απαραίτητα cookies"),
            headers: {
              name: ui("Όνομα"),
              desc: ui("Σκοπός"),
              expiration: ui("Διάρκεια"),
            },
            body: [
              {
                name: 'firewatch_consent',
                desc: ui("Αποθήκευση των επιλογών συγκατάθεσης."),
                expiration: ui("182 ημέρες"),
              },
            ],
          },
        },
        {
          title: ui("Στατιστικά"),
          description: ui("Το Google Analytics μάς βοηθά να κατανοούμε συγκεντρωτικά την επισκεψιμότητα και τη χρήση της εφαρμογής. Παραμένει απενεργοποιημένο μέχρι να το επιλέξετε."),
          linkedCategory: 'analytics',
          cookieTable: {
            caption: ui("Cookies στατιστικών"),
            headers: {
              name: ui("Όνομα"),
              desc: ui("Σκοπός"),
              expiration: ui("Διάρκεια"),
            },
            body: [
              {
                name: '_ga',
                desc: ui("Διάκριση επισκεπτών για συγκεντρωτικά στατιστικά."),
                expiration: ui("Έως 2 έτη"),
              },
              {
                name: '_ga_*',
                desc: ui("Διατήρηση της κατάστασης μιας επίσκεψης."),
                expiration: ui("Έως 2 έτη"),
              },
            ],
          },
        },
        {
          title: ui("Περισσότερες πληροφορίες"),
          description: ui("Δείτε την {{v0}} ή επικοινωνήστε στο troboukis[at]gmail[dot]com.", { v0: `<a href="${privacyHref}">${ui("Πολιτική απορρήτου και cookies")}</a>` }),
        },
      ],
    },
  }
}

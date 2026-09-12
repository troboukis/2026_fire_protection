import unittest
from classify_decisions import classify_document


class ClassificationTests(unittest.TestCase):
    def category(self, title, text=''):
        return classify_document(title, text)['category_id']

    def test_action_over_project_name(self):
        self.assertEqual(self.category('Έγκριση παράτασης της μελέτης «Σχέδιο προστασίας»'), 'changes')
        self.assertEqual(self.category('Έγκριση 2ου λογαριασμού για τη μελέτη του έργου «Προστασία»'), 'payments')
        self.assertEqual(self.category('Έγκριση 1ου Α.Π.Ε. και συμπληρωματικής σύμβασης'), 'changes')
        self.assertEqual(self.category('Συγκρότηση Επιτροπής Παραλαβής του έργου «Δάση»'), 'studies')
        self.assertEqual(self.category('Έγκριση πρωτοκόλλου τμηματικής παραλαβής'), 'acceptance')

    def test_text_fallback_ignores_recitals_and_quoted_project(self):
        result = classify_document('Απόφαση', 'Έχοντας υπόψη την παράταση και τον λογαριασμό. ΑΠΟΦΑΣΙΖΟΥΜΕ Την έγκριση τελικής επιμέτρησης του έργου «Μελέτη δάσους».')
        self.assertEqual(result['category_id'], 'acceptance')
        self.assertEqual(result['basis'], 'operative_text')
        self.assertIsNotNone(result['text_sha256'])
        self.assertEqual(self.category('Απόφαση', 'Έχοντας υπόψη την παράταση και τον λογαριασμό.'), 'other')
        self.assertEqual(self.category('Απόφαση για το έργο «Έγκριση μελέτης»'), 'other')

    def test_greek_normalization_and_updated_schedule(self):
        self.assertEqual(self.category('ΕΓΚΡΙΣΗ_ΧΡΟΝΟΔΙΑΓΡΑΜΜΑΤΟΣ'), 'studies')
        self.assertEqual(self.category('Έγκριση τροποποιημένου χρονοδιαγράμματος'), 'changes')
        self.assertEqual(self.category('«Έγκριση Τελικής Επιμέτρησης»'), 'acceptance')
        self.assertEqual(self.category('Βεβαίωση περαίωσης έργου'), 'acceptance')

    def test_unknown_and_ambiguous_abstain(self):
        self.assertEqual(self.category('Απόφαση μείωσης εγγυήσεων λόγω έγκρισης επιμέτρησης'), 'other')
        self.assertEqual(self.category('Έγκριση μελέτης και αποζημίωσης'), 'other')
        self.assertEqual(self.category('Ενημερωτικό σημείωμα'), 'other')
        self.assertEqual(self.category(''), 'other')


if __name__ == '__main__':
    unittest.main()

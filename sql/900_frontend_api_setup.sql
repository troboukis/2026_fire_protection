-- =============================================================
-- Frontend API Setup (single-run bundle)
-- Purpose:
--   1) Create/refresh SQL functions used by frontend RPC calls.
--   2) Grant execute/select privileges for anon/authenticated/service_role.
--   3) Force PostgREST schema reload.
--   4) Provide quick verification queries.
--
-- Run this whole file in Supabase SQL Editor.
-- =============================================================

-- BEGIN municipality English names (067)
-- Reviewed municipality place names from the existing EL/EN municipality catalogue.
-- Match the canonical Greek name, not historical/abbreviated municipality_value.
-- Keep Greek source fields, identifiers, and any later manual English corrections.
ALTER TABLE public.municipality_normalized_name
  ADD COLUMN IF NOT EXISTS municipality_name_en TEXT;

COMMENT ON COLUMN public.municipality_normalized_name.municipality_name_en IS
  'Reviewed English place name corresponding to municipality_normalized_value; excludes the Municipality of prefix. Greek source names and municipality_key remain unchanged.';

UPDATE public.municipality_normalized_name AS municipality
SET municipality_name_en = names.municipality_name_en
FROM (VALUES
  ('9001', 'ΔΟΞΑΤΟΥ', 'Doxato'),
  ('9002', 'ΔΡΑΜΑΣ', 'Drama'),
  ('9003', 'ΚΑΤΩ ΝΕΥΡΟΚΟΠΙΟΥ', 'Kato Nevrokopi'),
  ('9004', 'ΠΑΡΑΝΕΣΤΙΟΥ', 'Paranesti'),
  ('9005', 'ΠΡΟΣΟΤΣΑΝΗΣ', 'Prosotsani'),
  ('9006', 'ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ', 'Alexandroupolis'),
  ('9007', 'ΔΙΔΥΜΟΤΕΙΧΟΥ', 'Didymoteicho'),
  ('9008', 'ΟΡΕΣΤΙΑΔΑΣ', 'Orestiada'),
  ('9009', 'ΣΑΜΟΘΡΑΚΗΣ', 'Samothrace'),
  ('9010', 'ΣΟΥΦΛΙΟΥ', 'Soufli'),
  ('9011', 'ΘΑΣΟΥ', 'Thasos'),
  ('9012', 'ΚΑΒΑΛΑΣ', 'Kavala'),
  ('9013', 'ΝΕΣΤΟΥ', 'Nestos'),
  ('9014', 'ΠΑΓΓΑΙΟΥ', 'Pangaio'),
  ('9015', 'ΑΒΔΗΡΩΝ', 'Avdira'),
  ('9016', 'ΜΥΚΗΣ', 'Myki'),
  ('9017', 'ΞΑΝΘΗΣ', 'Xanthi'),
  ('9018', 'ΤΟΠΕΙΡΟΥ', 'Topeiros'),
  ('9019', 'ΑΡΡΙΑΝΩΝ', 'Arriana'),
  ('9020', 'ΙΑΣΜΟΥ', 'Iasmos'),
  ('9021', 'ΚΟΜΟΤΗΝΗΣ', 'Komotini'),
  ('9022', 'ΜΑΡΩΝΕΙΑΣ - ΣΑΠΩΝ', 'Maroneia – Sapes'),
  ('9023', 'ΑΛΕΞΑΝΔΡΕΙΑΣ', 'Alexandria'),
  ('9024', 'ΒΕΡΟΙΑΣ', 'Veroia'),
  ('9025', 'ΝΑΟΥΣΑΣ', 'Naoussa'),
  ('9026', 'ΑΜΠΕΛΟΚΗΠΩΝ - ΜΕΝΕΜΕΝΗΣ', 'Ampelokipoi – Menemeni'),
  ('9027', 'ΒΟΛΒΗΣ', 'Volvi'),
  ('9028', 'ΔΕΛΤΑ', 'Delta'),
  ('9029', 'ΘΕΡΜΑΙΚΟΥ', 'Thermaikos'),
  ('9030', 'ΘΕΡΜΗΣ', 'Thermi'),
  ('9031', 'ΘΕΣΣΑΛΟΝΙΚΗΣ', 'Thessaloniki'),
  ('9032', 'ΚΑΛΑΜΑΡΙΑΣ', 'Kalamaria'),
  ('9033', 'ΚΟΡΔΕΛΙΟΥ - ΕΥΟΣΜΟΥ', 'Kordelio – Evosmos'),
  ('9034', 'ΛΑΓΚΑΔΑ', 'Lagkadas'),
  ('9035', 'ΝΕΑΠΟΛΗΣ - ΣΥΚΕΩΝ', 'Neapoli – Sykies'),
  ('9036', 'ΠΑΥΛΟΥ ΜΕΛΑ', 'Pavlos Melas'),
  ('9037', 'ΠΥΛΑΙΑΣ - ΧΟΡΤΙΑΤΗ', 'Pylaia – Chortiatis'),
  ('9038', 'ΧΑΛΚΗΔΟΝΟΣ', 'Chalkidona'),
  ('9039', 'ΩΡΑΙΟΚΑΣΤΡΟΥ', 'Oraiokastro'),
  ('9040', 'ΚΙΛΚΙΣ', 'Kilkis'),
  ('9041', 'ΠΑΙΟΝΙΑΣ', 'Paionia'),
  ('9042', 'ΑΛΜΩΠΙΑΣ', 'Almopia'),
  ('9043', 'ΕΔΕΣΣΑΣ', 'Edessa'),
  ('9044', 'ΠΕΛΛΑΣ', 'Pella'),
  ('9045', 'ΣΚΥΔΡΑΣ', 'Skydra'),
  ('9046', 'ΔΙΟΥ - ΟΛΥΜΠΟΥ', 'Dion – Olympus'),
  ('9047', 'ΚΑΤΕΡΙΝΗΣ', 'Katerini'),
  ('9048', 'ΠΥΔΝΑΣ - ΚΟΛΙΝΔΡΟΥ', 'Pydna – Kolindros'),
  ('9049', 'ΑΜΦΙΠΟΛΗΣ', 'Amphipolis'),
  ('9050', 'ΒΙΣΑΛΤΙΑΣ', 'Visaltia'),
  ('9051', 'ΕΜΜΑΝΟΥΗΛ ΠΑΠΠΑ', 'Emmanouil Pappas'),
  ('9052', 'ΗΡΑΚΛΕΙΑΣ', 'Irakleia'),
  ('9053', 'ΝΕΑΣ ΖΙΧΝΗΣ', 'Nea Zichni'),
  ('9054', 'ΣΕΡΡΩΝ', 'Serres'),
  ('9055', 'ΣΙΝΤΙΚΗΣ', 'Sintiki'),
  ('9056', 'ΑΡΙΣΤΟΤΕΛΗ', 'Aristotelis'),
  ('9057', 'ΚΑΣΣΑΝΔΡΑΣ', 'Kassandra'),
  ('9058', 'ΝΕΑΣ ΠΡΟΠΟΝΤΙΔΑΣ', 'Nea Propontida'),
  ('9059', 'ΠΟΛΥΓΥΡΟΥ', 'Polygyros'),
  ('9060', 'ΣΙΘΩΝΙΑΣ', 'Sithonia'),
  ('9061', 'ΓΡΕΒΕΝΩΝ', 'Grevena'),
  ('9062', 'ΔΕΣΚΑΤΗΣ', 'Deskati'),
  ('9063', 'ΚΑΣΤΟΡΙΑΣ', 'Kastoria'),
  ('9064', 'ΝΕΣΤΟΡΙΟΥ', 'Nestorio'),
  ('9065', 'ΑΡΓΟΥΣ - ΟΡΕΣΤΙΚΟΥ', 'Argos Orestiko'),
  ('9066', 'ΒΟΙΟΥ', 'Voio'),
  ('9067', 'ΕΟΡΔΑΙΑΣ', 'Eordaia'),
  ('9068', 'ΚΟΖΑΝΗΣ', 'Kozani'),
  ('9069', 'ΣΕΡΒΙΩΝ - ΒΕΛΒΕΝΤΟΥ', 'Servia – Velventos'),
  ('9070', 'ΑΜΥΝΤΑΙΟΥ', 'Amyntaio'),
  ('9071', 'ΠΡΕΣΠΩΝ', 'Prespa'),
  ('9072', 'ΦΛΩΡΙΝΑΣ', 'Florina'),
  ('9073', 'ΑΡΤΑΙΩΝ', 'Arta'),
  ('9074', 'ΓΕΩΡΓΙΟΥ ΚΑΡΑΙΣΚΑΚΗ', 'Georgios Karaiskakis'),
  ('9075', 'ΚΕΝΤΡΙΚΩΝ ΤΖΟΥΜΕΡΚΩΝ', 'Central Tzoumerka'),
  ('9076', 'ΝΙΚΟΛΑΟΥ ΣΚΟΥΦΑ', 'Nikolaos Skoufas'),
  ('9077', 'ΗΓΟΥΜΕΝΙΤΣΑΣ', 'Igoumenitsa'),
  ('9078', 'ΣΟΥΛΙΟΥ', 'Souli'),
  ('9079', 'ΦΙΛΙΑΤΩΝ', 'Filiates'),
  ('9080', 'ΒΟΡΕΙΩΝ ΤΖΟΥΜΕΡΚΩΝ', 'Northern Tzoumerka'),
  ('9081', 'ΔΩΔΩΝΗΣ', 'Dodoni'),
  ('9082', 'ΖΑΓΟΡΙΟΥ', 'Zagori'),
  ('9083', 'ΖΙΤΣΑΣ', 'Zitsa'),
  ('9084', 'ΙΩΑΝΝΙΤΩΝ', 'Ioannina'),
  ('9085', 'ΚΟΝΙΤΣΑΣ', 'Konitsa'),
  ('9086', 'ΜΕΤΣΟΒΟΥ', 'Metsovo'),
  ('9087', 'ΠΩΓΩΝΙΟΥ', 'Pogoni'),
  ('9088', 'ΖΗΡΟΥ', 'Ziros'),
  ('9089', 'ΠΑΡΓΑΣ', 'Parga'),
  ('9090', 'ΠΡΕΒΕΖΑΣ', 'Preveza'),
  ('9091', 'ΑΡΓΙΘΕΑΣ', 'Argithea'),
  ('9092', 'ΚΑΡΔΙΤΣΑΣ', 'Karditsa'),
  ('9093', 'ΛΙΜΝΗΣ ΠΛΑΣΤΗΡΑ', 'Lake Plastira'),
  ('9094', 'ΜΟΥΖΑΚΙΟΥ', 'Mouzaki'),
  ('9095', 'ΠΑΛΑΜΑ', 'Palamas'),
  ('9096', 'ΣΟΦΑΔΩΝ', 'Sofades'),
  ('9097', 'ΑΓΙΑΣ', 'Agia'),
  ('9098', 'ΕΛΑΣΣΟΝΑΣ', 'Elassona'),
  ('9099', 'ΚΙΛΕΛΕΡ', 'Kileler'),
  ('9100', 'ΛΑΡΙΣΑΙΩΝ', 'Larissa'),
  ('9101', 'ΤΕΜΠΩΝ', 'Tempi'),
  ('9102', 'ΤΥΡΝΑΒΟΥ', 'Tyrnavos'),
  ('9103', 'ΦΑΡΣΑΛΩΝ', 'Farsala'),
  ('9104', 'ΑΛΜΥΡΟΥ', 'Almyros'),
  ('9105', 'ΒΟΛΟΥ', 'Volos'),
  ('9106', 'ΖΑΓΟΡΑΣ - ΜΟΥΡΕΣΙΟΥ', 'Zagora – Mouresi'),
  ('9107', 'ΝΟΤΙΟΥ ΠΗΛΙΟΥ', 'Southern Pelion'),
  ('9108', 'ΡΗΓΑ ΦΕΡΡΑΙΟΥ', 'Rigas Feraios'),
  ('9109', 'ΑΛΟΝΝΗΣΟΥ', 'Alonnisos'),
  ('9110', 'ΣΚΙΑΘΟΥ', 'Skiathos'),
  ('9111', 'ΣΚΟΠΕΛΟΥ', 'Skopelos'),
  ('9112', 'ΚΑΛΑΜΠΑΚΑΣ', 'Kalambaka'),
  ('9113', 'ΠΥΛΗΣ', 'Pyli'),
  ('9114', 'ΤΡΙΚΚΑΙΩΝ', 'Trikala'),
  ('9115', 'ΦΑΡΚΑΔΟΝΑΣ', 'Farkadona'),
  ('9116', 'ΖΑΚΥΝΘΟΥ', 'Zakynthos'),
  ('9117', 'ΙΘΑΚΗΣ', 'Ithaca'),
  ('9118', 'ΚΕΡΚΥΡΑΣ', 'Corfu'),
  ('9119', 'ΠΑΞΩΝ', 'Paxos'),
  ('9120', 'ΚΕΦΑΛΟΝΙΑΣ', 'Kefalonia'),
  ('9121', 'ΛΕΥΚΑΔΑΣ', 'Lefkada'),
  ('9122', 'ΜΕΓΑΝΗΣΙΟΥ', 'Meganisi'),
  ('9123', 'ΑΓΡΙΝΙΟΥ', 'Agrinio'),
  ('9124', 'ΑΚΤΙΟΥ - ΒΟΝΙΤΣΑΣ', 'Aktio – Vonitsa'),
  ('9125', 'ΑΜΦΙΛΟΧΙΑΣ', 'Amfilochia'),
  ('9126', 'ΘΕΡΜΟΥ', 'Thermo'),
  ('9127', 'ΙΕΡΑΣ ΠΟΛΗΣ ΜΕΣΟΛΟΓΓΙΟΥ', 'Holy City of Messolonghi'),
  ('9128', 'ΝΑΥΠΑΚΤΙΑΣ', 'Nafpaktia'),
  ('9129', 'ΞΗΡΟΜΕΡΟΥ', 'Xiromero'),
  ('9130', 'ΑΙΓΙΑΛΕΙΑΣ', 'Aigialeia'),
  ('9131', 'ΔΥΤΙΚΗΣ ΑΧΑΙΑΣ', 'Western Achaea'),
  ('9132', 'ΕΡΥΜΑΝΘΟΥ', 'Erymanthos'),
  ('9133', 'ΚΑΛΑΒΡΥΤΩΝ', 'Kalavryta'),
  ('9134', 'ΠΑΤΡΕΩΝ', 'Patras'),
  ('9135', 'ΑΝΔΡΑΒΙΔΑΣ - ΚΥΛΛΗΝΗΣ', 'Andravida – Kyllini'),
  ('9136', 'ΑΝΔΡΙΤΣΑΙΝΑΣ - ΚΡΕΣΤΕΝΩΝ', 'Andritsaina – Krestena'),
  ('9137', 'ΑΡΧΑΙΑΣ ΟΛΥΜΠΙΑΣ', 'Ancient Olympia'),
  ('9138', 'ΖΑΧΑΡΩΣ', 'Zacharo'),
  ('9139', 'ΗΛΙΔΑΣ', 'Ilida'),
  ('9140', 'ΠΗΝΕΙΟΥ', 'Pineios'),
  ('9141', 'ΠΥΡΓΟΥ', 'Pyrgos'),
  ('9142', 'ΑΛΙΑΡΤΟΥ', 'Aliartos'),
  ('9143', 'ΔΙΣΤΟΜΟΥ - ΑΡΑΧΟΒΑΣ - ΑΝΤΙΚΥΡΑΣ', 'Distomo – Arachova – Antikyra'),
  ('9144', 'ΘΗΒΑΙΩΝ', 'Thebes'),
  ('9145', 'ΛΕΒΑΔΕΩΝ', 'Livadeia'),
  ('9146', 'ΟΡΧΟΜΕΝΟΥ', 'Orchomenos'),
  ('9147', 'ΤΑΝΑΓΡΑΣ', 'Tanagra'),
  ('9148', 'ΔΙΡΦΥΩΝ - ΜΕΣΣΑΠΙΩΝ', 'Dirfys – Messapia'),
  ('9149', 'ΕΡΕΤΡΙΑΣ', 'Eretria'),
  ('9150', 'ΙΣΤΙΑΙΑΣ - ΑΙΔΗΨΟΥ', 'Istiaia – Aidipsos'),
  ('9151', 'ΚΑΡΥΣΤΟΥ', 'Karystos'),
  ('9152', 'ΚΥΜΗΣ - ΑΛΙΒΕΡΙΟΥ', 'Kymi – Aliveri'),
  ('9153', 'ΜΑΝΤΟΥΔΙΟΥ - ΛΙΜΝΗΣ - ΑΓΙΑΣ ΑΝΝΑΣ', 'Mantoudi – Limni – Agia Anna'),
  ('9154', 'ΣΚΥΡΟΥ', 'Skyros'),
  ('9155', 'ΧΑΛΚΙΔΕΩΝ', 'Chalkida'),
  ('9156', 'ΑΓΡΑΦΩΝ', 'Agrafa'),
  ('9157', 'ΚΑΡΠΕΝΗΣΙΟΥ', 'Karpenisi'),
  ('9158', 'ΑΜΦΙΚΛΕΙΑΣ - ΕΛΑΤΕΙΑΣ', 'Amfikleia – Elateia'),
  ('9159', 'ΔΟΜΟΚΟΥ', 'Domokos'),
  ('9160', 'ΛΑΜΙΕΩΝ', 'Lamia'),
  ('9161', 'ΛΟΚΡΩΝ', 'Lokroi'),
  ('9162', 'ΜΑΚΡΑΚΩΜΗΣ', 'Makrakomi'),
  ('9163', 'ΚΑΜΕΝΩΝ ΒΟΥΡΛΩΝ', 'Kamena Vourla'),
  ('9164', 'ΣΤΥΛΙΔΑΣ', 'Stylida'),
  ('9165', 'ΔΕΛΦΩΝ', 'Delphi'),
  ('9166', 'ΔΩΡΙΔΟΣ', 'Dorida'),
  ('9167', 'ΑΓΙΑΣ ΠΑΡΑΣΚΕΥΗΣ', 'Agia Paraskevi'),
  ('9168', 'ΑΜΑΡΟΥΣΙΟΥ', 'Marousi'),
  ('9169', 'ΒΡΙΛΗΣΣΙΩΝ', 'Vrilissia'),
  ('9170', 'ΗΡΑΚΛΕΙΟΥ', 'Heraklion'),
  ('9171', 'ΚΗΦΙΣΙΑΣ', 'Kifissia'),
  ('9172', 'ΛΥΚΟΒΡΥΣΗΣ - ΠΕΥΚΗΣ', 'Lykovrysi – Pefki'),
  ('9173', 'ΜΕΤΑΜΟΡΦΩΣΗΣ', 'Metamorfosi'),
  ('9174', 'ΝΕΑΣ ΙΩΝΙΑΣ', 'Nea Ionia'),
  ('9175', 'ΠΑΠΑΓΟΥ - ΧΟΛΑΡΓΟΥ', 'Papagou – Cholargos'),
  ('9176', 'ΠΕΝΤΕΛΗΣ', 'Penteli'),
  ('9177', 'ΦΙΛΟΘΕΗΣ - ΨΥΧΙΚΟΥ', 'Filothei – Psychiko'),
  ('9178', 'ΧΑΛΑΝΔΡΙΟΥ', 'Chalandri'),
  ('9179', 'ΑΓΙΑΣ ΒΑΡΒΑΡΑΣ', 'Agia Varvara'),
  ('9180', 'ΑΓΙΩΝ ΑΝΑΡΓΥΡΩΝ - ΚΑΜΑΤΕΡΟΥ', 'Agioi Anargyroi – Kamatero'),
  ('9181', 'ΑΙΓΑΛΕΩ', 'Aigaleo'),
  ('9182', 'ΙΛΙΟΥ', 'Ilion'),
  ('9183', 'ΠΕΡΙΣΤΕΡΙΟΥ', 'Peristeri'),
  ('9184', 'ΠΕΤΡΟΥΠΟΛΗΣ', 'Petroupolis'),
  ('9185', 'ΧΑΙΔΑΡΙΟΥ', 'Chaidari'),
  ('9186', 'ΑΘΗΝΑΙΩΝ', 'Athens'),
  ('9187', 'ΒΥΡΩΝΟΣ', 'Vyronas'),
  ('9188', 'ΓΑΛΑΤΣΙΟΥ', 'Galatsi'),
  ('9189', 'ΔΑΦΝΗΣ - ΥΜΗΤΤΟΥ', 'Dafni – Ymittos'),
  ('9190', 'ΖΩΓΡΑΦΟΥ', 'Zografou'),
  ('9191', 'ΗΛΙΟΥΠΟΛΗΣ', 'Ilioupoli'),
  ('9192', 'ΚΑΙΣΑΡΙΑΝΗΣ', 'Kaisariani'),
  ('9193', 'ΦΙΛΑΔΕΛΦΕΙΑΣ - ΧΑΛΚΗΔΟΝΟΣ', 'Filadelfeia – Chalkidona'),
  ('9194', 'ΑΓΙΟΥ ΔΗΜΗΤΡΙΟΥ', 'Agios Dimitrios'),
  ('9195', 'ΑΛΙΜΟΥ', 'Alimos'),
  ('9196', 'ΓΛΥΦΑΔΑΣ', 'Glyfada'),
  ('9197', 'ΕΛΛΗΝΙΚΟΥ - ΑΡΓΥΡΟΥΠΟΛΗΣ', 'Elliniko – Argyroupoli'),
  ('9198', 'ΚΑΛΛΙΘΕΑΣ', 'Kallithea'),
  ('9199', 'ΜΟΣΧΑΤΟΥ - ΤΑΥΡΟΥ', 'Moschato – Tavros'),
  ('9200', 'ΝΕΑΣ ΣΜΥΡΝΗΣ', 'Nea Smyrni'),
  ('9201', 'ΠΑΛΑΙΟΥ ΦΑΛΗΡΟΥ', 'Palaio Faliro'),
  ('9202', 'ΚΕΡΑΤΣΙΝΙΟΥ - ΔΡΑΠΕΤΣΩΝΑΣ', 'Keratsini – Drapetsona'),
  ('9203', 'ΚΟΡΥΔΑΛΛΟΥ', 'Korydallos'),
  ('9204', 'ΝΙΚΑΙΑΣ - ΑΓΙΟΥ ΙΩΑΝΝΗ ΡΕΝΤΗ', 'Nikaia – Agios Ioannis Rentis'),
  ('9205', 'ΠΕΙΡΑΙΩΣ', 'Piraeus'),
  ('9206', 'ΠΕΡΑΜΑΤΟΣ', 'Perama'),
  ('9207', 'ΑΓΚΙΣΤΡΙΟΥ', 'Agistri'),
  ('9208', 'ΑΙΓΙΝΑΣ', 'Aegina'),
  ('9209', 'ΚΥΘΗΡΩΝ', 'Kythira'),
  ('9210', 'ΠΟΡΟΥ', 'Poros'),
  ('9211', 'ΣΑΛΑΜΙΝΑΣ', 'Salamina'),
  ('9212', 'ΣΠΕΤΣΩΝ', 'Spetses'),
  ('9213', 'ΤΡΟΙΖΗΝΙΑΣ', 'Troizinia'),
  ('9214', 'ΥΔΡΑΣ', 'Hydra'),
  ('9215', 'ΑΧΑΡΝΩΝ', 'Acharnes'),
  ('9216', 'ΒΑΡΗΣ - ΒΟΥΛΑΣ - ΒΟΥΛΙΑΓΜΕΝΗΣ', 'Vari – Voula – Vouliagmeni'),
  ('9217', 'ΔΙΟΝΥΣΟΥ', 'Dionysos'),
  ('9218', 'ΚΡΩΠΙΑΣ', 'Kropia'),
  ('9219', 'ΛΑΥΡΕΩΤΙΚΗΣ', 'Lavreotiki'),
  ('9220', 'ΜΑΡΑΘΩΝΟΣ', 'Marathon'),
  ('9221', 'ΜΑΡΚΟΠΟΥΛΟΥ ΜΕΣΟΓΑΙΑΣ', 'Markopoulo Mesogaias'),
  ('9222', 'ΠΑΙΑΝΙΑΣ', 'Paiania'),
  ('9223', 'ΠΑΛΛΗΝΗΣ', 'Pallini'),
  ('9224', 'ΡΑΦΗΝΑΣ - ΠΙΚΕΡΜΙΟΥ', 'Rafina – Pikermi'),
  ('9225', 'ΣΑΡΩΝΙΚΟΥ', 'Saronikos'),
  ('9226', 'ΣΠΑΤΩΝ - ΑΡΤΕΜΙΔΟΣ', 'Spata – Artemis'),
  ('9227', 'ΩΡΩΠΟΥ', 'Oropos'),
  ('9228', 'ΑΣΠΡΟΠΥΡΓΟΥ', 'Aspropyrgos'),
  ('9229', 'ΕΛΕΥΣΙΝΑΣ', 'Eleusis'),
  ('9230', 'ΜΑΝΔΡΑΣ - ΕΙΔΥΛΛΙΑΣ', 'Mandra – Eidyllia'),
  ('9231', 'ΜΕΓΑΡΕΩΝ', 'Megara'),
  ('9232', 'ΦΥΛΗΣ', 'Fyli'),
  ('9233', 'ΑΡΓΟΥΣ - ΜΥΚΗΝΩΝ', 'Argos – Mycenae'),
  ('9234', 'ΕΠΙΔΑΥΡΟΥ', 'Epidaurus'),
  ('9235', 'ΕΡΜΙΟΝΙΔΑΣ', 'Ermionida'),
  ('9236', 'ΝΑΥΠΛΙΕΩΝ', 'Nafplio'),
  ('9237', 'ΒΟΡΕΙΑΣ ΚΥΝΟΥΡΙΑΣ', 'Northern Kynouria'),
  ('9238', 'ΓΟΡΤΥΝΙΑΣ', 'Gortynia'),
  ('9239', 'ΜΕΓΑΛΟΠΟΛΗΣ', 'Megalopolis'),
  ('9240', 'ΝΟΤΙΑΣ ΚΥΝΟΥΡΙΑΣ', 'Southern Kynouria'),
  ('9241', 'ΤΡΙΠΟΛΗΣ', 'Tripoli'),
  ('9242', 'ΒΕΛΟΥ - ΒΟΧΑΣ', 'Velo – Vocha'),
  ('9243', 'ΚΟΡΙΝΘΙΩΝ', 'Corinth'),
  ('9244', 'ΛΟΥΤΡΑΚΙΟΥ - ΠΕΡΑΧΩΡΑΣ - ΑΓΙΩΝ ΘΕΟΔΩΡΩΝ', 'Loutraki – Perachora – Agioi Theodoroi'),
  ('9245', 'ΝΕΜΕΑΣ', 'Nemea'),
  ('9246', 'ΞΥΛΟΚΑΣΤΡΟΥ - ΕΥΡΩΣΤΙΝΗΣ', 'Xylokastro – Evrostini'),
  ('9247', 'ΣΙΚΥΩΝΙΩΝ', 'Sikyona'),
  ('9248', 'ΑΝΑΤΟΛΙΚΗΣ ΜΑΝΗΣ', 'Eastern Mani'),
  ('9249', 'ΕΛΑΦΟΝΗΣΟΥ', 'Elafonisos'),
  ('9250', 'ΕΥΡΩΤΑ', 'Eurotas'),
  ('9251', 'ΜΟΝΕΜΒΑΣΙΑΣ', 'Monemvasia'),
  ('9252', 'ΣΠΑΡΤΗΣ', 'Sparta'),
  ('9253', 'ΔΥΤΙΚΗΣ ΜΑΝΗΣ', 'Western Mani'),
  ('9254', 'ΚΑΛΑΜΑΤΑΣ', 'Kalamata'),
  ('9255', 'ΜΕΣΣΗΝΗΣ', 'Messini'),
  ('9256', 'ΟΙΧΑΛΙΑΣ', 'Oichalia'),
  ('9257', 'ΠΥΛΟΥ - ΝΕΣΤΟΡΟΣ', 'Pylos – Nestor'),
  ('9258', 'ΤΡΙΦΥΛΙΑΣ', 'Trifylia'),
  ('9259', 'ΙΚΑΡΙΑΣ', 'Ikaria'),
  ('9260', 'ΦΟΥΡΝΩΝ ΚΟΡΣΕΩΝ', 'Fournoi Korseon'),
  ('9261', 'ΛΕΣΒΟΥ', 'Lesvos'),
  ('9262', 'ΑΓΙΟΥ ΕΥΣΤΡΑΤΙΟΥ', 'Agios Efstratios'),
  ('9263', 'ΛΗΜΝΟΥ', 'Lemnos'),
  ('9264', 'ΣΑΜΟΥ', 'Samos'),
  ('9265', 'ΟΙΝΟΥΣΣΩΝ', 'Oinousses'),
  ('9266', 'ΧΙΟΥ', 'Chios'),
  ('9267', 'ΨΑΡΩΝ', 'Psara'),
  ('9268', 'ΑΝΔΡΟΥ', 'Andros'),
  ('9269', 'ΑΝΑΦΗΣ', 'Anafi'),
  ('9270', 'ΘΗΡΑΣ', 'Thira'),
  ('9271', 'ΙΗΤΩΝ', 'Ios'),
  ('9272', 'ΣΙΚΙΝΟΥ', 'Sikinos'),
  ('9273', 'ΦΟΛΕΓΑΝΔΡΟΥ', 'Folegandros'),
  ('9274', 'ΑΓΑΘΟΝΗΣΙΟΥ', 'Agathonisi'),
  ('9275', 'ΑΣΤΥΠΑΛΑΙΑΣ', 'Astypalaia'),
  ('9276', 'ΚΑΛΥΜΝΙΩΝ', 'Kalymnos'),
  ('9277', 'ΛΕΙΨΩΝ', 'Lipsi'),
  ('9278', 'ΛΕΡΟΥ', 'Leros'),
  ('9279', 'ΠΑΤΜΟΥ', 'Patmos'),
  ('9280', 'ΚΑΡΠΑΘΟΥ', 'Karpathos'),
  ('9281', 'ΚΑΣΟΥ', 'Kasos'),
  ('9282', 'ΚΕΑΣ', 'Kea'),
  ('9283', 'ΚΥΘΝΟΥ', 'Kythnos'),
  ('9284', 'ΚΩ', 'Kos'),
  ('9285', 'ΝΙΣΥΡΟΥ', 'Nisyros'),
  ('9286', 'ΚΙΜΩΛΟΥ', 'Kimolos'),
  ('9287', 'ΜΗΛΟΥ', 'Milos'),
  ('9288', 'ΣΕΡΙΦΟΥ', 'Serifos'),
  ('9289', 'ΣΙΦΝΟΥ', 'Sifnos'),
  ('9290', 'ΜΥΚΟΝΟΥ', 'Mykonos'),
  ('9291', 'ΑΜΟΡΓΟΥ', 'Amorgos'),
  ('9292', 'ΝΑΞΟΥ & ΜΙΚΡΩΝ ΚΥΚΛΑΔΩΝ', 'Naxos & Small Cyclades'),
  ('9293', 'ΑΝΤΙΠΑΡΟΥ', 'Antiparos'),
  ('9294', 'ΠΑΡΟΥ', 'Paros'),
  ('9295', 'ΜΕΓΙΣΤΗΣ', 'Megisti'),
  ('9296', 'ΡΟΔΟΥ', 'Rhodes'),
  ('9297', 'ΣΥΜΗΣ', 'Symi'),
  ('9298', 'ΤΗΛΟΥ', 'Tilos'),
  ('9299', 'ΧΑΛΚΗΣ', 'Chalki'),
  ('9300', 'ΣΥΡΟΥ - ΕΡΜΟΥΠΟΛΗΣ', 'Syros – Ermoupoli'),
  ('9301', 'ΤΗΝΟΥ', 'Tinos'),
  ('9302', 'ΑΡΧΑΝΩΝ - ΑΣΤΕΡΟΥΣΙΩΝ', 'Archanes – Asterousia'),
  ('9303', 'ΒΙΑΝΝΟΥ', 'Viannos'),
  ('9304', 'ΓΟΡΤΥΝΑΣ', 'Gortyna'),
  ('9305', 'ΗΡΑΚΛΕΙΟΥ', 'Heraklion'),
  ('9306', 'ΜΑΛΕΒΙΖΙΟΥ', 'Malevizi'),
  ('9307', 'ΜΙΝΩΑ ΠΕΔΙΑΔΑΣ', 'Minoa Pediada'),
  ('9308', 'ΦΑΙΣΤΟΥ', 'Phaistos'),
  ('9309', 'ΧΕΡΣΟΝΗΣΟΥ', 'Hersonissos'),
  ('9310', 'ΑΓΙΟΥ ΝΙΚΟΛΑΟΥ', 'Agios Nikolaos'),
  ('9311', 'ΙΕΡΑΠΕΤΡΑΣ', 'Ierapetra'),
  ('9312', 'ΟΡΟΠΕΔΙΟΥ ΛΑΣΙΘΙΟΥ', 'Lasithi Plateau'),
  ('9313', 'ΣΗΤΕΙΑΣ', 'Sitia'),
  ('9314', 'ΑΓΙΟΥ ΒΑΣΙΛΕΙΟΥ', 'Agios Vasileios'),
  ('9315', 'ΑΜΑΡΙΟΥ', 'Amari'),
  ('9316', 'ΑΝΩΓΕΙΩΝ', 'Anogeia'),
  ('9317', 'ΜΥΛΟΠΟΤΑΜΟΥ', 'Mylopotamos'),
  ('9318', 'ΡΕΘΥΜΝΗΣ', 'Rethymno'),
  ('9319', 'ΑΠΟΚΟΡΩΝΟΥ', 'Apokoronas'),
  ('9320', 'ΓΑΥΔΟΥ', 'Gavdos'),
  ('9321', 'ΚΑΝΤΑΝΟΥ - ΣΕΛΙΝΟΥ', 'Kantanos – Selinos'),
  ('9322', 'ΚΙΣΣΑΜΟΥ', 'Kissamos'),
  ('9323', 'ΠΛΑΤΑΝΙΑ', 'Platanias'),
  ('9324', 'ΣΦΑΚΙΩΝ', 'Sfakia'),
  ('9325', 'ΧΑΝΙΩΝ', 'Chania')
) AS names (municipality_key, municipality_normalized_value, municipality_name_en)
WHERE municipality.municipality_key = names.municipality_key
  AND municipality.municipality_normalized_value = names.municipality_normalized_value
  AND municipality.municipality_name_en IS NULL;
-- END municipality English names (067)

-- BEGIN organization and region English names (068)
-- Reuse reviewed English labels, preserving Greek sources and existing corrections.
-- Organizations use original organization_value to preserve source acronyms.
-- For source_key type ΥΠΟΥΡΓΕΙΟ, use its reviewed full ministry name.
-- Regions store place names without the Region of prefix.
ALTER TABLE public.organization
  ADD COLUMN IF NOT EXISTS organization_name_en TEXT;
ALTER TABLE public.region
  ADD COLUMN IF NOT EXISTS region_name_en TEXT;

COMMENT ON COLUMN public.organization.organization_name_en IS
  'Reviewed English authority name from organization_value; source_key ministry type supplies the full ministry name. Greek source fields and identifiers remain unchanged.';
COMMENT ON COLUMN public.region.region_name_en IS
  'Reviewed English place name corresponding to region_normalized_value; excludes the Region of prefix. Greek source names and identifiers remain unchanged.';

UPDATE public.organization AS authority
SET organization_name_en = names.organization_name_en
FROM (VALUES
  ('org_b8680748c984673b5daa', 'ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ', 'ΥΠΟΥΡΓΕΙΟ::ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ', 'Ministry of Environment and Energy'),
  ('org_31bc787cb7b8af23fbad', 'ΕΛΛΗΝΙΚΗ ΕΤΑΙΡΕΙΑ ΣΥΜΜΕΤΟΧΩΝ ΚΑΙ ΠΕΡΙΟΥΣΙΑΣ ΑΕ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΕΛΛΗΝΙΚΗ ΕΤΑΙΡΕΙΑ ΣΥΜΜΕΤΟΧΩΝ ΚΑΙ ΠΕΡΙΟΥΣΙΑΣ ΑΕ', 'Hellenic Corporation of Assets and Participations S.A.'),
  ('org_ae2562c5d770b1f18197', 'ΕΘΝΙΚΗΣ ΑΜΥΝΑΣ', 'ΥΠΟΥΡΓΕΙΟ::ΕΘΝΙΚΗΣ ΑΜΥΝΑΣ', 'Ministry of National Defence'),
  ('org_9c86b9ce132adb1ac3c6', 'ΜΑΚΕΔΟΝΙΑΣ – ΘΡΑΚΗΣ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΜΑΚΕΔΟΝΙΑΣ – ΘΡΑΚΗΣ', 'Macedonia – Thrace'),
  ('org_911ae57efc85974d9790', 'ΠΟΛΙΤΙΣΜΟΥ', 'ΥΠΟΥΡΓΕΙΟ::ΠΟΛΙΤΙΣΜΟΥ', 'Ministry of Culture'),
  ('org_5d201080938fcce4fdd3', 'ΟΙΚΟΝΟΜΙΚΩΝ', 'ΥΠΟΥΡΓΕΙΟ::ΟΙΚΟΝΟΜΙΚΩΝ', 'Ministry of Finance'),
  ('org_e947b4654bccf4386b46', 'ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ', 'Peloponnese, Western Greece and the Ionian Islands'),
  ('org_ffefd3e4799ef6ddbe5a', 'ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ «ΣΥΝ ΠΑ»', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ::ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ «ΣΥΝ ΠΑ»', 'Protection and Development of Parnitha ‘Syn Pa’'),
  ('org_be2052fc0ad653f52c43', 'ΘΕΣΣΑΛΙΑΣ - ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΘΕΣΣΑΛΙΑΣ - ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ', 'Thessaly – Central Greece'),
  ('org_0125bac6cf9e80915770', 'ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ', 'Green Fund'),
  ('org_58bd4f9ed450b2a37109', 'ΜΕΤΑΝΑΣΤΕΥΣΗΣ ΚΑΙ ΑΣΥΛΟΥ', 'ΥΠΟΥΡΓΕΙΟ::ΜΕΤΑΝΑΣΤΕΥΣΗΣ ΚΑΙ ΑΣΥΛΟΥ', 'Ministry of Migration and Asylum'),
  ('org_287ebee5d6e5e055a518', 'ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ::ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)', 'Protection and Regeneration of Mount Penteli (SPAP)'),
  ('org_0c7ec11181f5a837f670', 'ΗΠΕΙΡΟΥ - ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΗΠΕΙΡΟΥ - ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', 'Epirus – Western Macedonia'),
  ('org_21035802794c70416c21', 'ΥΠΟΔΟΜΩΝ ΚΑΙ ΜΕΤΑΦΟΡΩΝ', 'ΥΠΟΥΡΓΕΙΟ::ΥΠΟΔΟΜΩΝ ΚΑΙ ΜΕΤΑΦΟΡΩΝ', 'Ministry of Infrastructure and Transport'),
  ('org_905a94d217b3f2968af6', 'ΡΥΘΜΙΣΤΙΚΗ ΑΡΧΗ ΑΠΟΒΛΗΤΩΝ ΕΝΕΡΓΕΙΑΣ ΚΑΙ ΥΔΑΤΩΝ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΡΥΘΜΙΣΤΙΚΗ ΑΡΧΗ ΑΠΟΒΛΗΤΩΝ ΕΝΕΡΓΕΙΑΣ ΚΑΙ ΥΔΑΤΩΝ', 'Regulatory Authority for Waste, Energy and Water'),
  ('org_5b1174647788f472fe8d', 'ΑΙΓΑΙΟΥ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΑΙΓΑΙΟΥ', 'Aegean'),
  ('org_db48525ab296094bd42b', 'ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ ΚΑΙ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ', 'ΥΠΟΥΡΓΕΙΟ::ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ ΚΑΙ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ', 'Ministry of Climate Crisis and Civil Protection'),
  ('org_4b2ee775d44f27a3f6cb', 'ΚΕΝΤΡΟ ΔΙΑΦΥΛΑΞΗΣ ΑΓΙΟΡΕΙΤΙΚΗΣ ΚΛΗΡΟΝΟΜΙΑΣ (Κ.Δ.Α.Κ.)', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΚΕΝΤΡΟ ΔΙΑΦΥΛΑΞΗΣ ΑΓΙΟΡΕΙΤΙΚΗΣ ΚΛΗΡΟΝΟΜΙΑΣ (Κ.Δ.Α.Κ.)', 'Centre for the Preservation of the Monastic Heritage of Mount Athos (K.D.A.K.)'),
  ('org_db93a7ce28345ac7e1fc', 'ΑΝΑΠΤΥΞΙΑΚΗ ΗΡΑΚΛΕΙΟΥ ΑΝΑΠΤΥΞΙΑΚΗ ΑΕ ΟΤΑ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΗ ΗΡΑΚΛΕΙΟΥ ΑΝΑΠΤΥΞΙΑΚΗ ΑΕ ΟΤΑ', 'Heraklion Development S.A. – Local Authorities'),
  ('org_6decee8e8755b0480f0e', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΘΡΙΑΣΙΟ Α Ε', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΘΡΙΑΣΙΟ Α Ε', 'Thriasio S.A. – Local Authority Development Organisation'),
  ('org_7fec6db0e3cfa6f9fe30', 'ΠΡΟΣΤΑΣΙΑΣ ΤΟΥ ΠΟΛΙΤΗ', 'ΥΠΟΥΡΓΕΙΟ::ΠΡΟΣΤΑΣΙΑΣ ΤΟΥ ΠΟΛΙΤΗ', 'Ministry of Citizen Protection'),
  ('org_41f140596606dd30d816', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΜΑΓΝΗΣΙΑΣ ΑΕ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΜΑΓΝΗΣΙΑΣ ΑΕ', 'Magnesia Local Authority Development Organisation S.A.'),
  ('org_17ac78379a8c9d97b785', 'ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΛΑΡΙΣΑΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΛΑΡΙΣΑΣ', 'Larissa Detention Centre'),
  ('org_b3a64c58dc367e777994', 'ΑΤΤΙΚΗΣ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΑΤΤΙΚΗΣ', 'Attica'),
  ('org_62a74133e465b58e371a', 'ΤΟΥΡΙΣΜΟΥ', 'ΥΠΟΥΡΓΕΙΟ::ΤΟΥΡΙΣΜΟΥ', 'Ministry of Tourism'),
  ('org_9e62f9cc92fe8bc375b2', 'ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ', 'Ymittos Protection and Development Association'),
  ('org_4e5878340eeacee52cb2', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΛΑΥΡΕΩΤΙΚΗΣ', 'Lavreotiki Development Association'),
  ('org_5a723e47860cafbd49d8', 'ΕΤΑΙΡΕΙΑ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΘΕΣΣΑΛΟΝΙΚΗΣ ΠΑΓΙΩΝ (ΕΥΑΘ ΠΑΓΙΩΝ)', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΕΤΑΙΡΕΙΑ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΘΕΣΣΑΛΟΝΙΚΗΣ ΠΑΓΙΩΝ (ΕΥΑΘ ΠΑΓΙΩΝ)', 'Thessaloniki Water Supply and Sewerage Fixed Assets Company (EYATH Pagion)'),
  ('org_67bce8f6bed1c94ece58', 'ΠΕΡΙΦΕΡΕΙΑΚΗ ΕΝΩΣΗ ΔΗΜΩΝ (ΠΕΔ) ΗΠΕΙΡΟΥ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΠΕΡΙΦΕΡΕΙΑΚΗ ΕΝΩΣΗ ΔΗΜΩΝ (ΠΕΔ) ΗΠΕΙΡΟΥ', 'Regional Union of Municipalities (Ped) of Epirus'),
  ('org_b4df5ac027d5c5e7e5ca', 'ΔΙΚΤΥΟ ΣΥΝΕΡΓΑΣΙΑΣ ΔΗΜΩΝ ΠΕΡΙΦΕΡΕΙΑΚΗΣ ΕΝΟΤΗΤΑΣ ΝΗΣΩΝ ΑΤΤΙΚΗΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΔΙΚΤΥΟ ΣΥΝΕΡΓΑΣΙΑΣ ΔΗΜΩΝ ΠΕΡΙΦΕΡΕΙΑΚΗΣ ΕΝΟΤΗΤΑΣ ΝΗΣΩΝ ΑΤΤΙΚΗΣ', 'Network of Co-Operating Municipalities of the Regional Unit of the Attica Islands'),
  ('org_c817e69ccc4a63d27571', 'ΚΕΡΚΥΡΑΣ ΔΗ.ΠΕ.ΘΕ.Κ.', 'ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ::ΚΕΡΚΥΡΑΣ ΔΗ.ΠΕ.ΘΕ.Κ.', 'Corfu Municipal Regional Theatre (DIPETHEK)'),
  ('org_440f970f5b6f69d7dde4', 'ΠΑΙΔΕΙΑΣ, ΘΡΗΣΚΕΥΜΑΤΩΝ ΚΑΙ ΑΘΛΗΤΙΣΜΟΥ', 'ΥΠΟΥΡΓΕΙΟ::ΠΑΙΔΕΙΑΣ, ΘΡΗΣΚΕΥΜΑΤΩΝ ΚΑΙ ΑΘΛΗΤΙΣΜΟΥ', 'Ministry of Education, Religious Affairs and Sports'),
  ('org_1cd2da6c86722f275410', 'ΑΝΑΠΤΥΞΙΑΚΗ ΕΤΑΙΡΕΙΑ ΣΕΡΡΩΝ ΑΕ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΝΟΜΟΥ ΣΕΡΡΩΝ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΗ ΕΤΑΙΡΕΙΑ ΣΕΡΡΩΝ ΑΕ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΝΟΜΟΥ ΣΕΡΡΩΝ', 'Serres Development Company S.A. Local Authority Development Organisation of the Prefecture of Serres'),
  ('org_7877f6ea11565a0f807f', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΙΑΣ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΤΡΟΙΖΗΝΙΑΣ', 'Troizina Development Association'),
  ('org_4561b79f38d1e63abc42', 'ΔΙΚΑΙΟΣΥΝΗΣ', 'ΥΠΟΥΡΓΕΙΟ::ΔΙΚΑΙΟΣΥΝΗΣ', 'Ministry of Justice'),
  ('org_c95084685d2f78ea1a2d', 'ΟΡΓΑΝΙΣΜΟΣ ΦΥΣΙΚΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΚΛΙΜΑΤΙΚΗΣ ΑΛΛΑΓΗΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΟΡΓΑΝΙΣΜΟΣ ΦΥΣΙΚΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΚΛΙΜΑΤΙΚΗΣ ΑΛΛΑΓΗΣ', 'Natural Environment and Climate Change Agency'),
  ('org_513806bec1bedcb228c1', 'ΑΝΑΠΤΥΞΙΑΚΗ ΠΡΟΟΠΤΙΚΗ Μ.Α.Ε. - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΥ ΓΑΛΑΤΣΙΟΥ Α.Ε.', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΗ ΠΡΟΟΠΤΙΚΗ Μ.Α.Ε. - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΥ ΓΑΛΑΤΣΙΟΥ Α.Ε.', 'Development Perspective Single-member S.A. – Galatsi Municipality Development Organisation S.A.'),
  ('org_f3f5e9eb2d2b06db04bc', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΝΟΜΟΥ ΛΑΡΙΣΑΣ ΑΕΝΟΛ ΑΕ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ ΝΟΜΟΥ ΛΑΡΙΣΑΣ ΑΕΝΟΛ ΑΕ', 'Larissa Prefecture Local Authority Development Organisation Aenol SA'),
  ('org_8b94b9e27446a84dbdb6', 'ΚΑΒΑΛΑΣ-ΔΗ.ΠΕ.ΘΕ. ΚΑΒΑΛΑΣ', 'ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ::ΚΑΒΑΛΑΣ-ΔΗ.ΠΕ.ΘΕ. ΚΑΒΑΛΑΣ', 'Kavala – Municipal Regional Theatre of Kavala'),
  ('org_8d46cb6e8a3ef078e9d6', 'ΕΣΩΤΕΡΙΚΩΝ', 'ΥΠΟΥΡΓΕΙΟ::ΕΣΩΤΕΡΙΚΩΝ', 'Ministry of the Interior'),
  ('org_80241f9fdf0fe899913d', 'ΕΘΝΙΚΟ ΣΥΣΤΗΜΑ ΥΠΟΔΟΜΩΝ ΠΟΙΟΤΗΤΑΣ (Ε.Σ.Υ.Π.)', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΕΘΝΙΚΟ ΣΥΣΤΗΜΑ ΥΠΟΔΟΜΩΝ ΠΟΙΟΤΗΤΑΣ (Ε.Σ.Υ.Π.)', 'National Quality Infrastructure System (ESYP)'),
  ('org_09e2c6a4fd9f9c98e3de', 'ΑΝΑΠΤΥΞΙΑΚΗ ΑΡΚΑΔΙΑΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΗ ΑΡΚΑΔΙΑΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ', 'Arkadia Development S.A. Local Authority Development Organisation'),
  ('org_ad8428dba322b2d17e37', 'ΠΕΡΙΦΕΡΕΙΑΣ ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ', 'ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ::ΠΕΡΙΦΕΡΕΙΑΣ ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ', 'Region of the North Aegean'),
  ('org_0cff27a84324a021e360', 'ΠΡΟΕΔΡΙΑ ΤΗΣ ΚΥΒΕΡΝΗΣΗΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΠΡΟΕΔΡΙΑ ΤΗΣ ΚΥΒΕΡΝΗΣΗΣ', 'Presidency of the Government'),
  ('org_c8b3631db6a25400055a', 'ΤΑΜΕΙΟ ΑΞΙΟΠΟΙΗΣΗΣ ΙΔΙΩΤΙΚΗΣ ΠΕΡΙΟΥΣΙΑΣ ΤΟΥ ΔΗΜΟΣΙΟΥ ΑΕ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΤΑΜΕΙΟ ΑΞΙΟΠΟΙΗΣΗΣ ΙΔΙΩΤΙΚΗΣ ΠΕΡΙΟΥΣΙΑΣ ΤΟΥ ΔΗΜΟΣΙΟΥ ΑΕ', 'Hellenic Republic Asset Development Fund S.A.'),
  ('org_243dbe7e118ce25c21b1', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΛΑΥΡΙΟΥ Α.Ε', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΛΑΥΡΙΟΥ Α.Ε', 'Lavrio Port Authority S.A.'),
  ('org_77f20d1582df63f4d18d', 'Δ.Ε.Υ.Α ΧΕΡΣΟΝΗΣΟΥ ΚΡΗΤΗΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::Δ.Ε.Υ.Α ΧΕΡΣΟΝΗΣΟΥ ΚΡΗΤΗΣ', 'DEYA Hersonissos, Crete'),
  ('org_daa27123ff0f4af57fb5', 'ΤΡΙΗΡΗΣ ΑΝΑΠΤΥΞΙΑΚΗ Α Ε ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΤΡΙΗΡΗΣ ΑΝΑΠΤΥΞΙΑΚΗ Α Ε ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ', 'Triiris Development S.A. – Local Authority Development Organisation'),
  ('org_33e6763be60f1da7b787', 'ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΩΝ ΤΟΥΡΚΟΒΟΥΝΙΩΝ', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ::ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΩΝ ΤΟΥΡΚΟΒΟΥΝΙΩΝ', 'Protection and Regeneration of Tourkovounia'),
  ('org_6f29bb58770d66fd4b7b', 'ΟΡΓΑΝΙΣΜΟΣ ΑΣΤΙΚΩΝ ΣΥΓΚΟΙΝΩΝΙΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΟΡΓΑΝΙΣΜΟΣ ΑΣΤΙΚΩΝ ΣΥΓΚΟΙΝΩΝΙΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ', 'Thessaloniki Urban Transport Organisation'),
  ('org_3052439cd9ee1a4afd9c', 'ΗΛΕΚΤΡΟΝΙΚΟΣ ΕΘΝΙΚΟΣ ΦΟΡΕΑΣ ΚΟΙΝΩΝΙΚΗΣ ΑΣΦΑΛΙΣΗΣ', '100059918', 'National Electronic Social Security Agency'),
  ('org_3b30addebb18596affcd', 'ΑΝΩΝΥΜΗ ΜΟΝΟΜΕΤΟΧΙΚΗ ΕΤΑΙΡΕΙΑ ΔΙΑΧΕΙΡΙΣΗΣ ΑΚΙΝΗΤΩΝ ΔΗΜΟΥ ΓΑΛΑΤΣΙΟΥ', '55035', 'Galatsi Municipality Single-Shareholder Real Estate Management Company'),
  ('org_4fac4e6913db6388857e', 'Γ.Ν. ΛΑΣΙΘΙΟΥ', '99221888', 'Lasithi General Hospital'),
  ('org_c252fdc144b592969922', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΚΡΗΤΗΣ', '99206917', 'University of Crete'),
  ('org_f9297d5db6d9bfb18cdb', 'ΥΠΟΥΡΓΕΙΟ ΠΟΛΙΤΙΣΜΟΥ', '100081912', 'Ministry of Culture'),
  ('org_7dd2e7cb902dbd7dd084', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΕΡΚΥΡΑΣ', '50308', 'Corfu Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_750b6d132be984ca5546', 'ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΚΙΝΗΤΩΝ ΛΑΥΡΕΩΤΙΚΗΣ', '100027533', 'Lavreotiki Municipal Real Estate Public Limited Company'),
  ('org_78fb69e4ef5c2d2000dd', 'ΗΔΙΚΑ Α.Ε (ΗΛΕΚΤΡΟΝΙΚΗ ΔΙΑΚΥΒΕΡΝΗΣΗ - ΚΟΙΝΩΝΙΚΗ ΑΣΦΑΛΙΣΗ Α.Ε)', '99219610', 'IDIKA S.A. – Electronic Governance and Social Security S.A.'),
  ('org_e21d1e806cf41d98c3d3', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΠΑΤΡΩΝ', '99206920', 'University of Patras'),
  ('org_a5ffb236a53cf3e9eeae', 'ΑΝΕΞΑΡΤΗΤΟΣ ΔΙΑΧΕΙΡΙΣΤΗΣ ΜΕΤΑΦΟΡΑΣ ΗΛΕΚΤΡΙΚΗΣ ΕΝΕΡΓΕΙΑΣ ΑΔΜΗΕ ΑΕ', '100034350', 'Independent Power Transmission Operator (ADMIE) S.A.'),
  ('org_d8e1808950646c30b21d', 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ', '100015996', 'Ministry of Environment and Energy'),
  ('org_15b47a9eca6ce1edb938', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΚΕΡΚΥΡΑΣ Α.Ε', '99221107', 'Corfu Port Authority S.A.'),
  ('org_bb7bdc88aa0706ee55d3', 'ΥΠΟΥΡΓΕΙΟ ΑΓΡΟΤΙΚΗΣ ΑΝΑΠΤΥΞΗΣ ΚΑΙ ΤΡΟΦΙΜΩΝ', '100015981', 'Ministry of Rural Development and Food'),
  ('org_d916d8a44a6493d2f89b', 'ΕΥΔΑΠ ΑΕ', '99201059', 'EYDAP SA'),
  ('org_0cbb82cab299820eff54', 'ΥΠΟΥΡΓΕΙΟ ΕΣΩΤΕΡΙΚΩΝ', '100054492', 'Ministry of the Interior'),
  ('org_f84d353783dfcb7912af', 'ΥΠΟΥΡΓΕΙΟ ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ ΚΑΙ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ', '100068650', 'Ministry of Climate Crisis and Civil Protection'),
  ('org_41e8879cc7d8c3851475', 'ΥΠΟΥΡΓΕΙΟ ΕΞΩΤΕΡΙΚΩΝ', '10', 'Ministry of Foreign Affairs'),
  ('org_1591024b6c337c80249a', 'ΥΠΟΥΡΓΕΙΟ ΥΠΟΔΟΜΩΝ ΚΑΙ ΜΕΤΑΦΟΡΩΝ', '100025905', 'Ministry of Infrastructure and Transport'),
  ('org_40e85b90da889ffb3553', 'ΥΠΟΥΡΓΕΙΟ ΕΡΓΑΣΙΑΣ ΚΑΙ ΚΟΙΝΩΝΙΚΗΣ ΑΣΦΑΛΙΣΗΣ', '100081533', 'Ministry of Labour and Social Security'),
  ('org_21e7db7bdc9aed72925b', 'ΥΠΟΥΡΓΕΙΟ ΕΘΝΙΚΗΣ ΑΜΥΝΑΣ', '6', 'Ministry of National Defence'),
  ('org_5ac13323c4cb1c7b009e', 'ΔΗΜΟΤΙΚΕΣ ΕΠΙΧΕΙΡΗΣΕΙΣ ΡΟΔΟΥ ΜΟΝΟΜΕΤΟΧΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΚΑΙ ΔΙΑΚΡΙΤΙΚΟ ΤΙΤΛΟ «Δ.Ε.Ρ.Μ.Α.Ε.»', '52681', 'Rhodes Municipal Enterprises, A Single-Shareholder Public Limited Company, with the Distinguishing Title ‘D.E.R.M.A.E.’'),
  ('org_8b0fcf7b182f5d4a9d70', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΠΑΡΟΥ', '50286', 'Paros Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_41171f91d88b426ae1de', 'ΕΘΝΙΚΟ ΚΕΝΤΡΟ ΑΙΜΟΔΟΣΙΑΣ (ΕΚΕΑ)', '50071', 'National Blood Donation Centre (EKEA)'),
  ('org_098300fd5e90ee9c6e45', 'ΙΟΝΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ', '99202011', 'Ionian University'),
  ('org_d596e6457a462fd94bc7', 'ΙΕΡΑ ΚΟΙΝΟΤΗΤΑ ΑΓΙΟΥ ΟΡΟΥΣ', '100038758', 'Holy Community of Mount Athos'),
  ('org_50f1938271d316e5e7da', '4Η ΔΥΠΕ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ', '50185', '4th Regional Health Authority of Macedonia and Thrace'),
  ('org_6b4fda73a6afd82df24f', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΙΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΜΕΙΖΟΝΟΣ ΒΟΛΟΥ', '50884', 'Greater Volos Municipal Water Supply and Sewerage Company'),
  ('org_f85feba7667f084aa86f', 'ΓΑΙΑ ΟΣΕ ΑΕ', '99200111', 'Gaia OSE SA'),
  ('org_ea6f660b05ccc0bf82c0', 'ΕΘΝΙΚΟ ΑΘΛΗΤΙΚΟ ΚΕΝΤΡΟ  ΛΑΡΙΣΑΣ', '99221224', 'Larissa National Sports Centre'),
  ('org_32b8a274193eb5c5b630', 'ΚΕΝΤΡΙΚΗ ΑΓΟΡΑ ΘΕΣ/ΝΙΚΗΣ', '99221070', 'Thessaloniki Central Market'),
  ('org_704bf2e432400ed492d9', 'Δ.Ε.Υ.Α ΘΗΡΑΣ', '51102', 'DEYA Thira'),
  ('org_90dd7cad46f5d6f3a818', 'ΑΝΕΞΑΡΤΗΤΗ ΑΡΧΗ ΔΗΜΟΣΙΩΝ ΕΣΟΔΩΝ ΑΑΔΕ', '100029495', 'Independent Authority for Public Revenue (AADE)'),
  ('org_19c822fca50a2334e540', 'ΔΥΠΕ   Α''  ΑΤΤΙΚΗΣ', '99221492', '1st Regional Health Authority of Attica'),
  ('org_d6af3bbc41684b7f4c9f', 'ΠΡΩΤΟΔΙΚΕΙΟ ΚΑΤΕΡΙΝΗΣ', '3387', 'Katerini Court of First Instance'),
  ('org_f13680cd3d20a2d63432', 'ΕΠΙΤΡΟΠΗ ΟΛΥΜΠΙΩΝ & ΚΛΗΡΟΔΟΤΗΜΑΤΩΝ', '50026', 'Committee on the Olympics and Bequests'),
  ('org_0f8098b528e9ca39a875', 'ΙΝΣΤΙΤΟΥΤΟ ΓΕΩΠΟΝΙΚΩΝ ΕΠΙΣΤΗΜΩΝ', '99200242', 'Institute of Agricultural Sciences'),
  ('org_a7c4b2c2eabe5b474ebe', 'ΜΕΤΟΧΙΚΟ ΤΑΜΕΙΟ ΑΕΡΟΠΟΡΙΑΣ', '99200366', 'Air Force Share Fund'),
  ('org_70f552a3c7706690ba02', 'ΕΤΑΙΡΕΙΑ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΘΕΣΣΑΛΟΝΙΚΗΣ (ΕΥΑΘ ΑΕ)', '99200068', 'Thessaloniki Water and Sewerage Company (EYATH SA)'),
  ('org_cf42d483ab5d3abd7b8e', 'ΑΣΤΙΚΗ ΑΝΑΠΤΥΞΗ ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', '51940', 'Urban Development Municipal Public Limited Company'),
  ('org_138f3ef5a128b1a2fc9d', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', '99206912', 'University of Western Macedonia'),
  ('org_32f7141f2edc1f1d702e', 'ΔΗΜΟΣΙΑ ΥΠΗΡΕΣΙΑ ΑΠΑΣΧΟΛΗΣΗΣ', '99219675', 'Public Employment Service'),
  ('org_f00ea0b248a1c6be1328', 'ΦΟΡΕΑΣ ΔΙΑΧΕΙΡΙΣΗΣ ΜΗΤΡΟΠΟΛΙΤΙΚΟΥ ΠΑΡΚΟΥ ΠΕΡΙΒΑΛΛΟΝΤΙΚΩΝ ΚΑΙ ΕΚΠΑΙΔΕΥΤΙΚΩΝ ΔΡΑΣΤΗΡΙΟΤΗΤΩΝ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΚΟΙΝΩΝΙΚΗΣ ΟΙΚΟΝ', '100025666', 'Management Body of the Metropolitan Park for Environmental and Educational Activities and Development Ofsocial Economy'),
  ('org_aeef42acca30e94c6340', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΣΑΛΑΜΙΝΑΣ', '55262', 'Salamina Municipal Port Fund'),
  ('org_4fc04d4234d5970e1a15', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΜΑΡΚΟΠΟΥΛΟΥ ΜΕΣΟΓΑΙΑΣ', '100086673', 'Markopoulo Mesogaias Municipal Port Fund'),
  ('org_5cb2a10ee6ca5691e7fb', 'ΠΕΡΙΒΑΛΛΟΝΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΑΘΗΝΑΣ ΠΕΙΡΑΙΑ', '50364', 'Environmental Association of Municipalities of Athens and Piraeus'),
  ('org_74e852af6a333e5c921a', 'ΠΑΝΠΕΛΟΠΟΝΝΗΣΙΑΚΟ ΕΘΝΙΚΟ ΑΘΛΗΤΙΚΟ ΚΕΝΤΡΟ ΠΑΤΡΩΝ', '99221443', 'Pan-Peloponnesian National Sports Centre of Patras'),
  ('org_59389e15c493f60d398c', 'ΕΛΛΗΝΙΚΟ ΦΕΣΤΙΒΑΛ Α.Ε', '99221314', 'Hellenic Festival S.A.'),
  ('org_ca6b1acf9a8c5d9ec8a2', 'ΕΘΝΙΚΑ ΑΘΛΗΤΙΚΑ ΚΕΝΤΡΑ ΘΕΣΣΑΛΟΝΙΚΗΣ', '54586', 'National Sports Centres of Thessaloniki'),
  ('org_82219565ec160f0ce7c4', 'ΕΘΝΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΠΑΡΟΧΗΣ ΥΠΗΡΕΣΙΩΝ ΥΓΕΙΑΣ', '54497', 'National Health Services Organisation'),
  ('org_9a982a1409126619f8dd', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ', '50065', 'Development Association of Western Athens'),
  ('org_07a1329bab564552fa51', 'ΣΥΝΔΕΣΜΟΣ ΥΔΡΕΥΣΗΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΛΕΚΑΝΟΠΕΔΙΟΥ ΙΩΑΝΝΙΝΩΝ', '50886', 'Association of Municipalities and Communities for Water Supply in the Ioannina Basin'),
  ('org_243dbe7e118ce25c21b1', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ  ΛΑΥΡΙΟΥ Α.Ε', '99221099', 'Lavrio Port Authority S.A.'),
  ('org_1d96a3424a09a87ac91a', 'ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΚΙΝΗΤΩΝ ΙΩΑΝΝΙΝΩΝ  (ΔΗ.ΑΝ.ΕΤ.Α.Ι.)', '55124', 'Ioannina Municipal Real Estate S.A. (DIANETAI)'),
  ('org_ba0e906fa3554796da42', 'ΟΡΓΑΝΙΣΜΟΣ ΣΙΔΗΡΟΔΡΟΜΩΝ ΕΛΛΑΔΑΣ (ΟΣΕ)', '99200192', 'Hellenic Railways Organisation (OSE)'),
  ('org_371561b3a358d1b9580f', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΚΡΗΤΗΣ', '50204', 'Decentralised Administration of Crete'),
  ('org_5cc825e67b387ab8ae5a', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΚΑΙ ΚΟΙΝΟΤΗΤΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΤΥΞΗ ΤΗΣ ΠΑΡΝΗΘΑΣ «ΣΥΝ ΠΑ»', '51410', 'Association of Municipalities and Communities for the Protection and Development of Parnitha “SYN PA”'),
  ('org_2df168a2f8649b38c80e', 'ΥΠΟΥΡΓΕΙΟ ΠΡΟΣΤΑΣΙΑΣ ΤΟΥ ΠΟΛΙΤΗ', '100054489', 'Ministry of Citizen Protection'),
  ('org_7b6f9b6059e6710ff86e', 'ΠΕΡΙΦΕΡΕΙΑΚΟΣ ΦΟΡΕΑΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ ΠΕΡΙΦΕΡΕΙΑΣ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΤΩΝ ΟΤΑ', '100016876', 'Regional Solid Waste Management Authority of the Central Greece Region, A Public Limited Company of the Local Authorities'),
  ('org_1d9b6f0a3caf48766047', 'ΕΘΝΙΚΟ & ΚΑΠΟΔΙΣΤΡΙΑΚΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΑΘΗΝΩΝ', '99203020', 'National and Kapodistrian University of Athens'),
  ('org_4b19771f941371e9cfbe', 'ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ ΠΑΤΡΩΝ', '53456', 'Patras Municipal Nursery'),
  ('org_4f37c0c3ac94578c0ec5', 'ΑΣΠΑΙΤΕ', '99202057', 'ASPAITE'),
  ('org_071aed93beeb20f6a9d6', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΗΠΕΙΡΟΥ - ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', '50201', 'Decentralised Administration of Epirus – Western Macedonia'),
  ('org_5f4209d9818f245e7fba', 'ΕΛΛΗΝΙΚΑ ΑΜΥΝΤΙΚΑ ΣΥΣΤΗΜΑΤΑ  ΑΒΕΕ', '99200358', 'Hellenic Defence Systems S.A.'),
  ('org_a2a3af6cc8b6d3dde457', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΕΣΣΑΛΙΑΣ', '99206913', 'University of Thessaly'),
  ('org_fc533458afa49dc6a993', 'Δ.Ε.Υ.Α ΚΙΛΚΙΣ', '52460', 'DEYA Kilkis'),
  ('org_0f62461963d09b8706f4', 'ΑΡΣΙΣ ΚΟΙΝΩΝΙΚΗ ΟΡΓΑΝΩΣΗ ΥΠΟΣΤΗΡΙΞΗΣ ΝΕΩΝ', '100033257', 'Arsis Social Organisation for Youth Support'),
  ('org_cb270bb92f5f823ad81e', 'ΠΕΡΙΦΕΡΕΙΑ ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ - ΘΡΑΚΗΣ', '5001', 'Region of Eastern Macedonia – Thrace'),
  ('org_06c78ebdfa87793da1ee', 'ΑΞΙΟΠΟΙΗΣΗ ΔΗΜΟΤΙΚΗΣ ΑΚΙΝΗΤΗΣ ΠΕΡΙΟΥΣΙΑΣ ΑΓΙΑΣ - ΑΝΩΝΥΜΟΣ ΕΤΑΙΡΕΙΑ', '51236', 'Utilisation of Municipal Real Estate in Agia – Public Limited Company'),
  ('org_d8e7bf1b2371cec5045b', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΕΡΜΙΟΝΙΔΑΣ', '52711', 'Ermionida Municipal Port Fund'),
  ('org_a5e67c128d3ce017eea7', 'ΥΠΟΥΡΓΕΙΟ ΠΑΙΔΕΙΑΣ, ΘΡΗΣΚΕΥΜΑΤΩΝ ΚΑΙ ΑΘΛΗΤΙΣΜΟΥ', '100081880', 'Ministry of Education, Religious Affairs and Sports'),
  ('org_856c2dddffbd738ed47e', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΛΕΣΒΟΥ', '53187', 'Lesvos Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_5f452b4f8ae6509d8b7a', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)', '53355', 'Association of Municipalities for the Protection and Regeneration of Mount Penteli (SPAP)'),
  ('org_94cef4e32c18906c06f9', 'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΑΤΤΙΚΗΣ', '55400', 'Attica Regional Social Welfare Centre'),
  ('org_1877507ef51193c12955', 'ΠΕΡΙΦΕΡΕΙΑΚΟ ΦΥΤΩΡΙΟ ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ ΑΝΩΝΥΜΟΣ ΕΤΑΙΡΕΙΑ', '100064310', 'South Aegean Regional Nursery Public Limited Company'),
  ('org_0b17d98feb3587e3c0e1', 'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', '55412', 'Central Macedonia Regional Social Welfare Centre'),
  ('org_5a757ffe05c755ffdb14', 'ΙΝΣΤΙΤΟΥΤΟ ΤΕΧΝΟΛΟΓΙΑΣ ΥΠΟΛΟΓΙΣΤΩΝ ΚΑΙ ΕΚΔΟΣΕΩΝ «ΔΙΟΦΑΝΤΟΣ»', '99206108', 'Computer Technology Institute and Press “Diophantus”'),
  ('org_c2a9526e0f18b6d661a3', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΒΟΡΕΙΟΥ ΑΞΟΝΑ ΧΑΝΙΩΝ', '50887', 'Municipal Water Supply and Sewerage Company of the Northern Axis of Chania'),
  ('org_42d4837a077f4a4c9a10', 'ΕΘΝΙΚΗ ΠΙΝΑΚΟΘΗΚΗ ΚΑΙ ΜΟΥΣΕΙΟ ΑΛΕΞΑΝΔΡΟΥ ΣΟΥΤΖΟΥ', '99221222', 'National Gallery and Alexandros Soutzou Museum'),
  ('org_ada6abef946b2c0b5663', 'ΔΙΑΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ Δ.Ε.ΔΙ.Σ.Α. Α.Ε. Ο.Τ.Α.', '50564', 'Inter-municipal Solid Waste Management Company (DEDISA) S.A. – Local Authorities'),
  ('org_361f8ee0854ca6af0666', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΩΝ ΤΟΥΡΚΟΒΟΥΝΙΩΝ', '51989', 'Association of Municipalities for the Protection and Regeneration of Tourkovounia'),
  ('org_72706cba4fdf24c207c0', 'ΣΥΝΔΕΣΜΟΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ Ν ΑΧΑΙΑΣ', '100059389', 'Solid Waste Management Association of Achaia Prefecture'),
  ('org_9e84e5c294b02d2d85aa', 'ΤΑΜΕΙΟ ΠΑΡΑΚΑΤΑΘΗΚΩΝ K ΔΑΝΕΙΩΝ', '99221150', 'Consignment Deposits and Loans Fund'),
  ('org_8b854a43a9283f7a6b28', 'ΕΘΝΙΚΟ ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΑΛΛΗΛΕΓΓΥΗΣ', '99221513', 'National Centre for Social Solidarity'),
  ('org_16e33d42aee752dc7b9a', 'ΕΘΝΙΚΟ ΑΣΤΕΡΟΣΚΟΠΕΙΟ ΑΘΗΝΩΝ', '50067', 'National Observatory of Athens'),
  ('org_8cee97e73b077e60ea6e', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΕΛΕΥΣΙΝΟΣ Α.Ε', '99221102', 'Elefsina Port Authority S.A.'),
  ('org_cf5d6153438a27b51151', 'ΓΡΑΜΜΑΤΕΙΑ ΠΡΩΤΟΔΙΚΕΙΟΥ ΘΗΒΑΣ', '3553', 'Secretariat of the Court of First Instance of Thebes'),
  ('org_707811d1ecaa476b6a38', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΔΥΤΙΚΗΣ ΑΤΤΙΚΗΣ', '100038385', 'University of West Attica'),
  ('org_dbc6be8a1034ac9ee27d', 'Ο.ΣΥ. Α.Ε. - ΟΔΙΚΕΣ ΣΥΓΚΟΙΝΩΝΙΕΣ Α.Ε.', '53362', 'OSY S.A. – Road Transport S.A.'),
  ('org_10a663007810af7275f5', 'ΟΡΓΑΝΙΣΜΟΣ ΚΕΝΤΡΙΚΩΝ ΑΓΟΡΩΝ & ΑΛΙΕΙΑΣ', '99221096', 'Central Markets & Fisheries Organisation'),
  ('org_44681bb1139a12534cbb', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΛΑΡΙΣΑΣ', '50820', 'Larissa Municipal Water Supply and Sewerage Company'),
  ('org_eef4fc03100579d86acb', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΣΚΑΛΑΣ ΩΡΩΠΟΥ', '55028', 'Skala Oropos Municipal Port Fund'),
  ('org_4dd803a4ec3ddd89c879', 'ΒΙΟΤΕΧΝΙΚΟ ΕΠΙΜΕΛ. ΑΘΗΝΩΝ', '99220961', 'Athens Chamber of Small Industries'),
  ('org_ca369a17db333c0dffdf', 'ΚΑΥΤΑΝΤΖΟΓΛΕΙΟ ΕΘΝΙΚΟ ΣΤΑΔΙΟ', '99221365', 'Kaftanzoglio National Stadium'),
  ('org_a4de17d00817e74b1974', 'ΕΤΑΙΡΕΙΑ ΠΑΓΙΩΝ ΕΥΔΑΠ', '99201057', 'EYDAP Fixed Assets Company'),
  ('org_dd26eca7fd93fe98287c', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΕΔΕΣΣΑΣ', '50610', 'Edessa Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_e0edc7ab92fa6629f88a', 'ΒΙΟΤΕΧΝΙΚΟ ΕΠΙΜΕΛ. ΘΕΣ/ΝΙΚΗΣ', '99220962', 'Thessaloniki Chamber of Handicrafts'),
  ('org_aa705de541485ce9c369', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΜΥΚΟΝΟΥ', '50351', 'Mykonos Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_1517899391ad2ce8b2d3', 'ΠΕΡΙΦΕΡΕΙΑΚΗ ΕΝΩΣΗ ΔΗΜΩΝ ΑΤΤΙΚΗΣ', '54215', 'Regional Union of Municipalities of Attica'),
  ('org_6f266db51b004b97051e', 'ΣΥΝΔΕΣΜΟΣ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ ΚΑΙ ΑΛΛΗΛΕΓΓΥΗΣ ΔΗΜΩΝ ΜΥΤΙΛΗΝΗΣ ΚΑΙ ΔΥΤΙΚΗΣ ΛΕΣΒΟΥ', '100059958', 'Association for Social Protection and Solidarity of the Municipalities of Mytilene and Western Lesvos'),
  ('org_aff73174761228b0c762', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ', '50743', 'Alexandroupolis Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_2a63e6db55d9b296061b', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΚΑΙ ΑΠΟΧΕΤΕΥΣΗΣ ΔΗΜΟΥ ΒΟΡΕΙΑΣ ΚΕΡΚΥΡΑΣ', '100089549', 'Municipal Water Supply and Sewerage Company of the Municipality of Northern Corfu'),
  ('org_793eb1c4a1283619d733', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΣΥΡΟΥ', '50744', 'Syros Municipal Port Fund'),
  ('org_2389582d025b1319dcea', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΘΕΣΣΑΛΙΑΣ - ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ', '50200', 'Decentralised Administration of Thessaly – Central Greece'),
  ('org_5e71eb38f8fc3e389db2', 'ΛΟΥΤΡΑ ΛΟΥΤΡΑΚΙΟΥ ΔΗΜΟΥ ΑΛΜΩΠΙΑΣ Α.Ε', '51974', 'Loutraki Baths of the Municipality of Almopia S.A.'),
  ('org_da2c12d8c5615a977582', 'ΕΘΝΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΣΙΑΣ ΥΓΕΙΑΣ ΕΟΔΥ', '100046286', 'National Public Health Organisation (EODY)'),
  ('org_ff0f5532d7966f4e2ee9', 'ΕΘΝΙΚΟΣ ΚΗΠΟΣ ΜΗΤΡΟΠΟΛΙΤΙΚΟ ΠΡΑΣΙΝΟ ΑΕ', '100067665', 'National Garden Metropolitan Green Spaces PLC'),
  ('org_e22c772b24e3dfb958e2', 'ΣΤΑΘΕΡΕΣ ΣΥΓΚΟΙΝΩΝΙΕΣ Α.Ε. (ΣΤΑΣΥ)', '50083', 'STASY S.A. – Urban Rail Transport'),
  ('org_3127d595b0bba14fc6e0', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΑΝΔΡΟΥ', '100080881', 'Andros Municipal Port Fund'),
  ('org_c6c0b9d922a60b4c461f', 'ΓΕΝΙΚΟ ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΘΕΣΣΑΛΟΝΙΚΗΣ', '99200385', 'Thessaloniki General Detention Centre'),
  ('org_9c2f761f2e25d57b505e', 'ΙΔΡΥΜΑ ΝΕΟΛΑΙΑΣ ΚΑΙ ΔΙΑ ΒΙΟΥ ΜΑΘΗΣΗΣ', '99203024', 'Foundation for Youth and Lifelong Learning'),
  ('org_dfa8ddd1bf069d8e8934', 'ΜΟΥΣΕΙΟ ΦΥΣΙΚΗΣ ΙΣΤΟΡΙΑΣ ΑΠΟΛΙΘΩΜΕΝΟΥ ΔΑΣΟΥΣ ΛΕΣΒΟΥ (Μ.Φ.Ι.Α.Λ.)', '99221412', 'Natural History Museum of the Lesvos Petrified Forest (MFIAL)'),
  ('org_23b43379e5f13c8b5fdc', 'ΑΝΑΓΚΑΣΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ ΔΙΑΧΕΙΡΙΣΤΙΚΗΣ ΕΝΟΤΗΤΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΗΠΕΙΡΟΥ', '100055352', 'Compulsory Solid Waste Management Association of the Epirus Region Management Unit'),
  ('org_2ee85d06d446bcc055e7', 'ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙO ΑΘΗΝΩΝ ''ΙΠΠΟΚΡΑΤΕΙΟ''', '99221988', 'Athens Regional General Hospital “Ippokrateio”'),
  ('org_e80d6603776f6e3a9747', 'ΠΡΩΤΟΔΙΚΕΙΟ ΒΟΛΟΥ', '3380', 'Volos Court of First Instance'),
  ('org_eb1828bac8266e3c317b', 'ΕΙΔΙΚΟΣ ΠΕΡΙΦΕΡΕΙΑΚΟΣ ΔΙΑΒΑΘΜΙΔΙΚΟΣ ΦΟΡΕΑΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', '100062273', 'Special Regional Multi-Level Solid Waste Management Body of the South Aegean Public Limited Company'),
  ('org_42c73d8350d99df93b62', 'ΥΠΟΥΡΓΕΙΟ ΤΟΥΡΙΣΜΟΥ', '100025893', 'Ministry of Tourism'),
  ('org_a0f4ad43ba482421696f', 'ΥΠΟΥΡΓΕΙΟ ΑΝΑΠΤΥΞΗΣ', '100081597', 'Ministry of Development'),
  ('org_fab7a1226dff3166ea31', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΔΗΜΟΥ ΡΟΔΟΥ (Δ.Ε.Υ.ΑΡ)', '52826', 'Municipal Water Supply and Sewerage Company of the Municipality of Rhodes (DEYAR)'),
  ('org_7e36a950d1d11ac263b2', 'ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΠΑΤΡΩΝ', '99222013', 'Regional University General Hospital of Patras'),
  ('org_1371ba35f47b94ae00c0', 'ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΓΙΟΥ ΝΙΚΟΛΑΟΥ', '50545', 'Agios Nikolaos Municipal Public Limited Company'),
  ('org_22a67716c45c7d5989ea', 'ΟΙΚΟΝΟΜΙΚΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΑΘΗΝΩΝ', '99206861', 'Athens University of Economics and Business'),
  ('org_2b02948df19322b24fae', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΠΑΡΟΥ - ΑΝΤΙΠΑΡΟΥ', '52130', 'Paros – Antiparos Municipal Port Fund'),
  ('org_c814789181186e2d0c20', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΧΙΟΥ', '50087', 'Chios Municipal Port Fund'),
  ('org_b18df00ee8c68337188b', 'ΠΟΛΥΜΕΤΟΧΙΚΗ ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΠΟΛΙΤΙΣΜΟΥ ΤΟΥΡΙΣΜΟΥ ΑΝΑΠΤΥΞΗΣ ΗΡΑΚΛΕΙΟΥ', '100048449', 'Heraklion Multiple-Shareholder Municipal Public Limited Company for Culture, Tourism and Development'),
  ('org_813bbf2c8094bb9a740f', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΘΗΡΑΣ', '51724', 'Thira Municipal Port Fund'),
  ('org_bc5f0adad951e0eb7eee', 'ΔΕΥΑ Δήμου Τεμπών', '50734', 'DEYA of the Municipality of Tempi'),
  ('org_c4657dc491654880125d', 'ΑΡΧΑΙΟΛΟΓΙΚΟ ΜΟΥΣΕΙΟ ΗΡΑΚΛΕΙΟΥ', '100081047', 'Heraklion Archaeological Museum'),
  ('org_2e9547703ba2692564c2', 'ΚΡΑΤΙΚΗ ΣΧΟΛΗ ΟΡΧΗΣΤΙΚΗΣ ΤΕΧΝΗΣ', '99221383', 'State School of Dance'),
  ('org_fd96c133d940bc1b6803', 'ΕΘΝΙΚΟ ΜΟΥΣΕΙΟ ΣΥΓΧΡΟΝΗΣ ΤΕΧΝΗΣ', '50025', 'National Museum of Contemporary Art'),
  ('org_ee537228d8d1250fa1cc', '3Η ΔΥΠΕ ΜΑΚΕΔΟΝΙΑΣ', '50048', '3rd Regional Health Authority of Macedonia'),
  ('org_70166e38e0a0154cb968', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΑΤΡΑΣ ΔΕΥΑΠ', '50652', 'Patras Municipal Water Supply and Sewerage Company (DEYAP)'),
  ('org_486d8bcf00b5c63c7c57', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΕΩΣ ΚΑΙ ΑΠΟΧΕΤΕΥΣΕΩΣ ΧΑΛΚΙΔΟΣ', '51301', 'Chalkida Municipal Water Supply and Sewerage Company'),
  ('org_b771395bb245f78fccbc', 'ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΦΘΙΩΤΙΔΑΣ', '50226', 'Fthiotida Port Fund'),
  ('org_ff40882032064f1ad2a6', 'ΥΠΟΥΡΓΕΙΟ ΝΑΥΤΙΛΙΑΣ ΚΑΙ ΝΗΣΙΩΤΙΚΗΣ ΠΟΛΙΤΙΚΗΣ', '100015969', 'Ministry of Maritime Affairs and Insular Policy'),
  ('org_488a85498c3df1cdd618', 'ΚΟΙΝΩΝΙΑ ΤΗΣ ΠΛΗΡΟΦΟΡΙΑΣ ΑΕ', '99220730', 'Information Society S.A.'),
  ('org_5077a649a206e0cc5f88', 'ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΛΑΡΙΣΑΣ', '99222012', 'Regional University General Hospital of Larissa'),
  ('org_fb2d5fb8e008708cce86', 'ΞΙΝΟ ΝΕΡΟ ΜΟΝΟΜΕΤΟΧΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΙΑ ΟΤΑ', '55127', 'Xino Nero Single-Shareholder Public Limited Company of Local Authorities'),
  ('org_718c92c984e58c89b014', 'ΚΟΙΝΩΝΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΥ ΠΑΤΡΕΩΝ', '52939', 'Social Organisation of the Municipality of Patras'),
  ('org_143a6c922c7d7a726b79', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΣΕΡΡΩΝ (Δ.Ε.Υ.ΑΣ)', '50621', 'Serres Municipal Water Supply and Sewerage Company (DEYAS)'),
  ('org_047ac43305200cfe559b', '2Η ΔΥΠΕ ΠΕΙΡΑΙΩΣ ΚΑΙ ΑΙΓΑΙΟΥ', '99221499', '2nd Regional Health Authority of Piraeus and the Aegean'),
  ('org_6e58405a831716b35b50', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΠΑΤΡΩΝ  Α.Ε', '99221108', 'Patras Port Authority S.A.'),
  ('org_7921eda9fa54a9e3e276', 'ΦΕΣΤΙΒΑΛ ΑΘΗΝΩΝ ΕΠΙΔΑΥΡΟΥ', '100091875', 'Athens-Epidaurus Festival'),
  ('org_97921215530df0d0f56f', 'ΘΕΡΑΠΕΥΤΗΡΙΟ ΧΡΟΝΙΩΝ ΠΑΘΗΣΕΩΝ ΕΥΡΥΤΑΝΙΑΣ', '100011580', 'Eurytania Chronic Diseases Treatment Centre'),
  ('org_50cc98bf3e96adb5d16b', 'ΚΡΑΤΙΚΟ ΘΕΑΤΡΟ ΒΟΡΕΙΟΥ ΕΛΛΑΔΟΣ', '99221384', 'State Theatre of Northern Greece'),
  ('org_7fcd58724b61d58fa934', 'ΑΝΑΠΤΥΞΙΑΚΗ ΑΘΗΝΑΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ Α.Ε.', '50384', 'Athens Development S.A. – Local Authority Development Organisation S.A.'),
  ('org_45130d9005ea8895762d', 'ΜΟΝΟΜΕΤΟΧΙΚΗ ΑΞΙΟΠΟΙΗΣΗΣ ΑΚΙΝΗΤΩΝ ΔΗΜΟΥ ΧΙΟΥ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ Ο.Τ.Α. ΔΙΑΧΕΙΡΗΣΗ ΑΚΙΝΗΤΩΝ', '54788', 'Chios Municipal Property Development Single-shareholder S.A. – Local Authorities – Property Management'),
  ('org_00528c6d17f3bb833905', 'ΠΡΩΤΟΔΙΚΕΙΟ ΑΘΗΝΩΝ', '50111', 'Court of First Instance of Athens'),
  ('org_a5a8ca7d72d74c85e7e5', 'ΕΠΙΜΕΛΗΤΗΡΙΟ ΚΑΡΔΙΤΣΑΣ', '99221027', 'Karditsa Chamber of Commerce'),
  ('org_e2f4b74b5199fac53669', 'ΦΟΡΕΑΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ ΠΕΡΙΦΕΡΕΙΑΣ ΘΕΣΣΑΛΙΑΣ -ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ Ο.Τ.Α. Α.Ε.', '100051242', 'Solid Waste Management Authority of the Region of Thessaly – Local Authority Public Limited Company S.A.'),
  ('org_3694a03979ab993d2cf7', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΑΙΓΑΙΟΥ', '99206911', 'University of the Aegean'),
  ('org_fee793b6a0d39569db28', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΤΡΙΚΑΛΩΝ', '51546', 'Trikala Municipal Water Supply and Sewerage Company'),
  ('org_baa42e46c78673ae12fc', 'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΗΠΕΙΡΟΥ', '55403', 'Social Welfare Centre of the Region of Epirus'),
  ('org_a1045be0f39b1f3551ef', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΔΙΟΥ ΟΛΥΜΠΟΥ', '100029457', 'Dion – Olympus Municipal Water Supply and Sewerage Company'),
  ('org_a3ddd0db78642eed939d', 'ΠΑΡΚΟ ΠΑΡΟΥ ΔΗΜΟΤΙΚΗ Α.Ε.', '54924', 'Paros Park Municipal S.A.'),
  ('org_ec4f940c887042c647d2', 'ΕΙΔΙΚΟ ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΝΕΩΝ ΑΥΛΩΝΑ', '99201039', 'Avlona Special Youth Detention Centre'),
  ('org_2ab503c2a6828e6df9e0', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ', '50202', 'Decentralised Administration of the Peloponnese, Western Greece and the Ionian Islands'),
  ('org_ac85e80ec90d08116491', 'ΟΑΚΑ (ΣΠΥΡΟΣ ΛΟΥΗΣ)', '99221418', 'OAKA (Spyros Louis)'),
  ('org_3fcca179c798a85e8ab0', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΑΙΟΝΙΑΣ', '100060138', 'Paionia Municipal Water Supply and Sewerage Company'),
  ('org_521a68175edb0f0b2b64', 'ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΝΙΓΡΙΤΑΣ', '100031209', 'Nigrita Detention Centre'),
  ('org_0719f94b19ebf0777707', 'ΑΓΡΟΤΙΚΗ ΦΥΛΑΚΗ ΚΑΣΣΑΝΔΡΑΣ', '99200381', 'Kassandra Agricultural Prison'),
  ('org_264a3f8b0b4205ad0cbe', 'ΕΛΛΗΝΙΚΟ ΑΝΟΙΚΤΟ ΠΑΝΕΠΙΣΤΗΜΙΟ', '99203044', 'Hellenic Open University'),
  ('org_c13e0baf2dd02e63e2fe', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΙΟΥ', '50660', 'Ios Municipal Port Fund'),
  ('org_bd0c3747461369466516', 'ΠΕΡΙΦΕΡΕΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΦΟΡΕΩΝ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ (ΦΟΔΣΑ) ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', '55416', 'Regional Association of Solid Waste Management Bodies (FODSA) of Central Macedonia'),
  ('org_88e3f0bafacb7032e443', 'ΔΗΜΟΤΙΚΗ  ΕΠΙΧΕΙΡΗΣΗ  ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΤΟΛΕΜΑΙΔΑΣ', '52086', 'Ptolemaida Municipal Water Supply and Sewerage Company'),
  ('org_ff5bb596e5126e6cc9fd', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΜΥΚΟΝΟΥ', '50261', 'Mykonos Municipal Port Fund'),
  ('org_aa6f95dbac07c81a63ec', 'ΑΡΙΣΤΟΤΕΛΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΕΣ/ΝΙΚΗΣ', '99202041', 'Aristotle University of Thessaloniki'),
  ('org_0210d2ab38f034400014', 'ΣΤΑΔΙΟ ΕΙΡΗΝΗΣ ΚΑΙ ΦΙΛΙΑΣ', '99221454', 'Peace and Friendship Stadium'),
  ('org_cadd05e621c77bf162f5', 'ΔΙΑΧΕΙΡΙΣΗ ΑΠΟΡΡΙΜΜΑΤΩΝ ΝΟΜΟΥ ΧΙΟΥ Α.Ε.', '55451', 'Chios Waste Management S.A.'),
  ('org_6438ccb2ca2e08ca92b4', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΚΑΤΕΡΙΝΗΣ', '51621', 'Katerini Municipal Water Supply and Sewerage Company'),
  ('org_a0a93bc0ef727fd75ebe', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΟΖΑΝΗΣ', '50449', 'Kozani Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_898860e609a02f5d5864', 'ΔΗΜΟΚΡΙΤΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΡΑΚΗΣ', '99202868', 'Democritus University of Thrace'),
  ('org_05558dffdb460fae60fc', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ &  ΑΠΟΧΕΤΕΥΣΗΣ ΚΙΛΕΛΕΡ', '53607', 'Kileler Municipal Water Supply & Sewerage Company'),
  ('org_d23072599a93b03c2e1f', 'ΕΘΝΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΠΡΟΛΗΨΗΣ ΚΑΙ ΑΝΤΙΜΕΤΩΠΙΣΗΣ ΕΞΑΡΤΗΣΕΩΝ', '100095525', 'National Organisation for the Prevention and Treatment of Addictions'),
  ('org_4171193c2404fe8d9d83', 'ΑΝΩΤΑΤΗ ΣΧΟΛΗ ΚΑΛΩΝ ΤΕΧΝΩΝ', '99202037', 'Athens School of Fine Arts'),
  ('org_0c07837797621e4943b2', 'ΧΑΡΟΚΟΠΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ', '99219608', 'Harokopio University'),
  ('org_7ee3edb9b510e876449b', 'ΔΙΕΘΝΕΣ ΠΑΝΕΠΙΣΤΗΜΙΟ ΕΛΛΑΔΟΣ', '99202922', 'International Hellenic University'),
  ('org_d3f4a772b7a720d8a99e', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΔΗΜΟΥ ΚΕΦΑΛΛΟΝΙΑΣ', '50287', 'Municipal Water Supply and Sewerage Company (DEYA) of the Municipality of Kefalonia'),
  ('org_df87b2f576386286feee', 'ΕΙΣΑΓΓΕΛΙΑ ΕΦΕΤΩΝ ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ', '54365', 'Public Prosecutor’S Office of the North Aegean Court of Appeal'),
  ('org_34c56d80455f6f05473c', 'ΜΥΤΙΛΗΝΗ ΜΟΝΟΠΡΟΣΩΠΗ Α Ε   ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ', '100066804', 'Mytilene Single-Member S.A. Development Organisation'),
  ('org_a78a6842b336394fa0ce', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΑΒΑΛΑΣ', '51225', 'Kavala Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_64a5ae6062d0affa3fc1', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΠΕΙΡΑΙΩΣ', '99206922', 'University of Piraeus'),
  ('org_be31f320ac3412b468b7', 'ΠΑΠΑΦΕΙΟ ΚΕΝΤΡΟ ΠΑΙΔΙΚΗΣ ΜΕΡΙΜΝΑΣ', '99221987', 'Papafeio Child Welfare Centre'),
  ('org_2b83794cca39d51971af', 'ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)', 'σύνδεσμος::ΣΥΝΔΕΣΜΟΣ ΠΡΟΣΤΑΣΙΑΣ & ΑΝΑΠΤΥΞΗΣ ΥΜΗΤΤΟΥ (ΣΠΑΥ)', 'Ymittos Protection & Development Association (SPAY)'),
  ('org_f5dcc01e8b1e9096b75c', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)', 'σύνδεσμος::ΑΝΑΠΤΥΞΙΑΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΥΤΙΚΗΣ ΑΘΗΝΑΣ (ΑΣΔΑ)', 'Development Association of Western Athens (ASDA)'),
  ('org_60016a060910f28da8b2', 'ΜΑΚΕΔΟΝΙΑΣ – ΘΡΑΚΗΣ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΜΑΚΕΔΟΝΙΑΣ – ΘΡΑΚΗΣ', 'Macedonia – Thrace'),
  ('org_ce03104eb1084346ebd2', 'ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ::ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ', 'Peloponnese, Western Greece and the Ionian Islands'),
  ('org_08461bb6e70f886494db', 'ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ::ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)', 'Protection and Regeneration of Mount Penteli (SPAP)'),
  ('org_b3ef15597189461cc6ee', 'ΚΕΝΤΡΟ ΔΙΑΦΥΛΑΞΗΣ ΑΓΙΟΡΕΙΤΙΚΗΣ ΚΛΗΡΟΝΟΜΙΑΣ (Κ.Δ.Α.Κ.)', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΚΕΝΤΡΟ ΔΙΑΦΥΛΑΞΗΣ ΑΓΙΟΡΕΙΤΙΚΗΣ ΚΛΗΡΟΝΟΜΙΑΣ (Κ.Δ.Α.Κ.)', 'Centre for the Preservation of the Monastic Heritage of Mount Athos (K.D.A.K.)'),
  ('org_d8b0ae9261cbdf481c9b', 'ΠΑΙΔΕΙΑΣ, ΘΡΗΣΚΕΥΜΑΤΩΝ ΚΑΙ ΑΘΛΗΤΙΣΜΟΥ', 'ΥΠΟΥΡΓΕΙΟ::ΠΑΙΔΕΙΑΣ, ΘΡΗΣΚΕΥΜΑΤΩΝ ΚΑΙ ΑΘΛΗΤΙΣΜΟΥ', 'Ministry of Education, Religious Affairs and Sports'),
  ('org_84e61ddad1badc3719f6', 'ΑΝΑΠΤΥΞΙΑΚΗ ΠΡΟΟΠΤΙΚΗ Μ.Α.Ε. - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΥ ΓΑΛΑΤΣΙΟΥ Α.Ε.', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ::ΑΝΑΠΤΥΞΙΑΚΗ ΠΡΟΟΠΤΙΚΗ Μ.Α.Ε. - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΔΗΜΟΥ ΓΑΛΑΤΣΙΟΥ Α.Ε.', 'Development Perspective Single-member S.A. – Galatsi Municipality Development Organisation S.A.'),
  ('org_13a8ed472a5b78f60fa5', 'ΚΑΒΑΛΑΣ-ΔΗ.ΠΕ.ΘΕ. ΚΑΒΑΛΑΣ', 'ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ::ΚΑΒΑΛΑΣ-ΔΗ.ΠΕ.ΘΕ. ΚΑΒΑΛΑΣ', 'Kavala – Municipal Regional Theatre of Kavala'),
  ('org_258a3a467fff7bdc7a96', 'ΕΘΝΙΚΟ ΣΥΣΤΗΜΑ ΥΠΟΔΟΜΩΝ ΠΟΙΟΤΗΤΑΣ (Ε.Σ.Υ.Π.)', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΕΘΝΙΚΟ ΣΥΣΤΗΜΑ ΥΠΟΔΟΜΩΝ ΠΟΙΟΤΗΤΑΣ (Ε.Σ.Υ.Π.)', 'National Quality Infrastructure System (ESYP)'),
  ('org_378ef9dc43fbfc0e98d9', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΛΑΥΡΙΟΥ Α.Ε', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΛΑΥΡΙΟΥ Α.Ε', 'Lavrio Port Authority S.A.'),
  ('org_38a716914f10d0030728', 'Δ.Ε.Υ.Α ΧΕΡΣΟΝΗΣΟΥ ΚΡΗΤΗΣ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ::Δ.Ε.Υ.Α ΧΕΡΣΟΝΗΣΟΥ ΚΡΗΤΗΣ', 'DEYA Hersonissos, Crete'),
  ('org_9f8e174be1f90496a8bb', 'Γ.Ν. ΛΑΣΙΘΙΟΥ', '99221888', 'Lasithi General Hospital'),
  ('org_a790680f4e56dd57f865', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΕΡΚΥΡΑΣ', '50308', 'Corfu Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_674388d1b9b9cfcc777b', 'ΗΔΙΚΑ Α.Ε (ΗΛΕΚΤΡΟΝΙΚΗ ΔΙΑΚΥΒΕΡΝΗΣΗ - ΚΟΙΝΩΝΙΚΗ ΑΣΦΑΛΙΣΗ Α.Ε)', '99219610', 'IDIKA S.A. – Electronic Governance and Social Security S.A.'),
  ('org_8cb4d97744c0edf658ac', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΚΕΡΚΥΡΑΣ Α.Ε', '99221107', 'Corfu Port Authority S.A.'),
  ('org_c3611923c2422c8984ab', 'ΔΗΜΟΤΙΚΕΣ ΕΠΙΧΕΙΡΗΣΕΙΣ ΡΟΔΟΥ ΜΟΝΟΜΕΤΟΧΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΚΑΙ ΔΙΑΚΡΙΤΙΚΟ ΤΙΤΛΟ «Δ.Ε.Ρ.Μ.Α.Ε.»', '52681', 'Rhodes Municipal Enterprises, A Single-Shareholder Public Limited Company, with the Distinguishing Title ‘D.E.R.M.A.E.’'),
  ('org_6d9fefc301c633f1f611', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΠΑΡΟΥ', '50286', 'Paros Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_9213681e9a3389a4b195', 'ΚΕΝΤΡΙΚΗ ΑΓΟΡΑ ΘΕΣ/ΝΙΚΗΣ', '99221070', 'Thessaloniki Central Market'),
  ('org_7dae8576f3639af127aa', 'Δ.Ε.Υ.Α ΘΗΡΑΣ', '51102', 'DEYA Thira'),
  ('org_9d21c9a29c80cfd812a7', 'ΔΥΠΕ   Α''  ΑΤΤΙΚΗΣ', '99221492', '1st Regional Health Authority of Attica'),
  ('org_f4c527e56bdbc0170b0a', 'ΕΛΛΗΝΙΚΟ ΦΕΣΤΙΒΑΛ Α.Ε', '99221314', 'Hellenic Festival S.A.'),
  ('org_378ef9dc43fbfc0e98d9', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ  ΛΑΥΡΙΟΥ Α.Ε', '99221099', 'Lavrio Port Authority S.A.'),
  ('org_637c16d30dcbe7c3d0f9', 'ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΚΙΝΗΤΩΝ ΙΩΑΝΝΙΝΩΝ  (ΔΗ.ΑΝ.ΕΤ.Α.Ι.)', '55124', 'Ioannina Municipal Real Estate S.A. (DIANETAI)'),
  ('org_40b9b2738d3cf994e344', 'Δ.Ε.Υ.Α ΚΙΛΚΙΣ', '52460', 'DEYA Kilkis'),
  ('org_77265364db0b009ade6c', 'ΥΠΟΥΡΓΕΙΟ ΠΑΙΔΕΙΑΣ, ΘΡΗΣΚΕΥΜΑΤΩΝ ΚΑΙ ΑΘΛΗΤΙΣΜΟΥ', '100081880', 'Ministry of Education, Religious Affairs and Sports'),
  ('org_9cce4da8a319b9fc8fb2', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΛΕΣΒΟΥ', '53187', 'Lesvos Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_e478d5db215515c10103', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ ΓΙΑ ΤΗΝ ΠΡΟΣΤΑΣΙΑ ΚΑΙ ΑΝΑΠΛΑΣΗ ΤΟΥ ΠΕΝΤΕΛΙΚΟΥ (Σ.Π.Α.Π.)', '53355', 'Association of Municipalities for the Protection and Regeneration of Mount Penteli (SPAP)'),
  ('org_6a01f682cf4532cf3e1e', 'ΔΙΑΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ Δ.Ε.ΔΙ.Σ.Α. Α.Ε. Ο.Τ.Α.', '50564', 'Inter-municipal Solid Waste Management Company (DEDISA) S.A. – Local Authorities'),
  ('org_b003d589389529582435', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΕΛΕΥΣΙΝΟΣ Α.Ε', '99221102', 'Elefsina Port Authority S.A.'),
  ('org_709878af40365e3e9d63', 'Ο.ΣΥ. Α.Ε. - ΟΔΙΚΕΣ ΣΥΓΚΟΙΝΩΝΙΕΣ Α.Ε.', '53362', 'OSY S.A. – Road Transport S.A.'),
  ('org_514cba7068d31298662e', 'ΒΙΟΤΕΧΝΙΚΟ ΕΠΙΜΕΛ. ΑΘΗΝΩΝ', '99220961', 'Athens Chamber of Small Industries'),
  ('org_3fb9e4515821af770641', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΕΔΕΣΣΑΣ', '50610', 'Edessa Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_17141b2c332e59c98dac', 'ΒΙΟΤΕΧΝΙΚΟ ΕΠΙΜΕΛ. ΘΕΣ/ΝΙΚΗΣ', '99220962', 'Thessaloniki Chamber of Handicrafts'),
  ('org_39e4b90e92d90d2ded13', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΜΥΚΟΝΟΥ', '50351', 'Mykonos Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_d4a570241e4f14f674ca', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ', '50743', 'Alexandroupolis Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_02ce9baf86b124eab31d', 'ΛΟΥΤΡΑ ΛΟΥΤΡΑΚΙΟΥ ΔΗΜΟΥ ΑΛΜΩΠΙΑΣ Α.Ε', '51974', 'Loutraki Baths of the Municipality of Almopia S.A.'),
  ('org_b833a086b9c932069b0a', 'ΣΤΑΘΕΡΕΣ ΣΥΓΚΟΙΝΩΝΙΕΣ Α.Ε. (ΣΤΑΣΥ)', '50083', 'STASY S.A. – Urban Rail Transport'),
  ('org_fbea73c6b5ddf0230936', 'ΜΟΥΣΕΙΟ ΦΥΣΙΚΗΣ ΙΣΤΟΡΙΑΣ ΑΠΟΛΙΘΩΜΕΝΟΥ ΔΑΣΟΥΣ ΛΕΣΒΟΥ (Μ.Φ.Ι.Α.Λ.)', '99221412', 'Natural History Museum of the Lesvos Petrified Forest (MFIAL)'),
  ('org_3fe014f76f97442dac09', 'ΠΕΡ.ΓΕΝ. ΝΟΣΟΚΟΜΕΙO ΑΘΗΝΩΝ ''ΙΠΠΟΚΡΑΤΕΙΟ''', '99221988', 'Athens Regional General Hospital “Ippokrateio”'),
  ('org_a9bd49c84757180befd5', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΔΗΜΟΥ ΡΟΔΟΥ (Δ.Ε.Υ.ΑΡ)', '52826', 'Municipal Water Supply and Sewerage Company of the Municipality of Rhodes (DEYAR)'),
  ('org_5d3c4f66fd3be9a42058', 'ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΠΑΤΡΩΝ', '99222013', 'Regional University General Hospital of Patras'),
  ('org_a8a6f5fd2cf259996c95', 'ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΛΑΡΙΣΑΣ', '99222012', 'Regional University General Hospital of Larissa'),
  ('org_4ed1dc440243d6443b54', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΣΕΡΡΩΝ (Δ.Ε.Υ.ΑΣ)', '50621', 'Serres Municipal Water Supply and Sewerage Company (DEYAS)'),
  ('org_068169712ad2235aa394', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΠΑΤΡΩΝ  Α.Ε', '99221108', 'Patras Port Authority S.A.'),
  ('org_9019162c86e859b38df7', 'ΑΝΑΠΤΥΞΙΑΚΗ ΑΘΗΝΑΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ - ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ Α.Ε.', '50384', 'Athens Development S.A. – Local Authority Development Organisation S.A.'),
  ('org_722bdf57ba41fc0e4acf', 'ΜΟΝΟΜΕΤΟΧΙΚΗ ΑΞΙΟΠΟΙΗΣΗΣ ΑΚΙΝΗΤΩΝ ΔΗΜΟΥ ΧΙΟΥ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ Ο.Τ.Α. ΔΙΑΧΕΙΡΗΣΗ ΑΚΙΝΗΤΩΝ', '54788', 'Chios Municipal Property Development Single-shareholder S.A. – Local Authorities – Property Management'),
  ('org_b98956b6dae88913364a', 'ΦΟΡΕΑΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ ΠΕΡΙΦΕΡΕΙΑΣ ΘΕΣΣΑΛΙΑΣ -ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ Ο.Τ.Α. Α.Ε.', '100051242', 'Solid Waste Management Authority of the Region of Thessaly – Local Authority Public Limited Company S.A.'),
  ('org_62b9f87d0ca44b46ee9e', 'ΠΑΡΚΟ ΠΑΡΟΥ ΔΗΜΟΤΙΚΗ Α.Ε.', '54924', 'Paros Park Municipal S.A.'),
  ('org_f09a5e617b10c257a649', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΠΕΛΟΠΟΝΝΗΣΟΥ, ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ ΚΑΙ ΙΟΝΙΟΥ', '50202', 'Decentralised Administration of the Peloponnese, Western Greece and the Ionian Islands'),
  ('org_13c25dd0f138e65ebbfe', 'ΑΡΙΣΤΟΤΕΛΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ ΘΕΣ/ΝΙΚΗΣ', '99202041', 'Aristotle University of Thessaloniki'),
  ('org_be69ec8d2f56fcac6bec', 'ΔΙΑΧΕΙΡΙΣΗ ΑΠΟΡΡΙΜΜΑΤΩΝ ΝΟΜΟΥ ΧΙΟΥ Α.Ε.', '55451', 'Chios Waste Management S.A.'),
  ('org_269df5bc1326563167a7', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΟΖΑΝΗΣ', '50449', 'Kozani Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_f40411469b4812476a1d', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΔΗΜΟΥ ΚΕΦΑΛΛΟΝΙΑΣ', '50287', 'Municipal Water Supply and Sewerage Company (DEYA) of the Municipality of Kefalonia'),
  ('org_efa2a758ce66e9c853a6', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΚΑΒΑΛΑΣ', '51225', 'Kavala Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_28840498ff810951c9c5', 'ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.', 'ΝΠΙΔ::ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.', 'Ionian Development – Development Organisation of Local Authorities of the Ionian Islands Region (AOPIN) S.A.'),
  ('org_c5497f1ff31bfe06c167', 'ΚΕΡΚΥΡΑΣ ΔΗ.ΠΕ.ΘΕ.Κ.', 'ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ::ΚΕΡΚΥΡΑΣ ΔΗ.ΠΕ.ΘΕ.Κ.', 'Corfu Municipal Regional Theatre (DIPETHEK)'),
  ('org_fc88ab10fbbbb37f6560', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΘΕΡΜΑΙΚΟΥ', '50285', 'Thermaikos Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_0bad364d5e34cc05d88c', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΜΕΤΕΩΡΩΝ', '100063177', 'Meteora Municipal Water Supply and Sewerage Company'),
  ('org_6c601d089f232057d920', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΣΙΘΩΝΙΑΣ', '100012572', 'Sithonia Municipal Port Fund'),
  ('org_4c833ee10110ea5c841a', 'Δ.Ε.Υ.Α ΠΡΕΒΕΖΑΣ', '52188', 'DEYA Preveza'),
  ('org_5a66541dce4477249738', 'ΔΗΜΟΤΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΠΡΟΝΟΙΑΣ ΔΗΜΟΥ ΡΟΔΟΥ', '50416', 'Municipal Welfare Organisation of the Municipality of Rhodes'),
  ('org_958dedbc96466911ffaf', 'ΚΟΙΝΩΦΕΛΗΣ ΔΗΜ. ΕΠΙΧΕΙΡΗΣΗ ΠΟΛΙΤΙΣΤΙΚΗΣ ΑΝΑΠΤΥΞΗΣ ΔΗΜΟΥ ΠΕΡΙΣΤΕΡΙΟΥ ΔΕΠΑΔΠ', '50894', 'Public Benefit Municipal Cultural Development Enterprise of the Municipality of Peristeri (DEPADP)'),
  ('org_127f5639bac32a1e5fa2', 'ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ ΚΕΡΚΥΡΑΣ ΔΗ.ΠΕ.ΘΕ.Κ.', '54457', 'Corfu Municipal Regional Theatre (DIPETHEK)'),
  ('org_36c9e677eb4c0c6ad9b9', 'ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΕΚΜΕΤΑΛΕΥΣΗΣ ΠΕΡΙΟΥΣΙΑΣ ΧΑΛΚΙΔΑΣ Δ.Α.Ε.Π.Ε. ΧΑΛΚΙΔΑ Α.Ε.', '52850', 'Municipal Public Limited Company for the Management of Chalkida’s Assets (D.A.E.P.E. Chalkida S.A.)'),
  ('org_0f754fb2b5024617228d', 'ΔΗΜΟΤΙΚΟ ΜΟΥΣΕΙΟ ΚΑΛΑΒΡΥΤΙΝΟΥ ΟΛΟΚΑΥΤΩΜΑΤΟΣ', '54977', 'Kalavryta Holocaust Municipal Museum'),
  ('org_ca5c828141033317e862', 'ΑΝΑΠΤΥΞΗ ΑΘΛΗΤΙΣΜΟΥ ΗΡΑΚΛΕΙΟΥ - ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΟΤΑ', '52379', 'Heraklion Sports Development – Local Authority Public Limited Company'),
  ('org_bc7c4aff98b8280f4c6c', 'ΜΟΝΟΜΕΤΟΧΙΚΗ ΑΝΩΝΥΜΗ ΔΗΜΟΤΙΚΗ ΕΤΑΙΡΕΙΑ ΔΗΜΟΥ ΙΤΑΝΟΥ (Μ.Α.Δ.Ε.Δ.Ι. Α.Ε.)', '50622', 'Single-Shareholder Municipal Public Limited Company of the Municipality of Itanos (M.A.D.E.D.I. S.A.)'),
  ('org_b16e4c9c273db4af702e', 'ΕΘΝΙΚΟ ΑΘΛΗΤΙΚΟ ΠΡΟΠΟΝΗΤΙΚΟ ΚΕΝΤΡΟ ΟΛΥΜΠΙΑΚΟΥ ΧΩΡΙΟΥ', '100070463', 'National Sports Training Centre of the Olympic Village'),
  ('org_070af9b65aae73365225', 'ΕΛΛΗΝΙΚΑ ΤΑΧΥΔΡΟΜΕΙΑ Α.Ε', '50039', 'Hellenic Post S.A.'),
  ('org_0e30256de25a7c968a4b', 'ΜΟΥΣΕΙΟ ΑΚΡΟΠΟΛΗΣ (Ν.3711/2008)', '99221400', 'ACROPOLIS MUSEUM (Law 3711/2008)'),
  ('org_3d5ca4b72cac8399fd9d', 'ΑΝΑΓΚΑΣΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ 2ΗΣ ΔΙΑΧΕΙΡΙΣΤΙΚΗΣ ΕΝΟΤΗΤΑΣ ΔΗΜΟΥ ΣΤΡΑΤΟΥ', '51891', 'Compulsory Solid Waste Management Association of the 2nd Management Unit of the Municipality of Stratos'),
  ('org_47843a0e29e05c4adc47', 'ΠΡΟΕΔΡΙΑ ΤΗΣ ΔΗΜΟΚΡΑΤΙΑΣ', '100014144', 'Presidency of the Republic'),
  ('org_b062ee4045b744cc24a3', 'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ', '55406', 'Social Welfare Centre of the Region of Eastern Macedonia and Thrace'),
  ('org_d0f027b84f67dd9dcc20', 'ΑΝΑΓΚΑΣΤΙΚΟΣ ΣΥΝΔΕΣΜΟΣ ΔΙΑΧ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ 3ΗΣ ΔΙΑΧ ΕΝΟΤΗΤΑΣ ΤΟΥ ΝΟΜΟΥ ΑΙΤΩΛΟΑΚΑΡΝΑΝΙΑΣ ΠΕΡΙΦ ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ', '54752', 'Compulsory Solid Waste Management Association of the 3rd Management Unit of Aetoloakarnania Prefecture, Western Greece Region'),
  ('org_587e5e21643a436ecd48', 'ΥΠΟΥΡΓΕΙΟ ΔΙΚΑΙΟΣΥΝΗΣ', '5', 'Ministry of Justice'),
  ('org_29c107dc85461edfda95', 'ΕΚΕΦΕ ΔΗΜΟΚΡΙΤΟΣ', '99220979', 'National Centre for Scientific Research “Demokritos”'),
  ('org_ab349e2ba4a1970aab16', 'ΕΛΛΗΝΙΚΟ ΙΝΣΤΙΤΟΥΤΟ ΠΑΣΤΕΡ', '99200054', 'Hellenic Pasteur Institute'),
  ('org_b97ad8a6092d0c005068', 'ΙΔΡΥΜΑ ΙΑΤΡΟΒΙΟΛΟΓΙΚΩΝ ΕΡΕΥΝΩΝ ΤΗΣ ΑΚΑΔΗΜΙΑΣ ΑΘΗΝΩΝ (ΙΙΒΕΑΑ)', '99206224', 'Biomedical Research Foundation of the Academy of Athens (IIBEAA)'),
  ('org_3107812f1f0910d776d2', 'ΕΘΝΙΚΟ ΚΕΝΤΡΟ ΑΜΕΣΗΣ ΒΟΗΘΕΙΑΣ (ΕΚΑΒ)', '50184', 'National Emergency Medical Service (EKAV)'),
  ('org_5eeaf4f3d7bafa87e305', 'ΕΛΛΗΝΙΚΗ ΑΡΧΗ ΓΕΩΛΟΓΙΚΩΝ ΚΑΙ ΜΕΤΑΛΛΕΥΤΙΚΩΝ ΕΡΕΥΝΩΝ', '100046052', 'Hellenic Authority for Geological and Mining Research'),
  ('org_32be9f10bade7ac2eca0', 'ΙΕΡΑ ΜΟΝΗ ΒΑΤΟΠΕΔΙΟΥ', '100068146', 'Holy Monastery of Vatopedi'),
  ('org_8832f4224a3726636ace', 'ΕΘΝΙΚΟ ΜΕΤΣΟΒΙΟ ΠΟΛΥΤΕΧΝΕΙΟ', '99203026', 'National Technical University of Athens'),
  ('org_4c8ea0cd4668d867d1a0', 'ΑΝΑΠΤΥΞΙΑΚΗ ΝΟΜΟΥ ΘΕΣΣΑΛΟΝΙΚΗΣ ΑΕ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΠΙΚΗΣ ΑΥΤΟΔΙΟΙΚΗΣΗΣ', '100066120', 'Thessaloniki Prefecture Development S.A. Local Authority Development Organisation'),
  ('org_f8612e654daf47ba53a7', 'ΝΟΣΗΛΕΥΤΙΚΟ ΙΔΡΥΜΑ ΜΕΤΟΧΙΚΟΥ ΤΑΜΕΙΟΥ ΣΤΡΑΤΟΥ', '50098', 'Nursing Institution of the Army Share Fund'),
  ('org_fff675d2e04dda269d18', 'ΕΛΛΗΝΙΚΗ ΕΤΑΙΡΕΙΑ ΔΙΚΑΙΗΣ ΑΝΑΠΤΥΞΙΑΚΗΣ ΜΕΤΑΒΑΣΗΣ ΑΕ ΕΛΕΔΑΜ', '100075220', 'Hellenic Company for Just Development Transition S.A. (ELEDAM)'),
  ('org_2dc62d4072c0200a8339', 'ΔΙΚΤΥΟ ΠΟΛΕΩΝ ΔΕΠΑΝ', '100035541', 'DEPAN Network of Cities'),
  ('org_cf23890d58a6c573f215', 'ΙΕΡΑ ΜΗΤΡΟΠΟΛΙΣ ΙΩΑΝΝΙΝΩΝ', '100012128', 'Holy Metropolis of Ioannina'),
  ('org_fbd17f134370cc3d9871', 'ΠΡΩΤΟΔΙΚΕΙΟ ΜΥΤΙΛΗΝΗΣ', '3405', 'Mytilene Court of First Instance'),
  ('org_837dd8dfcd4a44ac3406', 'ΔΙΟΙΚΗΤΙΚΟ ΕΦΕΤΕΙΟ ΚΟΜΟΤΗΝΗΣ', '50211', 'Komotini Administrative Court of Appeal'),
  ('org_d347370019194e4be700', 'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ', '99221970', 'Social Welfare Centre of the Ionian Islands Region'),
  ('org_3a09a93372d488013569', 'ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΜΟΝΑΔΩΝ ΥΓΕΙΑΣ Α.Ε. (ΑΕΜΥ Α.Ε.)', '50037', 'Health Units S.A. (AEMY S.A.)'),
  ('org_17a74e8838799edb4800', '5Η ΥΓΕΙΟΝΟΜΙΚΗ ΠΕΡΙΦΕΡΕΙΑ ΘΕΣΣΑΛΙΑΣ & ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ', '99221503', '5th Regional Health Authority of Thessaly & Central Greece'),
  ('org_afd2e15d8f5114aac8a9', 'ΠΑΝΕΠΙΣΤΗΜΙΟ ΙΩΑΝΝΙΝΩΝ', '99206915', 'University of Ioannina'),
  ('org_afm_997732812', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΑΓΓΑΙΟΥ', '52870', 'Pangaio Municipal Water Supply and Sewerage Company'),
  ('org_afm_997223821', 'ΜΗΤΡΟΠΟΛΙΤΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΜΟΥΣΕΙΩΝ ΕΙΚΑΣΤΙΚΩΝ ΤΕΧΝΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ', '100044187', 'Metropolitan Organisation of Fine Arts Museums of Thessaloniki'),
  ('org_afm_090052773', 'ΔΕΤΕΠΑ', '51636', 'Detepa'),
  ('org_afm_999875161', 'ΑΣΤΙΚΗ ΜΗ ΚΕΡΔΟΣΚΟΠΙΚΗ ΕΤΑΙΡΕΙΑ ΚΟΙΝΩΝΙΚΗΣ ΦΡΟΝΤΙΔΑΣ ΚΑΙ ΑΝΑΠΤΥΞΗΣ ΔΗΜΟΥ ΡΟΔΙΩΝ', '100027447', 'Civil Non-profit Company for Social Care and Development of the Municipality of Rhodes'),
  ('org_afm_090401013', 'ΕΛΛΗΝΙΚΗ ΣΤΑΤΙΣΤΙΚΗ ΑΡΧΗ (ΕΛ.ΣΤΑΤ.)', '7839', 'Hellenic Statistical Authority (ELSTAT)'),
  ('org_afm_090016497', 'ΟΡΓΑΝΙΣΜΟΣ ΛΙΜΕΝΟΣ ΗΡΑΚΛΕΙΟΥ Α.Ε', '99221104', 'Heraklion Port Authority S.A.'),
  ('org_afm_999161766', 'ΠΕΡΙΦ. ΠΑΝΕΠΙΣΤ. ΓΕΝ. ΝΟΣΟΚ. ΗΡΑΚΛΕΙΟΥ', '99222010', 'Regional University General Hospital of Heraklion'),
  ('org_afm_997992855', 'ΚΟΙΝΩΦΕΛΗΣ ΕΠΙΧΕΙΡΗΣΗ ΔΗΜΟΥ ΠΑΤΡΕΩΝ - ΚΑΡΝΑΒΑΛΙ ΠΑΤΡΑΣ (ΚΕΔΗΠ)', '53359', 'Public Benefit Enterprise of the Municipality of Patras – Patras Carnival (KEDIP)'),
  ('org_afm_094052358', 'ΕΛΛΗΝΙΚΗ ΑΕΡΟΠΟΡΙΚΗ ΒΙΟΜΗΧΑΝΙΑ (Ε.Α.Β.) Α.Ε. (N.43/1975)', '99221129', 'Hellenic Aerospace Industry (EAB) S.A. (Law 43/1975)'),
  ('org_afm_090016713', 'ΥΠΟΥΡΓΕΙΟ ΟΙΚΟΝΟΜΙΚΩΝ', '15', 'Ministry of Finance'),
  ('org_afm_090016657', 'ΜΕΤΟΧΙΚΟ ΤΑΜΕΙΟ ΝΑΥΤΙΚΟΥ', '99200367', 'Navy Share Fund'),
  ('org_afm_090016485', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΡΕΘΥΜΝΟΥ', '50273', 'Rethymno Municipal Port Fund'),
  ('org_afm_997204911', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΔΗΜΟΥ ΔΕΛΦΩΝ', '100034521', 'Delphi Municipality Water Supply and Sewerage Utility'),
  ('org_afm_997003338', 'ΚΟΙΝΩΝΙΚΟ ΕΛΛΗΝΙΚΟ ΚΛΙΜΑΚΙΟ ΑΜΕΣΗΣ ΒΟΗΘΕΙΑΣ', '100051919', 'Hellenic Social Emergency Aid Unit'),
  ('org_afm_090078395', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΛΙΒΑΔΕΙΑΣ (Δ.Ε.Υ.ΑΛ)', '50995', 'Livadeia Municipal Water Supply and Sewerage Company (DEYAL)'),
  ('org_afm_997631251', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΔΗΜΟΥ ΔΕΛΤΑ', '100006552', 'Delta Municipality Water Supply and Sewerage Utility'),
  ('org_afm_090073295', 'Ε.Π.Ι. ΨΥΧΙΚΗΣ ΥΓΙΕΙΝΗΣ - ΠΑΝ/ΜΙΟ ΑΘΗΝΩΝ', '99203011', 'EPI Mental Health – University of Athens'),
  ('org_afm_090093560', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΑΓΙΟΥ ΝΙΚΟΛΑΟΥ (Δ.Ε.Υ.Α.Α.Ν.)', '51130', 'Agios Nikolaos Municipal Water Supply and Sewerage Company (D.E.Y.A.A.N.)'),
  ('org_afm_997724037', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΚΥΜΗΣ-ΑΛΙΒΕΡΙΟΥ ( Δ.Ε.Υ.Α.Κ.Α. )', '54150', 'Kymi-Aliveri Municipal Water Supply and Sewerage Company (DEYAKA)'),
  ('org_afm_090127530', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΜΕΣΟΛΟΓΓΙΟΥ (Δ.Ε.Υ.Α.Μ.)', '51156', 'Messolonghi Municipal Water Supply and Sewerage Company (DEYAM)'),
  ('org_afm_996454791', 'ΕΛΛΗΝΙΚΟ ΚΕΝΤΡΟ ΚΙΝΗΜΑΤΟΓΡΑΦΟΥ ΟΠΤΙΚΟΑΚΟΥΣΤΙΚΩΝ ΜΕΣΩΝ ΚΑΙ ΔΗΜΙΟΥΡΓΙΑΣ Α Ε', '100093061', 'Hellenic Centre for Cinema, Audiovisual Media and Creativity S.A.'),
  ('org_afm_899966473', 'ΕΠΙΤΡΟΠΗ ΟΛΥΜΠΙΩΝ ΚΑΙ ΖΑΠΠΕΙΟΥ ΚΛΗΡΟΔΟΤΗΜΑΤΟΣ', '100102586', 'Committee of the Olympic and Zappeion Bequest'),
  ('org_afm_999174184', 'ΣΥΛΛΟΓΟΣ ΑΤΟΜΩΝ ΜΕ ΑΝΑΠΗΡΙΕΣ ΓΟΝΕΩΝ Κ ΦΙΛΩΝ ΝΟΤΙΑΣ ΚΡΗΤΗΣ', '100083373', 'Association of People with Disabilities, Parents and Friends of Southern Crete'),
  ('org_afm_090169846', 'ΕΙΔΙΚΟ ΚΑΤΑΣΤΗΜΑ ΚΡΑΤΗΣΗΣ ΝΕΩΝ ΒΟΛΟΥ', '99200404', 'Volos Special Youth Detention Centre'),
  ('org_afm_090169846', 'ΚΛΕΙΣΤΗ ΚΕΝΤΡΙΚΗ ΦΥΛΑΚΗ ΓΥΝΑΙΚΩΝ', '99200522', 'Closed Central Women’S Prison'),
  ('org_afm_090180273', 'ΟΡΓΑΝΙΣΜΟΣ ΜΕΓΑΡΟΥ ΜΟΥΣΙΚΗΣ ΘΕΣΣΑΛΟΝΙΚΗΣ', '100022203', 'Thessaloniki Concert Hall Organisation'),
  ('org_afm_090088051', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΚΟΜΟΤΗΝΗΣ', '50414', 'Komotini Municipal Water Supply and Sewerage Company'),
  ('org_afm_998010303', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΠΕΛΛΑΣ', '52823', 'Pella Municipal Water Supply and Sewerage Company'),
  ('org_afm_090016100', 'ΚΕΝΤΡΟ ΕΚΠ/ΣΗΣ ΚΑΙ ΑΠΟΚΑΤΑΣΤΑΣΗΣ ΤΥΦΛΩΝ (ΚΕΑΤ)', '99221632', 'Training and Rehabilitation Centre for the Blind (KEAT)'),
  ('org_afm_099170002', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΒΟΡΕΙΑΣ ΚΥΝΟΥΡΙΑΣ', '55180', 'Northern Kynouria Municipal Port Fund'),
  ('org_afm_998793274', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΣΗΤΕΙΑΣ', '55230', 'Sitia Municipal Port Fund'),
  ('org_afm_090153086', 'ΟΡΓΑΝΙΣΜΟΣ ΑΝΤΙΣΕΙΣΜΙΚΟΥ ΣΧΕΔΙΑΣΜΟΥ Κ ΠΡΟΣΤΑΣΙΑΣ', '99201071', 'Earthquake Planning and Protection Organisation'),
  ('org_afm_999147077', 'ΦΟΡΕΑΣ ΔΙΑΧΕΙΡΙΣΗΣ ΣΤΕΡΕΩΝ ΑΠΟΒΛΗΤΩΝ ΠΕΡΙΦΕΡΕΙΑΣ ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΟΤΑ Α.Ε.', '100100968', 'North Aegean Region Solid Waste Management Authority, Local Authority Public Limited Company'),
  ('org_afm_999478710', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΝΑΥΠΛΙΟΥ', '50275', 'Nafplio Municipal Port Fund'),
  ('org_afm_090157872', 'ΕΘΝΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΦΑΡΜΑΚΩΝ (ΕΟΦ)', '99221514', 'National Organisation for Medicines (EOF)'),
  ('org_afm_996844260', 'ΕΛΛΗΝΙΚΟ ΜΕΣΟΓΕΙΑΚΟ ΠΑΝΕΠΙΣΤΗΜΙΟ', '100046889', 'Hellenic Mediterranean University'),
  ('org_afm_099180289', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ-ΑΠΟΧΕΤΕΥΣΗΣ ΜΕΣΣΗΝΗΣ', '51825', 'Messini Municipal Water Supply and Sewerage Company'),
  ('org_afm_090151530', 'ΘΕΣΣΑΛΙΚΗ ΕΤΑΙΡΕΙΑ ΨΥΧΙΚΗΣ ΥΓΕΙΑΣ ΠΕΡΙΘΑΛΨΗΣ ΑΠΟΚΑΤΑΣΤΑΣΗΣ', '100090709', 'Thessalian Mental Health, Care and Rehabilitation Company'),
  ('org_afm_999696736', 'Π.Γ.Ν. «ΑΤΤΙΚΟΝ» - Γ.Ν.Δ.Α. «Η ΑΓΙΑ ΒΑΡΒΑΡΑ» ΟΡΓΑΝΙΚΗ ΜΟΝΑΔΑ ΕΔΡΑΣ Π.Γ.Ν. «ΑΤΤΙΚΟΝ»', '99221986', '“Attikon” University General Hospital – “Agia Varvara” General Hospital of Western Athens: Headquarters Unit of “Attikon” University General Hospital'),
  ('org_afm_090059670', 'ΤΟΥΡΙΣΤΙΚΕΣ ΕΠΙΧΕΙΡΗΣΕΙΣ ΤΟΡΩΝΗΣ ΔΗΜΟΤΙΚΗ Α.Ε.', '52383', 'Toroni Municipal Tourism Enterprises S.A.'),
  ('org_afm_090169846', 'ΔΙΚΑΣΤIΚΗ ΦΥΛΑΚΗ ΧΑΝΙΩΝ', '99200394', 'Chania Court Prison'),
  ('org_afm_997841008', 'ΜΑΚΕΔΝΟΣ ΑΕ  - ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΠΑΡΑΓΩΓΗΣ ΑΔΡΑΝΩΝ ΥΛΙΚΩΝ ΚΑΙ ΑΞΙΟΠΟΙΗΣΗΣ ΤΗΣ ΠΕΡΙΟΥΣΙΑΣ ΤΟΥ ΔΗΜΟΥ ΚΑΣΤΟΡΙΑΣ', '53191', 'Makednos SA - Municipal Public Limited Company for the Production of Aggregates and the Management of the Assets of the Municipality of Kastoria'),
  ('org_afm_090360708', 'ΕΠΙΤΡΟΠΗ ΕΓΧΩΡΙΟΥ ΠΕΡΙΟΥΣΙΑΣ ΚΥΘΗΡΩΝ ΚΑΙ ΑΝΤΙΚΥΘΗΡΩΝ', '50216', 'Committee for Local Property of Kythira and Antikythira'),
  ('org_afm_090167877', 'ΕΘΝΙΚΗ ΕΠΙΤΡΟΠΗ ΤΗΛΕΠΙΚΟΙΝΩΝΙΩΝ & ΤΑΧΥΔΡΟΜΕΙΩΝ (ΕΕΤΤ)', '7726', 'Hellenic Telecommunications and Post Commission (EETT)'),
  ('org_afm_099120810', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΑΙΓΙΝΑΣ', '52768', 'Aegina Municipal Port Fund'),
  ('org_afm_090162593', 'ΕΜΠ ΕΡΕΥΝ ΠΑΝΕΠ ΙΝΣΤ ΣΥΣΤΗΜ ΕΠΙΚΟΙΝΩΝΙΩΝ Κ ΥΠΟΛΟΓΙΣΤΩΝ', '100018414', 'Institute of Communication and Computer Systems – National Technical University of Athens'),
  ('org_afm_090028785', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΟΡΕΣΤΙΑΔΑΣ', '50444', 'Orestiada Municipal Water Supply and Sewerage Company'),
  ('org_afm_996972489', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΚΑΙ ΑΠΟΧΕΥΤΕΣΗΣ ΔΟΞΑΤΟΥ', '100094885', 'Doxato Municipal Water Supply and Sewerage Company'),
  ('org_afm_090101551', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΤΡΙΠΟΛΗΣ ( Δ.Ε.Υ.Α.Τ. )', '50417', 'Tripoli Municipal Water Supply and Sewerage Company (DEYAT)'),
  ('org_afm_090028804', 'ΜΟΝΟΜΕΤΟΧΙΚΗ ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΤΟΥΡΙΣΤΙΚΗΣ ΚΑΙ ΟΙΚΟΝΟΜΙΚΗΣ ΑΝΑΠΤΥΞΗΣ ΘΑΣΟΥ ''''ΘΑΣΟΣ ΔΗΜΟΤΙΚΗ ΑΕ''''', '55096', 'Single-Shareholder Municipal Public Limited Company for Tourism and Economic Development of Thasos ‘Thasos Municipal PLC’'),
  ('org_afm_099642279', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΣΙΦΝΟΥ', '50372', 'Sifnos Municipal Port Fund'),
  ('org_afm_996673084', 'ΚΟΙΝΩΦΕΛΗΣ ΕΠΙΧΕΙΡΗΣΗ ΔΗΜΟΥ ΚΑΣΣΑΝΔΡΑΣ', '52906', 'Public Benefit Enterprise of the Municipality of Kassandra'),
  ('org_afm_999388198', 'ΔΙΑΑΜΑΘ ΑΑΕ', '50255', 'DIAAMATH AAE'),
  ('org_afm_998246985', 'Δ.Ε.Υ.Α ΛΑΓΚΑΔΑ', '51508', 'DEYA Lagkadas'),
  ('org_afm_996797093', 'ΥΠΟΥΡΓΕΙΟ  ΜΕΤΑΝΑΣΤΕΥΣΗΣ ΚΑΙ ΑΣΥΛΟΥ', '100056663', 'Ministry of Migration and Asylum'),
  ('org_afm_090029757', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΧΙΟΥ', '50430', 'Chios Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_afm_094091439', 'ΔΙΩΡΥΓΑ ΚΟΡΙΝΘΟΥ ΑΕ', '99221127', 'Corinth Canal S.A.'),
  ('org_afm_999841121', 'ΕΛΛΗΝΙΚΑ ΣΙΔΗΡΟΔΡΟΜΙΚΑ ΑΚΙΝΗΤΑ  Α.Ε.', '99200111', 'Hellenic Railway Properties S.A.'),
  ('org_afm_090309792', 'ΠΑΝΘΡΑΚΙΚΟ ΕΘΝΙΚΟ ΣΤΑΔΙΟ ΚΟΜΟΤΗΝΗΣ', '99221442', 'Panthrakiko National Stadium, Komotini'),
  ('org_afm_090219485', 'ΕΘΝΙΚΗ ΒΙΒΛΙΟΘΗΚΗ ΕΛΛΑΔΟΣ', '99203019', 'National Library of Greece'),
  ('org_afm_090022770', 'ΔΗΜΟΤΙΚΟ ΓΗΡΟΚΟΜΕΙΟ ΑΡΓΟΣΤΟΛΙΟΥ', '99219997', 'Argostoli Municipal Retirement Home'),
  ('org_afm_999553370', 'ΤΟΥΡΙΣΤΙΚΗ ΜΟΝΟΜΕΤΟΧΙΚΗ ΔΗΜΟΤΙΚΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ ΑΞΙΟΠΟΙΗΣΗΣ ΔΗΜΟΤΙΚΩΝ ΑΚΙΝΗΤΩΝ (ΓΕΩΘΗΡΑ Μ.Α.Ε.) ΔΗΜΟΥ ΘΗΡΑΣ', '51278', 'Tourism Single-Shareholder Municipal Public Limited Company for the Development of Municipal Property (GEOTHIRA M.A.E.) Municipality of Thira'),
  ('org_afm_090169846', 'ΙΔΡΥΜΑ ΑΓΩΓΗΣ ΑΝΗΛΙΚΩΝ ΑΡΡΕΝΩΝ ΒΟΛΟΥ', '99200507', 'Institution for the Education of Minor Boys in Volos'),
  ('org_afm_998965076', 'Γ. ΟΓΚΟΛΟΓΙΚΟ ΝΟΣ. ΚΗΦΙΣΙΑΣ «ΟΙ ΑΓΙΟΙ ΑΝΑΡΓΥΡΟΙ»', '99221485', 'Kifissia General Oncology Hospital “Agioi Anargyroi”'),
  ('org_afm_090010364', 'ΕΛΛΗΝΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΤΟΥΡΙΣΜΟΥ (ΕΟΤ)', '99221315', 'Hellenic Tourism Organisation (EOT)'),
  ('org_afm_999461578', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΝΟΤΙΑΣ ΔΩΔΕΚΑΝΗΣΟΥ', '50359', 'Southern Dodecanese Municipal Port Fund'),
  ('org_afm_800125738', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ ΘΑΣΟΥ', '51604', 'Thasos Municipal Water Supply and Sewerage Company'),
  ('org_afm_099642231', 'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ ΜΗΛΟΥ', '50983', 'Milos Municipal Port Fund'),
  ('org_afm_090122534', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΥΔΡΕΥΣΗΣ ΑΠΟΧΕΤΕΥΣΗΣ (Δ.Ε.Υ.Α.) ΣΚΙΑΘΟΥ', '50407', 'Skiathos Municipal Water Supply and Sewerage Company (DEYA)'),
  ('org_afm_997476074', 'ΕΛΛΗΝΙΚΗ ΡΑΔΙΟΦΩΝΙΑ ΤΗΛΕΟΡΑΣΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ (Ε.Ρ.Τ. Α.Ε.)', '100012982', 'Hellenic Radio and Television S.A. (ERT S.A.)'),
  ('org_afm_094325955', 'ΕΛΛΗΝΙΚΟ ΜΕΤΡΟ ΜΟΝΟΠΡΟΣΩΠΗ ΑΕ', '99201041', 'Hellenic Metro Single-Member Public Limited Company')
) AS names (organization_key, organization_value, source_key, organization_name_en)
WHERE authority.organization_key = names.organization_key
  AND authority.organization_value = names.organization_value
  AND authority.source_key IS NOT DISTINCT FROM names.source_key
  AND authority.organization_name_en IS NULL;

UPDATE public.region AS authority
SET region_name_en = names.region_name_en
FROM (VALUES
  ('ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ', 'ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ', 'Eastern Macedonia and Thrace'),
  ('ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', 'ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', 'Central Macedonia'),
  ('ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', 'ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ', 'Western Macedonia'),
  ('ΗΠΕΙΡΟΥ', 'ΗΠΕΙΡΟΥ', 'Epirus'),
  ('ΘΕΣΣΑΛΙΑΣ', 'ΘΕΣΣΑΛΙΑΣ', 'Thessaly'),
  ('ΙΟΝΙΩΝ ΝΗΣΩΝ', 'ΙΟΝΙΩΝ ΝΗΣΩΝ', 'Ionian Islands'),
  ('ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ', 'ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ', 'Western Greece'),
  ('ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ', 'ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ', 'Central Greece'),
  ('ΑΤΤΙΚΗΣ', 'ΑΤΤΙΚΗΣ', 'Attica'),
  ('ΠΕΛΟΠΟΝΝΗΣΟΥ', 'ΠΕΛΟΠΟΝΝΗΣΟΥ', 'Peloponnese'),
  ('ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ', 'ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ', 'North Aegean'),
  ('ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ', 'ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ', 'South Aegean'),
  ('ΚΡΗΤΗΣ', 'ΚΡΗΤΗΣ', 'Crete')
) AS names (region_key, region_normalized_value, region_name_en)
WHERE authority.region_key = names.region_key
  AND authority.region_normalized_value = names.region_normalized_value
  AND authority.region_name_en IS NULL;
-- END organization and region English names (068)

-- BEGIN CPV English descriptions (069)
-- Reuse the reviewed CPV 2008 English code labels used by the app.
-- Preserve Greek source descriptions, codes, procurement links and later corrections.
ALTER TABLE public.cpv
  ADD COLUMN IF NOT EXISTS cpv_value_en TEXT;

COMMENT ON COLUMN public.cpv.cpv_value_en IS
  'Reviewed official CPV 2008 English description for cpv_key, reused from the existing app catalogue. Greek source descriptions and procurement links remain unchanged.';

UPDATE public.cpv AS cpv
SET cpv_value_en = descriptions.cpv_value_en
FROM (VALUES
  ('03111000-2', 'Σπόροι', 'Seeds'),
  ('03111100-3', 'Σπόροι σόγιας', 'Soya beans'),
  ('03114000-3', 'Άχυρα και χορτονομές', 'Straw and forage'),
  ('03121000-5', 'Φυτοκομικά προϊόντα', 'Horticultural products'),
  ('03410000-7', 'Ξύλο', 'Wood'),
  ('03419100-1', 'Προϊόντα ξυλείας', 'Timber products'),
  ('03451000-6', 'Φυτά', 'Plants'),
  ('03452000-3', 'Δένδρα', 'Trees'),
  ('09112200-9', 'Τύρφη', 'Peat'),
  ('09132100-4', 'Αμόλυβδη βενζίνη', 'Unleaded petrol'),
  ('09211000-1', 'Λιπαντικά έλαια και λιπαντικά μέσα', 'Lubricating oils and lubricating agents'),
  ('14211000-3', 'Άμμος', 'Sand'),
  ('14212000-0', 'Άμμος  λιθόκοκκοι  χαλίκι, λιθοτρίμματα και λιθόσκονη, κροκάλες, αμμοχάλικο, τεμαχισμένοι ή θρυμματισμένοι λίθοι, μείγματα αμμοχάλικου και άλλα αδρανή υλικά', 'Granules, chippings, stone powder, pebbles, gravel, broken and crushed stone, stone mixtures, sand-gravel mixtures and other aggregates'),
  ('14212110-4', 'Αμμοχάλικο', 'Pebbles'),
  ('14212400-4', 'Χώμα', 'Soil'),
  ('14622000-7', 'Χάλυβας', 'Steel'),
  ('14712000-5', 'Μόλυβδος', 'Lead'),
  ('14811300-2', 'Ακονιστικοί τροχοί', 'Grinding wheels'),
  ('15700000-5', 'Ζωοτροφές', 'Animal feedstuffs'),
  ('15710000-8', 'Παρασκευασμένες ζωοτροφές για ζώα εκτρεφόμενα σε αγροκτήματα και άλλα ζώα', 'Prepared animal feeds for farm and other animals'),
  ('15712000-2', 'Ξηρή ζωοτροφή', 'Dry fodder'),
  ('15800000-6', 'Διάφορα προϊόντα διατροφής', 'Miscellaneous food products'),
  ('16000000-5', 'Γεωργικά μηχανήματα', 'Agricultural machinery'),
  ('16150000-1', 'Κύλινδροι χλοοτάπητα ή αθλητικών γηπέδων', 'Lawn or sports-ground rollers'),
  ('16160000-4', 'Διάφορα είδη εξοπλισμού κηπουρικής', 'Miscellaneous gardening equipment'),
  ('16310000-1', 'Χορτοκοπτικές μηχανές', 'Mowers'),
  ('16311100-9', 'Χορτοκοπτικές μηχανές για χλοοτάπητες', 'Lawn, park or sports-ground mowers'),
  ('16600000-1', 'Ειδικά γεωργικά και δασοκομικά μηχανήματα', 'Specialist agricultural or forestry machinery'),
  ('16800000-3', 'Μέρη γεωργικών και δασοκομικών μηχανημάτων', 'Parts of agricultural and forestry machinery'),
  ('16820000-9', 'Μέρη δασοκομικών μηχανημάτων', 'Parts of forestry machinery'),
  ('18100000-0', 'Ρουχισμός επαγγελματικής χρήσης', 'Occupational clothing, special workwear and accessories'),
  ('18130000-9', 'Ειδικός ρουχισμός εργασίας', 'Special workwear'),
  ('18143000-3', 'Προστατευτικά εξαρτήματα', 'Protective gear'),
  ('18223200-0', 'Επενδύτες', 'Jackets'),
  ('18410000-6', 'Ειδικά ενδύματα', 'Special clothing'),
  ('18424000-7', 'Γάντια', 'Gloves'),
  ('18424500-2', 'Μακριά γάντια', 'Gauntlets'),
  ('18444000-3', 'Προστατευτικά καλύμματα κεφαλής', 'Protective headgear'),
  ('18444100-4', 'Κράνη ασφαλείας', 'Safety headgear'),
  ('18444110-7', 'Κράνη', 'Helmets'),
  ('18830000-6', 'Προστατευτικά υποδήματα', 'Protective footwear'),
  ('18831000-3', 'Υποδήματα με ενσωματωμένη προστατευτική μεταλλική μύτη', 'Footwear incorporating a protective metal toecap'),
  ('18832000-0', 'Ειδικά υποδήματα', 'Special footwear'),
  ('18930000-7', 'Σάκοι και τσάντες', 'Sacks and bags'),
  ('19211100-9', 'Υφαντά υφάσματα από ανάμεικτες ίνες', 'Mixed woven fabrics'),
  ('19520000-7', 'Πλαστικά προϊόντα', 'Plastic products'),
  ('22100000-1', 'Τυπωμένα βιβλία', 'Printed books, brochures and leaflets'),
  ('24000000-4', 'Χημικά προϊόντα', 'Chemical products'),
  ('24200000-6', 'Χρώματα και χρωστικές ουσίες', 'Dyes and pigments'),
  ('24440000-0', 'Διάφορα λιπάσματα', 'Miscellaneous fertilisers'),
  ('24951210-0', 'Σκόνη γόμωσης πυροσβεστήρων', 'Fire-extinguisher powder'),
  ('24951230-6', 'Υλικά γόμωσης πυροσβεστήρων', 'Fire-extinguisher charges'),
  ('24957000-7', 'Χημικά πρόσθετα', 'Chemical additives'),
  ('30000000-9', 'Μηχανήματα γραφείου και υπολογιστές', 'Office and computing machinery, equipment and supplies except furniture and software packages'),
  ('30193100-9', 'Οργανωτές συρταριών', 'Desk drawer organisers'),
  ('30200000-1', 'Εξοπλισμός ηλεκτρονικών υπολογιστών και προμήθειες', 'Computer equipment and supplies'),
  ('30213100-6', 'Φορητοί επιτραπέζιοι μικροϋπολογιστές', 'Portable computers'),
  ('30213200-7', 'Φορητοί υπολογιστές για την εισαγωγή χειρόγραφου κειμένου με τη χρήση γραφίδας', 'Tablet computer'),
  ('30213300-8', 'Επιτραπέζιοι ηλεκτρονικοί υπολογιστές', 'Desktop computer'),
  ('30214000-2', 'Σταθμοί εργασίας', 'Workstations'),
  ('30231000-7', 'Οθόνες και κονσόλες ηλεκτρονικών υπολογιστών', 'Computer screens and consoles'),
  ('30233000-1', 'Μονάδες αποθήκευσης και ανάγνωσης', 'Media storage and reader devices'),
  ('30233140-4', 'Συσκευές αποθήκευσης άμεσης προσπέλασης (DASD)', 'Direct-access storage devices (DASD)'),
  ('30237240-3', 'Κάμερες Ιστού', 'Web camera'),
  ('31110000-0', 'Ηλεκτρικοί κινητήρες', 'Electric motors'),
  ('31120000-3', 'Γεννήτριες', 'Generators'),
  ('31127000-2', 'Γεννήτρια εκτάκτου ανάγκης', 'Emergency generator'),
  ('31130000-6', 'Εναλλάκτες', 'Alternators'),
  ('31154000-0', 'Αδιάλειπτα τροφοδοτικά ρεύματος', 'Uninterruptible power supplies'),
  ('31161000-2', 'Μέρη ηλεκτρικών κινητήρων και γεννητριών', 'Parts for electrical motors and generators'),
  ('31200000-8', 'Συσκευές διανομής και ελέγχου ηλεκτρικού ρεύματος', 'Electricity distribution and control apparatus'),
  ('31211110-2', 'Πίνακες ελέγχου', 'Control panels'),
  ('31212000-5', 'Αποζεύκτες ισχύος', 'Circuit breakers'),
  ('31224400-6', 'Καλώδια συνδέσεων', 'Connection cables'),
  ('31440000-2', 'Μπαταρίες', 'Batteries'),
  ('31500000-1', 'Φωτιστικός εξοπλισμός και ηλεκτρικοί λαμπτήρες', 'Lighting equipment and electric lamps'),
  ('31520000-7', 'Λαμπτήρες και φωτιστικά εξαρτήματα', 'Lamps and light fittings'),
  ('31521320-3', 'Ηλεκτρικοί φορητοί φανοί', 'Torches'),
  ('31521330-6', 'Επαναφορτιζόμενοι φορητοί ηλεκτρικοί λαμπτήρες', 'Rechargeable portable electric lamps'),
  ('31524120-2', 'Φωτιστικά οροφής', 'Ceiling lights'),
  ('31524210-0', 'Φωτιστικά τοίχου', 'Wall lights'),
  ('31527200-8', 'Φωτιστικά εξωτερικών χώρων', 'Exterior lights'),
  ('31600000-2', 'Ηλεκτρολογικός εξοπλισμός', 'Electrical equipment and apparatus'),
  ('31610000-5', 'Ηλεκτρολογικός εξοπλισμός για κινητήρες και οχήματα', 'Electrical equipment for engines and vehicles'),
  ('31612300-2', 'Ηλεκτρικός εξοπλισμός σηματοδότησης για κινητήρες', 'Electrical signalling equipment for engines'),
  ('31625000-3', 'Συστήματα συναγερμού κλοπής και πυρκαγιάς', 'Burglar and fire alarms'),
  ('31625100-4', 'Συστήματα πυρανίχνευσης', 'Fire-detection systems'),
  ('31625200-5', 'Συστήματα συναγερμού πυρκαγιάς', 'Fire-alarm systems'),
  ('31625300-6', 'Συστήματα αντιδιαρρηκτικού συναγερμού', 'Burglar-alarm systems'),
  ('31680000-6', 'Προμήθειες ηλεκτρικών ειδών και εξαρτήματα', 'Electrical supplies and accessories'),
  ('31681000-3', 'Ηλεκτρολογικά εξαρτήματα', 'Electrical accessories'),
  ('31681200-5', 'Ηλεκτρικές αντλίες', 'Electric pumps'),
  ('31682210-5', 'Όργανα και εξοπλισμός ελέγχου', 'Instrumentation and control equipment'),
  ('31710000-6', 'Ηλεκτρονικός εξοπλισμός', 'Electronic equipment'),
  ('31711000-3', 'Ηλεκτρονικό υλικό', 'Electronic supplies'),
  ('31711423-4', 'Εξοπλισμός ασύρματης επικοινωνίας με μικροκύματα', 'Microwave radio equipment'),
  ('31720000-9', 'Ηλεκτρομηχανολογικός εξοπλισμός', 'Electromechanical equipment'),
  ('31730000-2', 'Ηλεκτροτεχνικός εξοπλισμός', 'Electrotechnical equipment'),
  ('32237000-3', 'Φορητά ραδιοτηλέφωνα', 'Walkie-talkies'),
  ('32250000-0', 'Κινητά τηλέφωνα', 'Mobile telephones'),
  ('32320000-2', 'Τηλεοπτικός και οπτικοακουστικός εξοπλισμός', 'Television and audio-visual equipment'),
  ('32323400-7', 'Εξοπλισμός αναπαραγωγής ταινιών βίντεο', 'Video-playback equipment'),
  ('32342410-9', 'Εξοπλισμός ήχου', 'Sound equipment'),
  ('32344210-1', 'Εξοπλισμός ασύρματης επικοινωνίας', 'Radio equipment'),
  ('32360000-4', 'Εξοπλισμός ενδοεπικοινωνίας', 'Intercom equipment'),
  ('32420000-3', 'Εξοπλισμός δικτύου', 'Network equipment'),
  ('32423000-4', 'Κόμβοι δικτύου', 'Network hubs'),
  ('32430000-6', 'Δίκτυο ευρείας ζώνης', 'Wide area network'),
  ('32441100-7', 'Τηλεμετρικό σύστημα παρακολούθησης', 'Telemetry surveillance system'),
  ('32500000-8', 'Τηλεπικοινωνιακό υλικό', 'Telecommunications equipment and supplies'),
  ('32510000-1', 'Σύστημα ασύρματης τηλεπικοινωνίας', 'Wireless telecommunications system'),
  ('32580000-2', 'Εξοπλισμός δικτύου δεδομένων', 'Data equipment'),
  ('33111620-3', 'Κάμερες ακτίνων γάμμα', 'Gamma cameras'),
  ('33735000-1', 'Δίοπτρα-προσωπίδες', 'Goggles'),
  ('33735100-2', 'Προφυλακτικά δίοπτρα', 'Protective goggles'),
  ('34113000-2', 'Αυτοκίνητα με κίνηση στους τέσσερις τροχούς', '4-wheel-drive vehicles'),
  ('34131000-4', 'Ημιφορτηγά', 'Pick-ups'),
  ('34133100-9', 'Βυτιοφόρα', 'Tankers'),
  ('34134200-7', 'Φορτηγά με ανατρεπόμενη καρότσα', 'Tipper trucks'),
  ('34142300-7', 'Ανατρεπόμενα οχήματα', 'Dumper trucks'),
  ('34144200-0', 'Οχήματα για τις υπηρεσίες εκτάκτου ανάγκης', 'Vehicles for the emergency services'),
  ('34144210-3', 'Πυροσβεστικά οχήματα', 'Firefighting vehicles'),
  ('34144212-7', 'Υδροφόρες πυροσβεστικών οχημάτων', 'Water-tender vehicles'),
  ('34144213-4', 'Πυροσβεστικές αντλίες', 'Fire engines'),
  ('34144420-8', 'Μηχανήματα διασκόρπισης αλατιού', 'Salt spreaders'),
  ('34144511-3', 'Οχήματα συμπίεσης απορριμμάτων', 'Refuse-collection vehicles'),
  ('34144512-0', 'Απορριμματοφόρα οχήματα με συμπιεστή απορριμμάτων', 'Refuse-compaction vehicles'),
  ('34144710-8', 'Τροχοφόροι φορτωτές', 'Wheeled loaders'),
  ('34144750-0', 'Οχήματα μεταφοράς φορτίων', 'Cargo carriers'),
  ('34311120-7', 'Κινητήρες εσωτερικής καύσης με ανάφλεξη μέσω συμπίεσης', 'Compression-ignition engines'),
  ('34312500-2', 'Στεγανοποιητικά παρεμβύσματα', 'Gaskets'),
  ('34515100-1', 'Σημαδούρες', 'Marker buoys'),
  ('34731000-0', 'Μέρη αεροσκαφών', 'Parts for aircraft'),
  ('34731500-5', 'Μέρη κινητήρων αεροσκαφών', 'Parts for aircraft engines'),
  ('34913000-0', 'Διάφορα ανταλλακτικά', 'Miscellaneous spare parts'),
  ('34922100-7', 'Υλικά σήμανσης οδών', 'Road markings'),
  ('34924000-0', 'Πινακίδες μεταβαλλόμενου μηνύματος', 'Variable message signs'),
  ('34928200-0', 'Περιφράξεις', 'Fences'),
  ('34928400-2', 'Αστικός εξοπλισμός', 'Urban furniture'),
  ('34928410-5', 'Πάσσαλοι οριοθέτησης', 'Marker posts'),
  ('34928460-0', 'Οδικοί κώνοι', 'Road cones'),
  ('34928471-0', 'Εξοπλισμός πινακίδων σήμανσης', 'Sign materials'),
  ('34928472-7', 'Στύλοι πινακίδων σήμανσης', 'Sign posts'),
  ('34931000-2', 'Εξοπλισμός λιμένων', 'Harbour equipment'),
  ('34946221-5', 'Λάμες', 'Switch blades'),
  ('34953000-2', 'Ράμπες πρόσβασης', 'Access ramps'),
  ('34990000-3', 'Εξοπλισμός ελέγχου', 'Control, safety, signalling and light equipment'),
  ('34991000-0', 'Φωτιστικά σώματα υπαίθριων χώρων', 'Field operating lights'),
  ('34992000-7', 'Πινακίδες και φωτεινές πινακίδες', 'Signs and illuminated signs'),
  ('35100000-5', 'Εξοπλισμός εκτάκτου ανάγκης και ασφαλείας', 'Emergency and security equipment'),
  ('35110000-8', 'Εξοπλισμός πυρόσβεσης', 'Firefighting, rescue and safety equipment'),
  ('35111000-5', 'Πυροσβεστικός εξοπλισμός', 'Firefighting equipment'),
  ('35111100-6', 'Αναπνευστικές συσκευές πυρόσβεσης', 'Breathing apparatus for firefighting'),
  ('35111200-7', 'Υλικά πυρόσβεσης', 'Firefighting materials'),
  ('35111300-8', 'Πυροκατασβεστήρες', 'Fire extinguishers'),
  ('35111310-1', 'Αφρός πλήρωσης πυροσβεστήρων', 'Foam packages'),
  ('35111320-4', 'Φορητοί πυροσβεστήρες', 'Portable fire-extinguishers'),
  ('35111400-9', 'Εξοπλισμός πυρασφάλειας', 'Fire escape equipment'),
  ('35111500-0', 'Συστήματα πυρόσβεσης', 'Fire suppression system'),
  ('35111510-3', 'Πυροσβεστικά εργαλεία χειρός', 'Fire suppression hand tools'),
  ('35111520-6', 'Πυροσβεστικός αφρός ή παρόμοιες ενώσεις', 'Fire suppression foam or similar compounds'),
  ('35112000-2', 'Εξοπλισμός διάσωσης και κινδύνου', 'Rescue and emergency equipment'),
  ('35113100-0', 'Εξοπλισμός ασφάλειας εργοταξίων', 'Site-safety equipment'),
  ('35113400-3', 'Ρούχα προστασίας και ασφαλείας', 'Protective and safety clothing'),
  ('35121000-8', 'Εξοπλισμός ασφαλείας (π.χ. έναντι κλοπής', 'Security equipment'),
  ('35121300-1', 'Εξαρτήματα ασφαλείας', 'Security fittings'),
  ('35121700-5', 'Συστήματα συναγερμού', 'Alarm systems'),
  ('35125100-7', 'Αισθητήρες', 'Sensors'),
  ('35125300-2', 'Κάμερες ασφαλείας', 'Security cameras'),
  ('35240000-8', 'Σειρήνες', 'Sirens'),
  ('35261000-1', 'Πίνακες πληροφοριών', 'Information panels'),
  ('35613000-4', 'Μη επανδρωμένα εναέρια οχήματα', 'Unmanned aerial vehicles'),
  ('35642000-7', 'Ηλεκτρονικά και ηλεκτρικά ανταλλακτικά μέρη για στρατιωτικό αεροδιαστημικό εξοπλισμό', 'Electronic and electrical spare parts for military aerospace equipment'),
  ('35810000-5', 'Ατομικός εξοπλισμός', 'Individual equipment'),
  ('35811100-3', 'Στολές πυροσβεστών', 'Fire-brigade uniforms'),
  ('35813000-6', 'Στρατιωτικά κράνη', 'Military helmets'),
  ('35813100-7', 'Καλύμματα για κράνη', 'Helmet covers'),
  ('35814000-3', 'Μάσκες αερίου', 'Gas masks'),
  ('37410000-5', 'Εξοπλισμός για αθλήματα υπαίθριων χώρων', 'Outdoor sports equipment'),
  ('37415000-0', 'Αθλητικός εξοπλισμός', 'Athletics equipment'),
  ('37416000-7', 'Εξοπλισμός αναψυχής', 'Leisure equipment'),
  ('37442900-8', 'Πολυόργανα γυμναστικής', 'Multi gyms'),
  ('37820000-2', 'Υλικά καλλιτεχνίας', 'Art supplies'),
  ('38000000-5', 'Εξοπλισμός εργαστηριακός', 'Laboratory, optical and precision equipments (excl. glasses)'),
  ('38112100-4', 'Συστήματα παγκόσμιας πλοήγησης και παγκόσμιου προσδιορισμού θέσης (GPS ή ισοδύναμα)', 'Global navigation and positioning systems (GPS or equivalent)'),
  ('38121000-9', 'Ανεμόμετρα', 'Anemometers'),
  ('38127000-1', 'Μετεωρολογικοί σταθμοί', 'Weather stations'),
  ('38291000-1', 'Συσκευές τηλεμετρίας', 'Telemetry apparatus'),
  ('38631000-7', 'Διόπτρες', 'Binoculars'),
  ('38651000-3', 'Φωτογραφικές μηχανές', 'Cameras'),
  ('38651600-9', 'Ψηφιακές κάμερες', 'Digital cameras'),
  ('38652120-7', 'Βιντεοπροβολείς', 'Video projectors'),
  ('38820000-9', 'Εξοπλισμός τηλεχειρισμού', 'Remote-control equipment'),
  ('39112000-0', 'Καρέκλες', 'Chairs'),
  ('39113300-0', 'Πάγκοι-καθίσματα', 'Bench seats'),
  ('39121200-8', 'Τραπέζια', 'Tables'),
  ('39131100-0', 'Ράφια αρχειοθέτησης', 'Archive shelving'),
  ('39141300-5', 'Ερμάρια', 'Cabinets'),
  ('39162000-5', 'Εποπτικά μέσα', 'Educational equipment'),
  ('39293400-6', 'Τεχνητός χορτοτάπητας', 'Artificial lawn'),
  ('39522530-1', 'Σκηνές', 'Tents'),
  ('39525300-1', 'Σωσίβια', 'Life jackets'),
  ('39525400-2', 'Κουβέρτες πυρόσβεσης', 'Fire blankets'),
  ('39525700-5', 'Ιμάντες διάσωσης', 'Life belts'),
  ('39541120-3', 'Σκοινιά', 'Cordage'),
  ('39714100-1', 'Εξαεριστήρες', 'Ventilators'),
  ('39715100-8', 'Ηλεκτρικοί θερμαντήρες νερού', 'Electric instantaneous or storage water heaters and immersion heaters'),
  ('39830000-9', 'Προϊόντα καθαρισμού', 'Cleaning products'),
  ('42120000-6', 'Αντλίες και συμπιεστές', 'Pumps and compressors'),
  ('42122000-0', 'Αντλίες', 'Pumps'),
  ('42122110-4', 'Αντλίες πυρόσβεσης', 'Pumps for firefighting'),
  ('42122130-0', 'Αντλίες νερού', 'Water pumps'),
  ('42123400-1', 'Αεροσυμπιεστές', 'Air compressors'),
  ('42124200-6', 'Μέρη αντλιών ή διατάξεων ανύψωσης υγρών', 'Parts of pumps or liquid elevators'),
  ('42130000-9', 'Στρόφιγγες', 'Taps, cocks, valves and similar appliances'),
  ('42131160-5', 'Δίκτυο κρουνών υδροληψίας', 'Hydrants'),
  ('42410000-3', 'Εξοπλισμός ανύψωσης και διακίνησης', 'Lifting and handling equipment'),
  ('42415320-7', 'Εξοπλισμός οχημάτων έκτακτης ανάγκης', 'Equipment for emergency vehicles'),
  ('42512300-1', 'Συγκροτήματα θέρμανσης', 'HVAC packages'),
  ('42512500-3', 'Μέρη κλιματιστικών μηχανημάτων', 'Parts of air-conditioning machines'),
  ('42600000-2', 'Εργαλειομηχανές', 'Machine tools'),
  ('42620000-8', 'Τόρνοι', 'Lathes, boring and milling machine tools'),
  ('42675100-9', 'Μέρη αλυσοπριόνων', 'Parts of chain saws'),
  ('42900000-5', 'Διάφορα μηχανήματα γενικής και ειδικής χρήσης', 'Miscellaneous general and special-purpose machinery'),
  ('42924730-5', 'Συσκευές καθαρισμού με εκτόξευση νερού υπό πίεση', 'Pressurised water cleaning apparatus'),
  ('42924740-8', 'Συσκευές καθαρισμού υψηλής πίεσης', 'High-pressure cleaning apparatus'),
  ('42961300-3', 'Σύστημα εντοπισμού οχημάτων', 'Vehicle location system'),
  ('42996200-6', 'Πρέσες αποβλήτων', 'Sewage presses'),
  ('43000000-3', 'Μηχανήματα για εξοπλισμό ορυχείων', 'Machinery for mining, quarrying, construction equipment'),
  ('43130000-3', 'Εξοπλισμός γεώτρησης', 'Drilling equipment'),
  ('43132300-0', 'Τρυπάνια', 'Drills'),
  ('43200000-5', 'Μηχανήματα χωματουργικών εργασιών και εκσκαφών και συναφή μέρη', 'Earthmoving and excavating machinery, and associated parts'),
  ('43210000-8', 'Μηχανήματα χωματουργικών εργασιών', 'Earthmoving machinery'),
  ('43262000-7', 'Μηχανήματα εκσκαφής', 'Excavating machinery'),
  ('43313000-0', 'Εκχιονιστήρες και φυσητήρες χιονιού', 'Snowploughs and snowblowers'),
  ('43313100-1', 'Εκχιονιστήρες', 'Snowploughs'),
  ('43322000-6', 'Εξοπλισμός αποσυναρμολόγησης', 'Dismantling equipment'),
  ('43323000-3', 'Εξοπλισμός άρδευσης', 'Irrigation equipment'),
  ('43324000-0', 'Εξοπλισμός αποστράγγισης', 'Drainage equipment'),
  ('43327000-1', 'Προκατασκευασμένος εξοπλισμός', 'Prefabricated equipment'),
  ('43328100-9', 'Εξοπλισμός υδραυλικών εγκαταστάσεων', 'Hydraulic equipment'),
  ('43800000-1', 'Εξοπλισμός συνεργείου', 'Workshop equipment'),
  ('44100000-1', 'Κατασκευαστικά υλικά και παρόμοια είδη', 'Construction materials and associated items'),
  ('44110000-4', 'Κατασκευαστικά υλικά', 'Construction materials'),
  ('44111000-1', 'Οικοδομικά υλικά', 'Building materials'),
  ('44111200-3', 'Τσιμέντο', 'Cement'),
  ('44111520-2', 'Θερμομονωτικά υλικά', 'Thermal insulating material'),
  ('44111700-8', 'Πλακίδια', 'Tiles'),
  ('44112200-0', 'Επενδύσεις δαπέδων', 'Floor coverings'),
  ('44112500-3', 'Υλικά οροφών', 'Roofing materials'),
  ('44113100-6', 'Υλικά πλακόστρωσης', 'Paving materials'),
  ('44113330-7', 'Υλικά επίχρισης', 'Coating materials'),
  ('44114000-2', 'Σκυρόδεμα', 'Concrete'),
  ('44114200-4', 'Προϊόντα σκυροδέματος', 'Concrete products'),
  ('44115200-1', 'Υλικά υδραυλικών εγκαταστάσεων και εγκαταστάσεων θέρμανσης', 'Plumbing and heating materials'),
  ('44115210-4', 'Υλικά υδραυλικών εγκαταστάσεων', 'Plumbing materials'),
  ('44130000-0', 'Αποχετευτικοί αγωγοί', 'Sewer mains'),
  ('44141000-0', 'Αγωγοί', 'Conduit'),
  ('44160000-9', 'Σωλαγωγοί', 'Pipeline, piping, pipes, casing, tubing and related items'),
  ('44163000-0', 'Σωληνώσεις και εξαρτήματα', 'Pipes and fittings'),
  ('44163100-1', 'Σωληνώσεις', 'Pipes'),
  ('44163210-5', 'Σφιγκτήρες αγωγών', 'Pipe clamps'),
  ('44164200-9', 'Σωλήνες', 'Tubing'),
  ('44164310-3', 'Σωλήνες και εξαρτήματα', 'Tubes and fittings'),
  ('44165100-5', 'Εύκαμπτοί σωλήνες', 'Hoses'),
  ('44167000-8', 'Διάφορα εξαρτήματα σωληνώσεων', 'Various pipe fittings'),
  ('44167110-2', 'Φλάντζες', 'Flanges'),
  ('44167300-1', '"Καμπύλες, "ταυ" και εξαρτήματα σωληνώσεων', 'Bends, tees and pipe fittings'),
  ('44175000-7', 'Φατνώματα', 'Panels'),
  ('44190000-8', 'Ποικίλα κατασκευαστικά υλικά', 'Miscellaneous construction materials'),
  ('44191000-5', 'Ποικίλα κατασκευαστικά υλικά από ξύλο', 'Miscellaneous construction materials in wood'),
  ('44200000-2', 'Δομικά υλικά', 'Structural products'),
  ('44210000-5', 'Κατασκευές και μέρη κατασκευών', 'Structures and parts of structures'),
  ('44211000-2', 'Προκατασκευασμένα κτίρια', 'Prefabricated buildings'),
  ('44212310-5', 'Ικριώματα', 'Scaffolding'),
  ('44221000-5', 'Παράθυρα, πόρτες και συναφή είδη', 'Windows, doors and related items'),
  ('44233000-2', 'Κλίμακες', 'Staircases'),
  ('44300000-3', 'Καλώδια', 'Cable, wire and related products'),
  ('44312000-0', 'Σύρμα περιφράξεων', 'Fencing wire'),
  ('44313000-7', 'Μεταλλικά πλέγματα', 'Metal netting'),
  ('44315200-3', 'Υλικά συγκόλλησης', 'Welding materials'),
  ('44316000-8', 'Σιδηρικά είδη', 'Ironmongery'),
  ('44316500-3', 'Υλικά σιδηρουργού', 'Smiths'' wares'),
  ('44320000-9', 'Καλώδια και συναφή είδη', 'Cable and related products'),
  ('44321000-6', 'Καλώδια', 'Cable'),
  ('44410000-7', 'Είδη μπάνιου και κουζίνας', 'Articles for the bathroom and kitchen'),
  ('44411100-5', 'Κρουνοί', 'Taps'),
  ('44411300-7', 'Νιπτήρες', 'Washbasins'),
  ('44411740-3', 'Λεκάνες αποχωρητηρίου', 'Lavatory bowls'),
  ('44420000-0', 'Είδη για χρήση σε κατασκευαστικά έργα', 'Goods used in construction'),
  ('44421000-7', 'Θωρακισμένα ή ενισχυμένα θησαυροφυλάκια', 'Armoured or reinforced safes, strongboxes and doors'),
  ('44423000-1', 'Διάφορα είδη', 'Miscellaneous articles'),
  ('44423340-6', 'Εξαρτήματα συρματόσχοινων για τη διακίνηση εμπορευμάτων', 'Wire-rope accessories for handling goods'),
  ('44423700-8', 'Στοιχεία φρεατίων επίσκεψης', 'Manhole elements'),
  ('44424200-0', 'Κολλητική ταινία', 'Adhesive tape'),
  ('44470000-5', 'Διάφορα προϊόντα από χυτοσίδηρο', 'Cast-iron products'),
  ('44480000-8', 'Ποικίλος εξοπλισμός πυροπροστασίας', 'Miscellaneous fire-protection equipment'),
  ('44482000-2', 'Συσκευές πυροπροστασίας', 'Fire-protection devices'),
  ('44482100-3', 'Πυροσβεστικοί υδροσωλήνες', 'Fire hoses'),
  ('44482200-4', 'Κρουνοί υδροληψίας για πυρόσβεση', 'Fire hydrants'),
  ('44500000-5', 'Εργαλεία', 'Tools, locks, keys, hinges, fasteners, chain and springs'),
  ('44511000-5', 'Εργαλεία χειρός', 'Hand tools'),
  ('44511120-2', 'Φτυάρια', 'Shovels'),
  ('44511300-8', 'Αξίνες', 'Mattocks, picks, hoes, rakes and beach rakes'),
  ('44511400-9', 'Τσεκούρια', 'Axes'),
  ('44512000-2', 'Διάφορα εργαλεία χειρός', 'Miscellaneous hand tools'),
  ('44514000-6', 'Εργαλεία χειρός και μέρη εργαλείων', 'Tool handles and tool parts'),
  ('44531000-1', 'Κοχλιωτοί συνδετήρες', 'Threaded fasteners'),
  ('44531300-4', 'Αυτοκοχλιούμενοι κοχλίες', 'Self-tapping screws'),
  ('44531700-8', 'Κοχλιωτά είδη από σίδηρο ή χάλυβα', 'Iron or steel threaded articles'),
  ('44540000-7', 'Αλυσίδες', 'Chain'),
  ('44611500-1', 'Δεξαμενές νερού', 'Water tanks'),
  ('44611600-2', 'Ταμιευτήρες', 'Reservoirs'),
  ('44613500-5', 'Βυτία νερού', 'Water containers'),
  ('44613700-7', 'Απορριμματοφόροι κάδοι', 'Refuse skips'),
  ('44614000-7', 'Κάδος', 'Casks'),
  ('44618320-4', 'Πώματα', 'Stoppers'),
  ('44619400-6', 'Βαρέλια', 'Barrels'),
  ('44800000-8', 'Βαφές', 'Paints, varnishes and mastics'),
  ('44810000-1', 'Χρώματα επίχρισης', 'Paints'),
  ('44910000-2', 'Λίθοι για κατασκευές', 'Stone for construction'),
  ('45000000-7', 'Κατασκευαστικές εργασίες', 'Construction work'),
  ('45100000-8', 'Εργασίες προπαρασκευής εργοταξίων', 'Site preparation work'),
  ('45110000-1', 'Εργασίες κατεδάφισης και αποξήλωσης εγκαταστάσεων κτιρίων και εργασίες μετακίνησης γαιών', 'Building demolition and wrecking work and earthmoving work'),
  ('45111100-9', 'Εργασίες κατεδάφισης', 'Demolition work'),
  ('45111212-7', 'Εργασίες εκβραχισμού', 'Rock-removal work'),
  ('45111213-4', 'Εργασίες εκκαθάρισης εργοταξίου', 'Site-clearance work'),
  ('45111220-6', 'Εργασίες απομάκρυνσης μπάζων', 'Scrub-removal work'),
  ('45111230-9', 'Εργασίες σταθεροποίησης εδάφους', 'Ground-stabilisation work'),
  ('45111240-2', 'Εργασίες αποστράγγισης εδάφους', 'Ground-drainage work'),
  ('45112000-5', 'Εργασίες εκσκαφών και μετακίνησης γαιών', 'Excavating and earthmoving work'),
  ('45112100-6', 'Εργασίες διάνοιξης τάφρων', 'Trench-digging work'),
  ('45112320-4', 'Εργασίες αποκατάστασης εκτάσεων γης', 'Land-reclamation work'),
  ('45112330-7', 'Εργασίες αποκατάστασης περιοχής', 'Site-reclamation work'),
  ('45112400-9', 'Εργασίες εκσκαφών', 'Excavating work'),
  ('45112420-5', 'Εργασίες εκσκαφής θεμελίων', 'Basement excavation work'),
  ('45112450-4', 'Εργασίες εκσκαφής αρχαιολογικών χώρων', 'Excavation work at archaeological sites'),
  ('45112500-0', 'Εργασίες μετακίνησης γαιών', 'Earthmoving work'),
  ('45112600-1', 'Εργασίες αφαίρεσης και πλήρωσης γαιών', 'Cut and fill'),
  ('45112700-2', 'Εργασίες διαμόρφωσης τοπίου', 'Landscaping work'),
  ('45112710-5', 'Εργασίες διαμόρφωσης τοπίου για χώρους πρασίνου', 'Landscaping work for green areas'),
  ('45112712-9', 'Εργασίες διαμόρφωσης τοπίου για κήπους', 'Landscaping work for gardens'),
  ('45200000-9', 'Εργασίες για ολόκληρες κατασκευές ή μέρη κατασκευών και έργα πολιτικού μηχανικού', 'Works for complete or part construction and civil engineering work'),
  ('45210000-2', 'Εργασίες κατασκευής κτιρίων', 'Building construction work'),
  ('45212200-8', 'Κατασκευαστικές εργασίες για αθλητικές εγκαταστάσεις', 'Construction work for sports facilities'),
  ('45212290-5', 'Επισκευή και συντήρηση αθλητικών εγκαταστάσεων', 'Repair and maintenance work in connection with sports facilities'),
  ('45212314-0', 'Κατασκευαστικές εργασίες για ιστορικά μνημεία ή μνημεία', 'Historical monument or memorial construction work'),
  ('45214200-2', 'Κατασκευαστικές εργασίες για σχολικά κτίρια', 'Construction work for school buildings'),
  ('45215213-3', 'Κατασκευαστικές εργασίες για γηροκομεία στα οποία παρέχεται και περίθαλψη', 'Nursing home construction work'),
  ('45220000-5', 'Εργασίες μηχανικού και δομικές εργασίες', 'Engineering works and construction works'),
  ('45221220-0', 'Οχετός', 'Culverts'),
  ('45221230-3', 'Φρέατα', 'Shafts'),
  ('45223000-6', 'Κατασκευαστικές εργασίες για δομικές κατασκευές', 'Structures construction work'),
  ('45223100-7', 'Συναρμολόγηση μεταλλικών δομικών κατασκευών', 'Assembly of metal structures'),
  ('45223110-0', 'Τοποθέτηση μεταλλικών δομικών κατασκευών', 'Installation of metal structures'),
  ('45223500-1', 'Κατασκευές από οπλισμένο σκυρόδεμα', 'Reinforced-concrete structures'),
  ('45231110-9', 'Κατασκευαστικές εργασίες για τοποθέτηση αγωγών', 'Pipelaying construction work'),
  ('45231300-8', 'Κατασκευαστικές εργασίες για αγωγούς ύδρευσης και αποχέτευσης', 'Construction work for water and sewage pipelines'),
  ('45232121-6', 'Κατασκευαστικές εργασίες για σωληνώσεις διανομής νερού άρδευσης', 'Irrigation piping construction work'),
  ('45232130-2', 'Κατασκευαστικές εργασίες για σωληνώσεις ομβρίων υδάτων', 'Storm-water piping construction work'),
  ('45232150-8', 'Εργασίες σχετιζόμενες με τους αγωγούς διανομής νερού', 'Works related to water-distribution pipelines'),
  ('45232152-2', 'Κατασκευαστικές εργασίες για αντλιοστάσια', 'Pumping station construction work'),
  ('45232332-8', 'Εργασίες σχετιζόμενες με εγκαταστάσεις τηλεπικοινωνιών', 'Ancillary works for telecommunications'),
  ('45232400-6', 'Έργα κατασκευής αποχετεύσεων', 'Sewer construction work'),
  ('45232420-2', 'Κατασκευαστικές εργασίες αποχετευτικών δικτύων', 'Sewage work'),
  ('45232452-5', 'Εργασίες αποστράγγισης', 'Drainage works'),
  ('45233000-9', 'Κατασκευαστικά έργα', 'Construction, foundation and surface works for highways, roads'),
  ('45233120-6', 'Έργα οδοποιίας', 'Road construction works'),
  ('45233141-9', 'Εργασίες συντήρησης οδών', 'Road-maintenance works'),
  ('45233142-6', 'Εργασίες επισκευής οδών', 'Road-repair works'),
  ('45233144-0', 'Κατασκευαστικές εργασίες για άνω διαβάσεις', 'Overpass construction work'),
  ('45233160-8', 'Δίοδοι προσπέλασης και λοιπές μη ασφαλτοστρωμένες επιφάνειες', 'Paths and other metalled surfaces'),
  ('45233161-5', 'Κατασκευαστικές εργασίες για διόδους προσπέλασης', 'Footpath construction work'),
  ('45233200-1', 'Διάφορες εργασίες επικάλυψης του οδοστρώματος', 'Various surface works'),
  ('45233220-7', 'Επιφανειακές εργασίες οδοστρώματος οδών', 'Surface work for roads'),
  ('45233222-1', 'Κατασκευαστικές εργασίες για έργα οδοστρωσίας και ασφαλτόστρωσης', 'Paving and asphalting works'),
  ('45233229-0', 'Συντήρησης ερεισμάτων', 'Verge maintenance work'),
  ('45233250-6', 'Εργασίες επίστρωσης εκτός από οδοστρώσεις', 'Surfacing work except for roads'),
  ('45233260-9', 'Κατασκευαστικές εργασίες για πεζοδρόμους', 'Pedestrian ways construction work'),
  ('45233290-8', 'Εγκατάσταση οδικών πινακίδων', 'Installation of road signs'),
  ('45233291-5', 'Εγκατάσταση μικρών οδικών στύλων', 'Installation of bollards'),
  ('45233292-2', 'Εγκατάσταση εξοπλισμού ασφαλείας', 'Installation of safety equipment'),
  ('45233294-6', 'Εγκατάσταση οδικών σημάτων', 'Installation of road signals'),
  ('45234140-9', 'Κατασκευαστικές εργασίες για ισόπεδες διαβάσεις', 'Level crossing construction works'),
  ('45236000-0', 'Εργασίες διάστρωσης', 'Flatwork'),
  ('45240000-1', 'Κατασκευαστικές εργασίες για υδραυλικά έργα', 'Construction work for water projects'),
  ('45241600-4', 'Εγκατάσταση εξοπλισμού φωτισμού λιμένα', 'Installation of port lighting equipment'),
  ('45243510-0', 'Εργασίες επιχωμάτωσης', 'Embankment works'),
  ('45246400-7', 'Αντιπλημμυρικά έργα', 'Flood-prevention works'),
  ('45246410-0', 'Συντήρηση αντιπλημμυρικών έργων', 'Flood-defences maintenance works'),
  ('45247130-0', 'Κατασκευαστικές εργασίες για υδραγωγεία', 'Aqueduct construction work'),
  ('45247200-2', 'Κατασκευαστικές εργασίες φραγμάτων και παρόμοιων μόνιμων κατασκευών', 'Construction work for dams and similar fixed structures'),
  ('45247210-5', 'Κατασκευαστικές εργασίες για φράγματα', 'Dam construction work'),
  ('45247211-2', 'Κατασκευαστικές εργασίες για τοίχους φραγμάτων', 'Dam wall construction work'),
  ('45247212-9', 'Εργασίες ενίσχυσης φράγματος', 'Dam-reinforcement works'),
  ('45247220-8', 'Κατασκευαστικές εργασίες για μικρά ποτάμια φράγματα', 'Weir construction work'),
  ('45247230-1', 'Κατασκευαστικές εργασίες για αντιπλημμυρικά αναχώματα', 'Dyke construction work'),
  ('45247240-4', 'Κατασκευαστικές εργασίες για στατικά φράγματα', 'Static barrage construction work'),
  ('45247270-3', 'Κατασκευαστικές εργασίες δεξαμενής', 'Reservoir construction works'),
  ('45252127-4', 'Κατασκευαστικές εργασίες για σταθμούς επεξεργασίας λυμάτων', 'Wastewater treatment plant construction work'),
  ('45252130-8', 'Εξοπλισμός σταθμού επεξεργασίας λυμάτων', 'Sewage plant equipment'),
  ('45259000-7', 'Επισκευή και συντήρηση εγκαταστάσεων', 'Repair and maintenance of plant'),
  ('45261000-4', 'Οικοδομικές και συναφείς εργασίες επικάλυψης στεγών και κατασκευής διαχωριστικών πλαισίων', 'Erection and related works of roof frames and coverings'),
  ('45261100-5', 'Εργασίες κατασκευής πλαισίων στεγών', 'Roof-framing work'),
  ('45261210-9', 'Εργασίες κάλυψης στέγης', 'Roof-covering work'),
  ('45261320-3', 'Εργασίες τοποθέτησης υδρορροών', 'Guttering work'),
  ('45261410-1', 'Εργασίες μόνωσης στέγης', 'Roof insulation work'),
  ('45261900-3', 'Εργασίες επισκευής και συντήρησης στέγης', 'Roof repair and maintenance work'),
  ('45262100-2', 'Εργασίες ικριωμάτων', 'Scaffolding work'),
  ('45262120-8', 'Εργασίες ανέγερσης ικριωμάτων', 'Scaffolding erection work'),
  ('45262210-6', 'Εργασίες θεμελιώσεων', 'Foundation work'),
  ('45262300-4', 'Εργασίες σκυροδέματος', 'Concrete work'),
  ('45262310-7', 'Εργασίες οπλισμένου σκυροδέματος', 'Reinforced-concrete work'),
  ('45262311-4', 'Εργασίες καλουπώματος για την έγχυση σκυροδέματος', 'Concrete carcassing work'),
  ('45262320-0', 'Εργασίες επικάλυψης δαπέδων', 'Screed works'),
  ('45262360-2', 'Εργασίες με τσιμεντοκονίαμα', 'Cementing work'),
  ('45262370-5', 'Εργασίες επίχρισης σκυροδέματος', 'Concrete-coating work'),
  ('45262510-9', 'Λιθοκατασκευές', 'Stonework'),
  ('45262522-6', 'Εργασίες τοιχοποιίας', 'Masonry work'),
  ('45262640-9', 'Έργα αναβάθμισης περιβάλλοντος', 'Environmental improvement works'),
  ('45300000-0', 'Εργασίες κτιριακών εγκαταστάσεων', 'Building installation work'),
  ('45310000-3', 'Εργασίες ηλεκτρολογικών εγκαταστάσεων', 'Electrical installation work'),
  ('45312000-7', 'Εργασίες εγκατάστασης συστημάτων συναγερμού και κεραιών', 'Alarm system and antenna installation work'),
  ('45312100-8', 'Εργασίες εγκατάστασης συστήματος συναγερμού πυρκαγιάς', 'Fire-alarm system installation work'),
  ('45312200-9', 'Εργασίες εγκατάστασης συστήματος αντικλεπτικού συναγερμού', 'Burglar-alarm system installation work'),
  ('45312311-0', 'Εργασίες εγκατάστασης αλεξικέραυνου', 'Lightning-conductor installation work'),
  ('45315000-8', 'Ηλεκτρολογικές εργασίες εγκατάστασης θέρμανσης και άλλου ηλεκτρολογικού εξοπλισμού κτιρίων', 'Electrical installation work of heating and other electrical building-equipment'),
  ('45320000-6', 'Εργασίες μόνωσης', 'Insulation work'),
  ('45330000-9', 'Υδραυλικές εργασίες', 'Plumbing and sanitary works'),
  ('45331000-6', 'Εργασίες εγκαταστάσεων θέρμανσης', 'Heating, ventilation and air-conditioning installation work'),
  ('45331220-4', 'Εργασίες εγκατάστασης συστημάτων κλιματισμού', 'Air-conditioning installation work'),
  ('45332200-5', 'Υδραυλικά έργα', 'Water plumbing work'),
  ('45333100-1', 'Εργασίες εγκατάστασης εξοπλισμού ρύθμισης αερίου', 'Gas regulation equipment installation work'),
  ('45340000-2', 'Εργασίες τοποθέτησης περιφράξεων', 'Fencing, railing and safety equipment installation work'),
  ('45341000-9', 'Τοποθέτηση κιγκλιδωμάτων', 'Erection of railings'),
  ('45342000-6', 'Τοποθέτηση περιφράξεων', 'Erection of fencing'),
  ('45343000-3', 'Εργασίες εγκαταστάσεων πρόληψης πυρκαγιάς', 'Fire-prevention installation works'),
  ('45343100-4', 'Εργασίες πυροπροστασίας', 'Fireproofing work'),
  ('45343200-5', 'Εργασίες εγκατάστασης εξοπλισμών πυρόσβεσης', 'Firefighting equipment installation work'),
  ('45343220-1', 'Εργασίες εγκατάστασης πυροσβεστήρων', 'Fire-extinguishers installation work'),
  ('45351000-2', 'Εργασίες μηχανολογικών εγκαταστάσεων', 'Mechanical engineering installation works'),
  ('45410000-4', 'Εργασίες επίχρισης κονιαμάτων', 'Plastering work'),
  ('45420000-7', 'Εργασίες τοποθέτησης στοιχείων από ξύλο', 'Joinery and carpentry installation work'),
  ('45421100-5', 'Τοποθέτηση θυρών', 'Installation of doors and windows and related components'),
  ('45421131-1', 'Εγκατάσταση θυρών', 'Installation of doors'),
  ('45430000-0', 'Εργασίες επένδυσης δαπέδων και τοίχων', 'Floor and wall covering work'),
  ('45431100-8', 'Εργασίες τοποθέτησης πλακιδίων δαπέδων', 'Floor-tiling work'),
  ('45432114-6', 'Τοποθέτηση ξύλινων δαπέδων', 'Wood flooring work'),
  ('45441000-0', 'Υαλοτεχνικές εργασίες', 'Glazing work'),
  ('45442100-8', 'Εργασίες βαφής', 'Painting work'),
  ('45442110-1', 'Εργασίες βαφής κτιρίων', 'Painting work of buildings'),
  ('45442120-4', 'Εργασίες βαφής και προστατευτικής επικάλυψης τεχνικών κατασκευών', 'Painting and protective-coating work of structures'),
  ('45510000-5', 'Ενοικίαση γερανών με χειριστή', 'Hire of cranes with operator'),
  ('45520000-8', 'Ενοικίαση εξοπλισμού χωματουργικών εργασιών με χειριστή', 'Hire of earthmoving equipment with operator'),
  ('48000000-8', 'Πακέτα λογισμικού και συστήματα πληροφορικής', 'Software package and information systems'),
  ('48420000-8', 'Πακέτα λογισμικού διαχείρισης εγκαταστάσεων και πλατφόρμες πακέτων λογισμικού', 'Facilities management software package and software package suite'),
  ('48422000-2', 'Πλατφόρμες πακέτων λογισμικού', 'Software package suites'),
  ('48810000-9', 'Συστήματα πληροφόρησης', 'Information systems'),
  ('48900000-7', 'Διάφορα πακέτα λογισμικού και συστήματα ηλεκτρονικών υπολογιστών', 'Miscellaneous software package and computer systems'),
  ('48921000-0', 'Ηλεκτρονικά συστήματα αυτοματοπίησης', 'Automation system'),
  ('50000000-5', 'Υπηρεσίες επισκευής και συντήρησης', 'Repair and maintenance services'),
  ('50110000-9', 'Υπηρεσίες επισκευής και συντήρησης μηχανοκίνητων οχημάτων και παρεπόμενου εξοπλισμού', 'Repair and maintenance services of motor vehicles and associated equipment'),
  ('50118200-7', 'Υπηρεσίες επισκευής βλαβών επαγγελματικών οχημάτων', 'Breakdown and recovery services for commercial vehicles'),
  ('50211100-8', 'Υπηρεσίες συντήρησης αεροσκαφών', 'Aircraft maintenance services'),
  ('50211310-3', 'Υπηρεσίες γενικής επισκευής κινητήρων αεροσκαφών', 'Reconditioning services of aircraft engines'),
  ('50230000-6', 'Υπηρεσίες επισκευής', 'Repair, maintenance and associated services related to roads and other equipment'),
  ('50300000-8', 'Επισκευή', 'Repair, maintenance and associated services related to personal computers, office equipment, telecommunications and audio-visual equipment'),
  ('50313100-3', 'Υπηρεσίες επισκευής φωτοαντιγραφικών μηχανημάτων', 'Photocopier repair services'),
  ('50344000-8', 'Υπηρεσίες επισκευής και συντήρησης οπτικού εξοπλισμού', 'Repair and maintenance services of optical equipment'),
  ('50413200-5', 'Υπηρεσίες επισκευής και συντήρησης εξοπλισμού πυρόσβεσης', 'Repair and maintenance services of firefighting equipment'),
  ('50500000-0', 'Υπηρεσίες επισκευής και συντήρησης αντλιών', 'Repair and maintenance services for pumps, valves, taps and metal containers and machinery'),
  ('50514200-3', 'Υπηρεσίες επισκευής και συντήρησης δεξαμενών', 'Repair and maintenance services of reservoirs'),
  ('50531100-7', 'Υπηρεσίες επισκευής και συντήρησης καυστήρων', 'Repair and maintenance services of boilers'),
  ('50532200-5', 'Υπηρεσίες επισκευής και συντήρησης μετασχηματιστών', 'Repair and maintenance services of transformers'),
  ('50532300-6', 'Υπηρεσίες επισκευής και συντήρησης γεννητριών', 'Repair and maintenance services of generators'),
  ('50532400-7', 'Υπηρεσίες επισκευής και συντήρησης εξοπλισμού διανομής ηλεκτρικού ρεύματος', 'Repair and maintenance services of electrical distribution equipment'),
  ('50700000-2', 'Υπηρεσίες επισκευής και συντήρησης εγκαταστάσεων κτιρίου', 'Repair and maintenance services of building installations'),
  ('50710000-5', 'Υπηρεσίες επισκευής και συντήρησης ηλεκτρομηχανολογικών εγκαταστάσεων κτιρίου', 'Repair and maintenance services of electrical and mechanical building installations'),
  ('50711000-2', 'Υπηρεσίες επισκευής και συντήρησης ηλεκτρολογικών εγκαταστάσεων κτιρίου', 'Repair and maintenance services of electrical building installations'),
  ('50800000-3', 'Διάφορες υπηρεσίες επισκευής και συντήρησης', 'Miscellaneous repair and maintenance services'),
  ('51000000-9', 'Υπηρεσίες εγκατάστασης (εκτός από υπηρεσίες εγκατάστασης λογισμικού)', 'Installation services (except software)'),
  ('51112200-2', 'Υπηρεσίες εγκατάστασης εξοπλισμού ελέγχου ηλεκτρικού ρεύματος', 'Installation services of electricity control equipment'),
  ('51120000-9', 'Υπηρεσίες εγκατάστασης μηχανολογικού εξοπλισμού', 'Installation services of mechanical equipment'),
  ('51121000-6', 'Υπηρεσίες εγκατάστασης εξοπλισμού γυμναστικής', 'Installation services of fitness equipment'),
  ('51314000-6', 'Υπηρεσίες εγκατάστασης εξοπλισμού βίντεο', 'Installation services of video equipment'),
  ('51500000-7', 'Υπηρεσίες εγκατάστασης μηχανημάτων και εξοπλισμού', 'Installation services of machinery and equipment'),
  ('51700000-9', 'Υπηρεσίες εγκατάστασης εξοπλισμού πυροπροστασίας', 'Installation services of fire protection equipment'),
  ('51810000-3', 'Υπηρεσίες εγκατάστασης δεξαμενών', 'Installation services of tanks'),
  ('60181000-0', 'Ενοικίαση φορτηγών αυτοκινήτων με οδηγό', 'Hire of trucks with driver'),
  ('60182000-7', 'Ενοικίαση οχημάτων επαγγελματικής χρήσης με οδηγό', 'Hire of industrial vehicles with driver'),
  ('63122000-0', 'Υπηρεσίες αποθήκευσης αγαθών', 'Warehousing services'),
  ('66519300-4', 'Επικουρικές ασφαλιστικές υπηρεσίες', 'Auxiliary insurance services'),
  ('71242000-6', 'Προετοιμασία έργων και σχεδίων', 'Project and design preparation, estimation of costs'),
  ('71300000-1', 'Υπηρεσίες μηχανικού', 'Engineering services'),
  ('71320000-7', 'Υπηρεσίες εκπόνησης τεχνικών μελετών', 'Engineering design services'),
  ('71322000-1', 'Υπηρεσίες εκπόνησης τεχνικών μελετών για την κατασκευή έργων πολιτικού μηχανικού', 'Engineering design services for the construction of civil engineering works'),
  ('71323200-0', 'Υπηρεσίες σχεδιασμού τεχνικών εγκαταστάσεων', 'Plant engineering design services'),
  ('71334000-8', 'Μηχανολογικές και ηλεκτρολογικές υπηρεσίες', 'Mechanical and electrical engineering services'),
  ('71335000-5', 'Τεχνικές μελέτες', 'Engineering studies'),
  ('71337000-9', 'Τεχνικές υπηρεσίες σε θέματα διάβρωσης', 'Corrosion engineering services'),
  ('71351914-3', 'Αρχαιολογικές υπηρεσίες', 'Archaeological services'),
  ('71354100-5', 'Υπηρεσίες ψηφιακής χαρτογράφησης', 'Digital mapping services'),
  ('71354500-9', 'Υπηρεσίες θαλάσσιων μελετών', 'Marine survey services'),
  ('71356000-8', 'Τεχνικές υπηρεσίες', 'Technical services'),
  ('71356300-1', 'Υπηρεσίες τεχνικής υποστήριξης', 'Technical support services'),
  ('71500000-3', 'Κατασκευαστικές και συναφείς υπηρεσίες', 'Construction-related services'),
  ('71700000-5', 'Υπηρεσίες παρακολούθησης και ελέγχου', 'Monitoring and control services'),
  ('72000000-5', 'Υπηρεσίες τεχνολογίας των πληροφοριών: παροχή συμβουλών', 'IT services: consulting, software development, Internet and support'),
  ('72211000-7', 'Υπηρεσίες προγραμματισμού λογισμικών συστήματος και χρήστη', 'Programming services of systems and user software'),
  ('72212000-4', 'Υπηρεσίες προγραμματισμού λογισμικού εφαρμογών', 'Programming services of application software'),
  ('72221000-0', 'Υπηρεσίες παροχής συμβουλών επιχειρησιακής ανάλυσης', 'Business analysis consultancy services'),
  ('72224000-1', 'Υπηρεσίες παροχής συμβουλών σε θέματα διαχείρισης έργων', 'Project management consultancy services'),
  ('72253200-5', 'Υπηρεσίες υποστήριξης συστημάτων πληροφορικής', 'Systems support services'),
  ('72254000-0', 'Δοκιμές λογισμικού', 'Software testing'),
  ('72262000-9', 'Υπηρεσίες ανάπτυξης λογισμικού', 'Software development services'),
  ('72415000-2', 'Υπηρεσίες φιλοξενίας τοποθεσιών Παγκόσμιου Ιστού (WWW)', 'World wide web (www) site operation host services'),
  ('73300000-5', 'Μελέτη και εκτέλεση ερευνητικών και αναπτυξιακών δραστηριοτήτων', 'Design and execution of research and development'),
  ('75211100-9', 'Διπλωματικές υπηρεσίες', 'Diplomatic services'),
  ('75250000-3', 'Πυροσβεστικές υπηρεσίες και υπηρεσίες διάσωσης', 'Fire-brigade and rescue services'),
  ('75251000-0', 'Πυροσβεστικές υπηρεσίες', 'Fire-brigade services'),
  ('75251100-1', 'Υπηρεσίες καταπολέμησης πυρκαγιών', 'Firefighting services'),
  ('75251110-4', 'Υπηρεσίες πρόληψης πυρκαγιών', 'Fire-prevention services'),
  ('75251120-7', 'Υπηρεσίες καταπολέμησης δασοπυρκαγιών', 'Forest-firefighting services'),
  ('77000000-0', 'Υπηρεσίες γεωργίας', 'Agricultural, forestry, horticultural, aquacultural and apicultural services'),
  ('77111000-1', 'Ενοικίαση γεωργικών μηχανημάτων με χειριστή', 'Hire of agricultural machinery with operator'),
  ('77112000-8', 'Ενοικίαση χορτοκοπτικών ή γεωργικών μηχανημάτων με χειριστή', 'Hire of mowers or agricultural equipment with operator'),
  ('77200000-2', 'Υπηρεσίες δασοκομίας', 'Forestry services'),
  ('77210000-5', 'Υπηρεσίες υλοτομίας', 'Logging services'),
  ('77211000-2', 'Υπηρεσίες σχετιζόμενες με την υλοτομία', 'Services incidental to logging'),
  ('77211100-3', 'Υπηρεσίες περισυλλογής ξυλείας', 'Timber harvesting services'),
  ('77211200-4', 'Μεταφορά κορμών δένδρων στο εσωτερικό δάσους', 'Transport of logs within the forest'),
  ('77211300-5', 'Υπηρεσίες υλοτόμισης', 'Tree-clearing services'),
  ('77211400-6', 'Υπηρεσίες κοπής δένδρων', 'Tree-cutting services'),
  ('77211500-7', 'Υπηρεσίες φροντίδας δένδρων', 'Tree-maintenance services'),
  ('77230000-1', 'Υπηρεσίες σχετιζόμενες με τη δασοκομία', 'Services incidental to forestry'),
  ('77231000-8', 'Υπηρεσίες διαχείρισης δασοκομίας', 'Forestry management services'),
  ('77231200-0', 'Υπηρεσίες ελέγχου παρασίτων του δάσους', 'Forest pest control services'),
  ('77231300-1', 'Υπηρεσίες διαχείρισης δασών', 'Forest administration services'),
  ('77231600-4', 'Υπηρεσίες αναδάσωσης', 'Afforestation services'),
  ('77231700-5', 'Υπηρεσίες δασικής επέκτασης', 'Forestry extension services'),
  ('77231900-7', 'Υπηρεσίες δασικού τομεακού σχεδιασμού', 'Forest sectoral planning services'),
  ('77300000-3', 'Υπηρεσίες φυτοκομίας', 'Horticultural services'),
  ('77310000-6', 'Φύτευση και συντήρηση χώρων πρασίνου', 'Planting and maintenance services of green areas'),
  ('77311000-3', 'Συντήρηση διακοσμητικών κήπων ή κήπων αναψυχής', 'Ornamental and pleasure gardens maintenance services'),
  ('77312000-0', 'Υπηρεσίες εκκαθάρισης από αγριόχορτα', 'Weed-clearance services'),
  ('77312100-1', 'Υπηρεσίες εξόντωσης ζιζανίων', 'Weed-killing services'),
  ('77313000-7', 'Υπηρεσίες συντήρησης πάρκων', 'Parks maintenance services'),
  ('77314000-4', 'Υπηρεσίες συντήρησης οικοπέδων', 'Grounds maintenance services'),
  ('77314100-5', 'Υπηρεσίες χορτοκάλυψης', 'Grassing services'),
  ('77320000-9', 'Συντήρηση αθλητικών γηπέδων', 'Sports fields maintenance services'),
  ('77330000-2', 'Υπηρεσίες επίδειξης φυτών', 'Floral-display services'),
  ('77340000-5', 'Κλάδεμα δένδρων και θάμνων', 'Tree pruning and hedge trimming'),
  ('77341000-2', 'Κλάδεμα δένδρων', 'Tree pruning'),
  ('77342000-9', 'Κλάδεμα θάμνων', 'Hedge trimming'),
  ('79132000-8', 'Υπηρεσίες πιστοποίησης', 'Certification services'),
  ('79311100-8', 'Υπηρεσίες σχεδιασμού έρευνας', 'Survey design services'),
  ('79314000-8', 'Μελέτη σκοπιμότητας', 'Feasibility study'),
  ('79410000-1', 'Υπηρεσίες παροχής επιχειρηματικών συμβουλών και συμβουλών σε θέματα διαχείρισης', 'Business and management consultancy services'),
  ('79415200-8', 'Υπηρεσίες παροχής συμβουλών σε θέματα σχεδιασμού', 'Design consultancy services'),
  ('79417000-0', 'Υπηρεσίες παροχής συμβουλών σε θέματα ασφάλειας', 'Safety consultancy services'),
  ('79421200-3', 'Υπηρεσίες μελέτης έργων', 'Project-design services other than for construction work'),
  ('79430000-7', 'Υπηρεσίες διαχείρισης κρίσεων', 'Crisis management services'),
  ('79711000-1', 'Υπηρεσίες παρακολούθησης συστημάτων συναγερμού', 'Alarm-monitoring services'),
  ('79713000-5', 'Υπηρεσίες φύλαξης', 'Guard services'),
  ('79714000-2', 'Υπηρεσίες επιτήρησης', 'Surveillance services'),
  ('79930000-2', 'Ειδικές υπηρεσίες μελετών', 'Specialty design services'),
  ('79933000-3', 'Υπηρεσίες υποστήριξης στον τομέα του σχεδιασμού', 'Design support services'),
  ('79950000-8', 'Υπηρεσίες διοργάνωσης εκθέσεων και συνεδρίων', 'Exhibition, fair and congress organisation services'),
  ('79980000-7', 'Υπηρεσίες συνδρομών', 'Subscription services'),
  ('80400000-8', 'Υπηρεσίες εκπαίδευσης ενηλίκων και άλλες εκπαιδευτικές υπηρεσίες', 'Adult and other education services'),
  ('80530000-8', 'Υπηρεσίες επαγγελματικής κατάρτισης', 'Vocational training services'),
  ('85312320-8', 'Υπηρεσίες παροχής συμβουλών', 'Counselling services'),
  ('85320000-8', 'Κοινωνικές υπηρεσίες', 'Social services'),
  ('90000000-7', 'Υπηρεσίες λυμάτων', 'Sewage, refuse, cleaning and environmental services'),
  ('90460000-9', 'Υπηρεσίες εκκένωσης βόθρων ή σηπτικών δεξαμενών', 'Cesspool or septic tank emptying services'),
  ('90500000-2', 'Υπηρεσίες σχετιζόμενες με απορρίμματα και απόβλητα', 'Refuse and waste related services'),
  ('90511000-2', 'Υπηρεσίες αποκομιδής απορριμμάτων', 'Refuse collection services'),
  ('90511100-3', 'Υπηρεσίες αποκομιδής στερεών αστικών απορριμμάτων', 'Urban solid-refuse collection services'),
  ('90511300-5', 'Υπηρεσίες αποκομιδής απορριμμάτων από υπαίθρια δοχεία', 'Litter collection services'),
  ('90512000-9', 'Υπηρεσίες μεταφοράς απορριμμάτων', 'Refuse transport services'),
  ('90513000-6', 'Υπηρεσίες επεξεργασίας και διάθεσης μη επικίνδυνων απορριμμάτων και αποβλήτων', 'Non-hazardous refuse and waste treatment and disposal services'),
  ('90513200-8', 'Υπηρεσίες διάθεσης στερεών αστικών απορριμμάτων', 'Urban solid-refuse disposal services'),
  ('90513600-2', 'Υπηρεσίες αφαίρεσης ιλύος', 'Sludge removal services'),
  ('90513700-3', 'Υπηρεσίες μεταφοράς ιλύος', 'Sludge transport services'),
  ('90522400-6', 'Καθαρισμός και επεξεργασία εδάφους', 'Cleaning and treatment of soil'),
  ('90524400-0', 'Αποκομιδή', 'Collection, transport and disposal of hospital waste'),
  ('90530000-1', 'Λειτουργία χώρου απόθεσης απορριμμάτων', 'Operation of a refuse site'),
  ('90600000-3', 'Υπηρεσίες καθαριότητας και εξυγίανσης σε αστικές ή αγροτικές ζώνες', 'Cleaning and sanitation services in urban or rural areas, and related services'),
  ('90610000-6', 'Υπηρεσίες καθαρισμού και σάρωσης οδών', 'Street-cleaning and sweeping services'),
  ('90641000-2', 'Υπηρεσίες καθαρισμού φρεατίων υπονόμων', 'Gully cleaning services'),
  ('90670000-4', 'Υπηρεσίες απολύμανσης και εξολόθρευσης σε αστικές ή αγροτικές ζώνες', 'Disinfecting and exterminating services in urban or rural areas'),
  ('90680000-7', 'Υπηρεσίες καθαρισμού παραλίας', 'Beach cleaning services'),
  ('90711100-5', 'Αξιολόγηση υπαρκτών ή δυνητικών κινδύνων', 'Risk or hazard assessment other than for construction'),
  ('90711500-9', 'Περιβαλλοντική παρακολούθηση', 'Environmental monitoring other than for construction'),
  ('90712100-2', 'Σχεδιασμός αστικής περιβαλλοντικής ανάπτυξης', 'Urban environmental development planning'),
  ('90712200-3', 'Στρατηγικός σχεδιασμός διατήρησης δασών', 'Forest conservation strategy planning'),
  ('90713000-8', 'Υπηρεσίες παροχής συμβουλών για περιβαλλοντικά ζητήματα', 'Environmental issues consultancy services'),
  ('90713100-9', 'Υπηρεσίες παροχής συμβουλών για την υδροδότηση και τα λύματα', 'Consulting services for water-supply and waste-water other than for construction'),
  ('90720000-0', 'Προστασία του περιβάλλοντος', 'Environmental protection'),
  ('90721100-8', 'Υπηρεσίες προστασίας του τοπίου', 'Landscape protection services'),
  ('90721700-4', 'Υπηρεσίες προστασίας ειδών υπό εξαφάνιση', 'Endangered species protection services'),
  ('90721800-5', 'Υπηρεσίες προστασίας από φυσικούς υπαρκτούς ή δυνητικούς κινδύνους', 'Natural risks or hazards protection services'),
  ('90722000-4', 'Αποκατάσταση του περιβάλλοντος', 'Environmental rehabilitation'),
  ('90722300-7', 'Υπηρεσίες αναμόρφωσης γαιών', 'Land reclamation services'),
  ('90910000-9', 'Υπηρεσίες καθαρισμού', 'Cleaning services'),
  ('90911200-8', 'Υπηρεσίες καθαρισμού κτιρίων', 'Building-cleaning services'),
  ('90913200-2', 'Υπηρεσία καθαρισμού δεξαμενών', 'Reservoir cleaning services'),
  ('90921000-9', 'Υπηρεσίες απολύμανσης και εξολόθρευσης', 'Disinfecting and exterminating services'),
  ('90922000-6', 'Υπηρεσίες καταπολέμησης επιβλαβών εντόμων', 'Pest-control services'),
  ('98300000-6', 'Διάφορες υπηρεσίες', 'Miscellaneous services'),
  ('98314000-7', 'Υπηρεσίες χρωματισμού', 'Colouring services'),
  ('98371110-8', 'Υπηρεσίες νεκροταφείων', 'Cemetery services'),
  ('98371111-5', 'Υπηρεσίες συντήρησης νεκροταφείου', 'Cemetery maintenance services')
) AS descriptions (cpv_key, cpv_value, cpv_value_en)
WHERE cpv.cpv_key = descriptions.cpv_key
  AND cpv.cpv_value IS NOT DISTINCT FROM descriptions.cpv_value
  AND cpv.cpv_value_en IS NULL;
-- END CPV English descriptions (069)

-- -------------------------------------------------------------
-- Document prevention/suppression classification (064)
-- -------------------------------------------------------------
-- Forest-fire document classification: 1 = prevention, 0 = suppression,
-- NULL = unknown. No default or backfill; classification is assigned later.
ALTER TABLE public.procurement
  ADD COLUMN IF NOT EXISTS prevention INTEGER;
ALTER TABLE public.diavgeia
  ADD COLUMN IF NOT EXISTS prevention INTEGER;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.procurement'::regclass
      AND conname = 'procurement_prevention_check'
  ) THEN
    ALTER TABLE public.procurement
      ADD CONSTRAINT procurement_prevention_check
      CHECK (prevention IN (0, 1));
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.diavgeia'::regclass
      AND conname = 'diavgeia_prevention_check'
  ) THEN
    ALTER TABLE public.diavgeia
      ADD CONSTRAINT diavgeia_prevention_check
      CHECK (prevention IN (0, 1));
  END IF;
END;
$$;

COMMENT ON COLUMN public.procurement.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; NULL = unknown.';
COMMENT ON COLUMN public.diavgeia.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; NULL = unknown.';

-- Unclassifiable document result (065)
-- NULL = not yet classified; 2 = reviewed but cannot be classified.
ALTER TABLE public.procurement
  DROP CONSTRAINT IF EXISTS procurement_prevention_check;
ALTER TABLE public.procurement
  ADD CONSTRAINT procurement_prevention_check CHECK (prevention IN (0, 1, 2));
ALTER TABLE public.diavgeia
  DROP CONSTRAINT IF EXISTS diavgeia_prevention_check;
ALTER TABLE public.diavgeia
  ADD CONSTRAINT diavgeia_prevention_check CHECK (prevention IN (0, 1, 2));

COMMENT ON COLUMN public.procurement.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; 2 = reviewed but unclassifiable; NULL = not yet classified.';
COMMENT ON COLUMN public.diavgeia.prevention IS
  'Forest-fire classification: 1 = prevention; 0 = suppression; 2 = reviewed but unclassifiable; NULL = not yet classified.';

-- -------------------------------------------------------------
-- Last successful PDF link scans (066)
-- -------------------------------------------------------------
-- Last successful PDF reference extraction on the source document itself.
-- NULL = no successful scan; references = [] = successful scan with no findings.
-- A later failed attempt must not overwrite this result. No history or errors.
ALTER TABLE public.procurement
  ADD COLUMN IF NOT EXISTS pdf_link_scan JSONB;
ALTER TABLE public.diavgeia
  ADD COLUMN IF NOT EXISTS pdf_link_scan JSONB;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.procurement'::regclass
      AND conname = 'procurement_pdf_link_scan_check'
  ) THEN
    ALTER TABLE public.procurement
      ADD CONSTRAINT procurement_pdf_link_scan_check CHECK (
        pdf_link_scan IS NULL OR (
          jsonb_typeof(pdf_link_scan) = 'object'
          AND pdf_link_scan ?& ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ]
          AND pdf_link_scan - ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ] = '{}'::jsonb
          AND jsonb_typeof(pdf_link_scan->'fingerprint') = 'string'
          AND pdf_link_scan->>'fingerprint' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'extractor_version') = 'number'
          AND pdf_link_scan->>'extractor_version' ~ '^[1-9][0-9]*$'
          AND jsonb_typeof(pdf_link_scan->'checked_at') = 'string'
          AND length(pdf_link_scan->>'checked_at') > 0
          AND jsonb_typeof(pdf_link_scan->'pdf_sha256') = 'string'
          AND pdf_link_scan->>'pdf_sha256' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'references') = 'array'
        )
      );
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.diavgeia'::regclass
      AND conname = 'diavgeia_pdf_link_scan_check'
  ) THEN
    ALTER TABLE public.diavgeia
      ADD CONSTRAINT diavgeia_pdf_link_scan_check CHECK (
        pdf_link_scan IS NULL OR (
          jsonb_typeof(pdf_link_scan) = 'object'
          AND pdf_link_scan ?& ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ]
          AND pdf_link_scan - ARRAY[
            'fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references'
          ] = '{}'::jsonb
          AND jsonb_typeof(pdf_link_scan->'fingerprint') = 'string'
          AND pdf_link_scan->>'fingerprint' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'extractor_version') = 'number'
          AND pdf_link_scan->>'extractor_version' ~ '^[1-9][0-9]*$'
          AND jsonb_typeof(pdf_link_scan->'checked_at') = 'string'
          AND length(pdf_link_scan->>'checked_at') > 0
          AND jsonb_typeof(pdf_link_scan->'pdf_sha256') = 'string'
          AND pdf_link_scan->>'pdf_sha256' ~ '^[0-9a-f]{64}$'
          AND jsonb_typeof(pdf_link_scan->'references') = 'array'
        )
      );
  END IF;
END;
$$;

COMMENT ON COLUMN public.procurement.pdf_link_scan IS
  'Last successful PDF link scan: fingerprint, extractor_version, checked_at, pdf_sha256, references [{reference,page,evidence}]. NULL = no successful scan; empty references = scanned without findings. Failed attempts belong in logs.';
COMMENT ON COLUMN public.diavgeia.pdf_link_scan IS
  'Last successful PDF link scan: fingerprint, extractor_version, checked_at, pdf_sha256, references [{reference,page,evidence}]. NULL = no successful scan; empty references = scanned without findings. Failed attempts belong in logs.';


-- -------------------------------------------------------------
-- A) Procedure normalization function
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.normalize_procedure_type(p_val text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_val IS NULL OR btrim(p_val) = '' THEN '—'
    WHEN lower(btrim(p_val)) LIKE 'απευθείας ανάθεση%' THEN 'Απευθείας ανάθεση'
    ELSE btrim(p_val)
  END;
$$;

GRANT EXECUTE ON FUNCTION public.normalize_procedure_type(text) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- B) Hero section RPC
-- Source: sql/hero_section_rpc.sql
-- -------------------------------------------------------------
-- Hero section RPC based on:
-- - procurement.contract_signed_date (date axis and windows)
-- - payment.amount_without_vat (amount metric)
-- No diavgeia payment usage.

CREATE OR REPLACE FUNCTION public.get_hero_section_data(
  p_year_main integer,
  p_year_start integer DEFAULT 2024
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
DECLARE
  v_as_of_date date;
  v_end_month int;
  v_end_day int;
  v_period_main_start date;
  v_period_main_end date;
  v_period_prev1_start date;
  v_period_prev1_end date;
  v_period_prev2_start date;
  v_period_prev2_end date;
  v_total_main numeric := 0;
  v_total_prev1 numeric := 0;
  v_total_prev2 numeric := 0;
  v_top_contract_type text := null;
  v_top_contract_type_count int := 0;
  v_top_contract_type_prev1_count int := 0;
  v_top_cpv_text text := null;
  v_top_cpv_count int := 0;
  v_top_cpv_prev1_count int := 0;
  v_curve jsonb := '[]'::jsonb;
BEGIN
  v_as_of_date := LEAST(CURRENT_DATE, make_date(p_year_main, 12, 31));
  IF v_as_of_date < make_date(p_year_main, 1, 1) THEN
    v_as_of_date := make_date(p_year_main, 1, 1);
  END IF;

  v_end_month := EXTRACT(MONTH FROM v_as_of_date);
  v_end_day := EXTRACT(DAY FROM v_as_of_date);

  v_period_main_start := make_date(p_year_main, 1, 1);
  v_period_main_end := v_as_of_date;

  v_period_prev1_start := make_date(p_year_main - 1, 1, 1);
  v_period_prev1_end := make_date(
    p_year_main - 1,
    v_end_month,
    LEAST(v_end_day, EXTRACT(DAY FROM (date_trunc('month', make_date(p_year_main - 1, v_end_month, 1)) + interval '1 month - 1 day'))::int)
  );

  v_period_prev2_start := make_date(p_year_main - 2, 1, 1);
  v_period_prev2_end := make_date(
    p_year_main - 2,
    v_end_month,
    LEAST(v_end_day, EXTRACT(DAY FROM (date_trunc('month', make_date(p_year_main - 2, v_end_month, 1)) + interval '1 month - 1 day'))::int)
  );

  WITH pay_by_proc AS (
    SELECT
      py.procurement_id,
      SUM(py.amount_without_vat) AS amount_without_vat
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT
      pr.id,
      pr.contract_signed_date AS d,
      pr.procedure_type_value,
      pb.amount_without_vat
    FROM proc_ranked pr
    JOIN pay_by_proc pb ON pb.procurement_id = pr.id
    WHERE pr.rn = 1
  ),
  main_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_main_start AND v_period_main_end
  ),
  prev1_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_prev1_start AND v_period_prev1_end
  ),
  prev2_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_prev2_start AND v_period_prev2_end
  )
  SELECT
    COALESCE((SELECT SUM(amount_without_vat) FROM main_rows), 0),
    COALESCE((SELECT SUM(amount_without_vat) FROM prev1_rows), 0),
    COALESCE((SELECT SUM(amount_without_vat) FROM prev2_rows), 0)
  INTO v_total_main, v_total_prev1, v_total_prev2;

  WITH pay_by_proc AS (
    SELECT
      py.procurement_id,
      SUM(py.amount_without_vat) AS amount_without_vat
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT
      pr.id,
      pr.contract_signed_date AS d,
      pr.procedure_type_value
    FROM proc_ranked pr
    WHERE pr.rn = 1
  ),
  main_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_main_start AND v_period_main_end
  ),
  prev1_rows AS (
    SELECT * FROM rows_base WHERE d BETWEEN v_period_prev1_start AND v_period_prev1_end
  ),
  ranked AS (
    SELECT COALESCE(procedure_type_value, '—') AS procedure_name, COUNT(*)::int AS cnt
    FROM main_rows
    GROUP BY COALESCE(procedure_type_value, '—')
    ORDER BY cnt DESC
    LIMIT 1
  )
  SELECT
    r.procedure_name,
    r.cnt,
    COALESCE((
      SELECT COUNT(*)::int
      FROM prev1_rows p1
      WHERE COALESCE(p1.procedure_type_value, '—') = r.procedure_name
    ), 0)
  INTO v_top_contract_type, v_top_contract_type_count, v_top_contract_type_prev1_count
  FROM ranked r;

  WITH pay_by_proc AS (
    SELECT py.procurement_id
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT pr.id, pr.contract_signed_date AS d
    FROM proc_ranked pr
    WHERE pr.rn = 1
  ),
  main_ids AS (
    SELECT id FROM rows_base WHERE d BETWEEN v_period_main_start AND v_period_main_end
  ),
  prev1_ids AS (
    SELECT id FROM rows_base WHERE d BETWEEN v_period_prev1_start AND v_period_prev1_end
  ),
  top_cpv AS (
    SELECT c.cpv_value, COUNT(*)::int AS cnt
    FROM public.cpv c
    JOIN main_ids m ON m.id = c.procurement_id
    WHERE c.cpv_value IS NOT NULL
    GROUP BY c.cpv_value
    ORDER BY cnt DESC
    LIMIT 1
  )
  SELECT
    t.cpv_value,
    t.cnt,
    COALESCE((
      SELECT COUNT(*)::int
      FROM public.cpv c2
      JOIN prev1_ids p1 ON p1.id = c2.procurement_id
      WHERE c2.cpv_value = t.cpv_value
    ), 0)
  INTO v_top_cpv_text, v_top_cpv_count, v_top_cpv_prev1_count
  FROM top_cpv t;

  WITH pay_by_proc AS (
    SELECT
      py.procurement_id,
      SUM(py.amount_without_vat) AS amount_without_vat
    FROM public.payment py
    WHERE py.amount_without_vat IS NOT NULL
    GROUP BY py.procurement_id
  ),
  proc_ranked AS (
    SELECT
      p.id,
      p.contract_signed_date,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
        ORDER BY p.id DESC
      ) AS rn
    FROM public.procurement p
    JOIN pay_by_proc pp ON pp.procurement_id = p.id
    WHERE p.contract_signed_date IS NOT NULL
      AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
  ),
  rows_base AS (
    SELECT
      EXTRACT(YEAR FROM pr.contract_signed_date)::int AS series_year,
      pr.contract_signed_date AS point_date,
      pb.amount_without_vat AS amount_without_vat
    FROM proc_ranked pr
    JOIN pay_by_proc pb ON pb.procurement_id = pr.id
    WHERE pr.rn = 1
      AND EXTRACT(YEAR FROM pr.contract_signed_date)::int BETWEEN p_year_start AND p_year_main
  ),
  daily_agg AS (
    SELECT
      series_year,
      point_date,
      SUM(amount_without_vat) AS daily_amount
    FROM rows_base
    GROUP BY series_year, point_date
  ),
  cal AS (
    SELECT
      b.series_year,
      gs::date AS point_date,
      EXTRACT(DOY FROM gs)::int AS day_of_year,
      EXTRACT(DOY FROM b.year_full_end)::int AS year_days
    FROM (
      SELECT
        y AS series_year,
        make_date(y, 12, 31) AS year_full_end,
        CASE
          WHEN y = p_year_main THEN make_date(
            y,
            v_end_month,
            LEAST(v_end_day, EXTRACT(DAY FROM (date_trunc('month', make_date(y, v_end_month, 1)) + interval '1 month - 1 day'))::int)
          )
          ELSE make_date(y, 12, 31)
        END AS year_draw_end
      FROM generate_series(p_year_start, p_year_main) y
    ) b
    CROSS JOIN LATERAL generate_series(make_date(b.series_year, 1, 1), b.year_draw_end, interval '1 day') gs
  ),
  curve AS (
    SELECT
      c.series_year,
      c.point_date,
      c.day_of_year,
      c.year_days,
      SUM(COALESCE(d.daily_amount, 0)) OVER (
        PARTITION BY c.series_year
        ORDER BY c.point_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_amount
    FROM cal c
    LEFT JOIN daily_agg d
      ON d.series_year = c.series_year
     AND d.point_date = c.point_date
  )
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'series_year', series_year,
        'point_date', to_char(point_date, 'YYYY-MM-DD'),
        'day_of_year', day_of_year,
        'year_days', year_days,
        'cumulative_amount', cumulative_amount
      )
      ORDER BY series_year, day_of_year
    ),
    '[]'::jsonb
  )
  INTO v_curve
  FROM curve;

  RETURN jsonb_build_object(
    'period_main_start', to_char(v_period_main_start, 'YYYY-MM-DD'),
    'period_main_end', to_char(v_period_main_end, 'YYYY-MM-DD'),
    'total_main', v_total_main,
    'total_prev1', v_total_prev1,
    'total_prev2', v_total_prev2,
    'top_contract_type', v_top_contract_type,
    'top_contract_type_count', v_top_contract_type_count,
    'top_contract_type_prev1_count', v_top_contract_type_prev1_count,
    'top_cpv_text', v_top_cpv_text,
    'top_cpv_count', v_top_cpv_count,
    'top_cpv_prev1_count', v_top_cpv_prev1_count,
    'curve_points', v_curve
  );
END;
$$;

GRANT EXECUTE ON FUNCTION public.get_hero_section_data(integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- C) Contracts page RPC
-- Source: sql/contracts_page_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_contracts_page(text, text, text, date, date, numeric, integer, integer);

CREATE OR REPLACE FUNCTION public.get_contracts_page(
  p_q text DEFAULT NULL,
  p_org text DEFAULT NULL,
  p_procedure text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_min_amount numeric DEFAULT NULL,
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 50
)
RETURNS TABLE (
  id bigint,
  contract_signed_date date,
  organization_value text,
  title text,
  reference_number text,
  prev_reference_no text,
  next_ref_no text,
  cpv_value text,
  procedure_type_value text,
  beneficiary_name text,
  beneficiary_vat_number text,
  beneficiary_gemi text,
  amount_without_vat numeric,
  diavgeia_ada text,
  cancelled boolean,
  is_modified boolean,
  total_count bigint
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
WITH RECURSIVE raw_query_terms AS (
  SELECT parsed.raw_term
  FROM regexp_split_to_table(
    regexp_replace(LEFT(COALESCE(p_q, ''), 500), E'\\s*!\\s*', '&!', 'g'),
    E'\\s*&\\s*'
  ) WITH ORDINALITY AS parsed(raw_term, ordinal)
  WHERE parsed.ordinal <= 10
),
query_terms AS (
  SELECT
    upper(
      translate(
        LEFT(BTRIM(CASE WHEN LEFT(raw_term, 1) = '!' THEN SUBSTRING(raw_term FROM 2) ELSE raw_term END), 100),
        'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
        'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
      )
    ) AS value,
    LEFT(raw_term, 1) = '!' AS excluded
  FROM raw_query_terms
  WHERE BTRIM(CASE WHEN LEFT(raw_term, 1) = '!' THEN SUBSTRING(raw_term FROM 2) ELSE raw_term END) <> ''
),
payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_name), ''), ' | ') AS beneficiary_name,
    STRING_AGG(DISTINCT NULLIF(TRIM(py.beneficiary_vat_number), ''), ' | ') AS beneficiary_vat_number,
    STRING_AGG(DISTINCT NULLIF(TRIM(b.gemi), ''), ' | ') AS beneficiary_gemi
  FROM public.payment py
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = py.beneficiary_vat_number
  GROUP BY py.procurement_id
),
cpv_agg AS (
  SELECT
    c.procurement_id,
    STRING_AGG(
      DISTINCT NULLIF(TRIM(c.cpv_value), ''),
      ' | '
      ORDER BY NULLIF(TRIM(c.cpv_value), '')
    ) AS cpv_value
  FROM public.cpv c
  GROUP BY c.procurement_id
),
base AS (
  SELECT
    p.id,
    p.contract_signed_date,
    p.title,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.diavgeia_ada,
    COALESCE(p.cancelled, FALSE) AS cancelled,
    (
      NULLIF(TRIM(p.next_ref_no), '') IS NOT NULL
      OR COALESCE(p.next_modified, FALSE)
      OR COALESCE(p.next_extended, FALSE)
      OR EXISTS (
        SELECT 1
        FROM public.procurement p2
        WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
      )
    ) AS is_modified,
    p.reference_number,
    p.prev_reference_no,
    p.next_ref_no,
    p.contract_number,
    p.organization_key,
    COALESCE(pa.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    pa.beneficiary_name,
    pa.beneficiary_vat_number,
    pa.beneficiary_gemi,
    COALESCE(
      org.organization_value,
      CASE
        WHEN p.canonical_owner_scope = 'region' THEN COALESCE(
          CASE WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value) END,
          CASE WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value) END,
          CASE WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value) END,
          CASE WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value) END
        )
        ELSE COALESCE(
          CASE WHEN muni.municipality_normalized_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_normalized_value) END,
          CASE WHEN reg.region_normalized_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_normalized_value) END,
          CASE WHEN muni.municipality_value IS NOT NULL THEN CONCAT('ΔΗΜΟΣ ', muni.municipality_value) END,
          CASE WHEN reg.region_value IS NOT NULL THEN CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', reg.region_value) END
        )
      END
    ) AS organization_value,
    ca.cpv_value,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(TRIM(p.reference_number), ''),
        NULLIF(TRIM(p.diavgeia_ada), ''),
        NULLIF(TRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN payment_agg pa ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      m.municipality_normalized_value,
      m.municipality_value
    FROM public.municipality m
    WHERE m.municipality_key = p.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) muni ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      r.region_normalized_value,
      r.region_value
    FROM public.region r
    WHERE r.region_key = p.region_key
    ORDER BY r.id
    LIMIT 1
  ) reg ON TRUE
  LEFT JOIN LATERAL (
    SELECT o.organization_normalized_value AS organization_value
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN cpv_agg ca ON ca.procurement_id = p.id
),
dedup AS (
  SELECT *
  FROM base
  WHERE rn = 1
),
eligible AS (
  SELECT
    d.*,
    upper(
      translate(
        CONCAT_WS(
          ' ',
          COALESCE(d.title, ''),
          COALESCE(d.organization_value, ''),
          COALESCE(d.beneficiary_name, ''),
          COALESCE(d.cpv_value, ''),
          COALESCE(d.reference_number, '')
        ),
        'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
        'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
      )
    ) AS searchable_text
  FROM dedup d
  WHERE (p_date_from IS NULL OR d.contract_signed_date >= p_date_from)
    AND (p_date_to IS NULL OR d.contract_signed_date <= p_date_to)
    AND (p_min_amount IS NULL OR COALESCE(d.amount_without_vat, 0) >= p_min_amount)
    AND (p_procedure IS NULL OR p_procedure = '' OR d.procedure_type_value = p_procedure)
    AND (p_org IS NULL OR p_org = '' OR COALESCE(d.organization_value, '') ILIKE '%' || p_org || '%')
),
chain_members AS (
  SELECT
    e.id,
    e.reference_number,
    e.prev_reference_no,
    e.next_ref_no
  FROM eligible e
  WHERE BTRIM(COALESCE(p_q, '')) <> ''
    AND NOT EXISTS (
      SELECT 1
      FROM query_terms qt
      WHERE (NOT qt.excluded AND STRPOS(e.searchable_text, qt.value) = 0)
         OR (qt.excluded AND STRPOS(e.searchable_text, qt.value) > 0)
    )

  UNION

  SELECT
    neighbor.id,
    neighbor.reference_number,
    neighbor.prev_reference_no,
    neighbor.next_ref_no
  FROM chain_members member
  JOIN dedup neighbor
    ON NULLIF(BTRIM(neighbor.reference_number), '') = NULLIF(BTRIM(member.prev_reference_no), '')
    OR NULLIF(BTRIM(neighbor.prev_reference_no), '') = NULLIF(BTRIM(member.reference_number), '')
    OR NULLIF(BTRIM(neighbor.reference_number), '') = NULLIF(BTRIM(member.next_ref_no), '')
    OR NULLIF(BTRIM(neighbor.next_ref_no), '') = NULLIF(BTRIM(member.reference_number), '')
),
filtered AS (
  SELECT e.*
  FROM eligible e
  WHERE p_q IS NULL
    OR p_q = ''
    OR EXISTS (
      SELECT 1
      FROM chain_members member
      WHERE member.id = e.id
    )
),
counted AS (
  SELECT f.*, COUNT(*) OVER () AS total_count
  FROM filtered f
)
SELECT
  id,
  contract_signed_date,
  organization_value,
  title,
  reference_number,
  prev_reference_no,
  next_ref_no,
  cpv_value,
  procedure_type_value,
  beneficiary_name,
  beneficiary_vat_number,
  beneficiary_gemi,
  amount_without_vat,
  diavgeia_ada,
  cancelled,
  is_modified,
  total_count
FROM counted
ORDER BY contract_signed_date DESC NULLS LAST, id DESC
OFFSET (LEAST(GREATEST(COALESCE(p_page, 1), 1), 10000) - 1)
  * LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 10000)
LIMIT LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 10000);
$$;

GRANT EXECUTE ON FUNCTION public.get_contracts_page(text, text, text, date, date, numeric, integer, integer) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_contracts_page_snapshot(
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
SELECT jsonb_build_object(
  'rows', COALESCE(
    jsonb_agg(
      to_jsonb(r) - 'total_count'
      ORDER BY r.contract_signed_date DESC NULLS LAST, r.id DESC
    ),
    '[]'::jsonb
  )
)
FROM public.get_contracts_page(
  NULL, NULL, NULL, p_date_from, p_date_to, NULL, 1, 10000
) r;
$$;

GRANT EXECUTE ON FUNCTION public.get_contracts_page_snapshot(date, date) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- D) Featured records RPC
-- Source: sql/featured_records_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_featured_beneficiaries(integer, integer);

CREATE OR REPLACE FUNCTION public.get_featured_beneficiaries(
  p_year_main integer,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  beneficiary_vat_number text,
  beneficiary_gemi text,
  beneficiary_name text,
  organization text,
  total_amount numeric,
  contract_count integer,
  cpv text,
  start_date date,
  end_date date,
  duration_days integer,
  progress_pct numeric,
  signer text,
  relevant_contracts jsonb
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
WITH proc_ranked AS (
  SELECT
    p.id AS procurement_id,
    p.organization_key,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    p.short_descriptions,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.reference_number,
    p.contract_number,
    p.contract_budget,
    p.budget,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.prev_reference_no,
    p.next_ref_no,
    p.contract_related_ada,
    p.organization_vat_number,
    p.start_date,
    p.end_date,
    p.diavgeia_ada,
    COALESCE(py.amount_without_vat, 0) AS amount_without_vat,
    py.amount_with_vat,
    pb.beneficiary_vat_number,
    NULLIF(TRIM(b.gemi), '') AS beneficiary_gemi,
    COALESCE(
      NULLIF(TRIM(b.beneficiary_name), ''),
      NULLIF(TRIM(pb.beneficiary_vat_number), '')
    ) AS beneficiary_name,
    py.signers,
    py.payment_ref_no,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        ),
        pb.beneficiary_vat_number
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN public.payment py
    ON py.procurement_id = p.id
  JOIN public.payment_beneficiary pb
    ON pb.payment_id = py.id
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = pb.beneficiary_vat_number
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date BETWEEN make_date(p_year_main, 1, 1) AND make_date(p_year_main, 12, 31)
    AND NULLIF(TRIM(pb.beneficiary_vat_number), '') IS NOT NULL
),
dedup_base AS (
  SELECT
    pr.procurement_id,
    pr.organization_key,
    pr.title,
    pr.submission_at,
    pr.contract_signed_date,
    pr.short_descriptions,
    pr.procedure_type_value,
    pr.reference_number,
    pr.contract_number,
    pr.contract_budget,
    pr.budget,
    pr.assign_criteria,
    pr.contract_type,
    pr.award_procedure,
    pr.units_operator,
    pr.funding_details_cofund,
    pr.funding_details_self_fund,
    pr.funding_details_espa,
    pr.funding_details_regular_budget,
    pr.auction_ref_no,
    pr.prev_reference_no,
    pr.next_ref_no,
    pr.contract_related_ada,
    pr.organization_vat_number,
    pr.start_date,
    pr.end_date,
    pr.diavgeia_ada,
    pr.amount_without_vat,
    pr.amount_with_vat,
    pr.beneficiary_vat_number,
    pr.beneficiary_gemi,
    COALESCE(pr.beneficiary_name, pr.beneficiary_vat_number) AS beneficiary_name,
    COALESCE(NULLIF(TRIM(pr.signers), ''), '—') AS signers,
    pr.payment_ref_no
  FROM proc_ranked pr
  WHERE pr.rn = 1
),
beneficiary_totals AS (
  SELECT
    b.beneficiary_vat_number,
    SUM(b.amount_without_vat) AS total_amount,
    COUNT(*)::integer AS contract_count,
    MIN(b.start_date) AS start_date,
    MAX(b.end_date) AS end_date
  FROM dedup_base b
  GROUP BY b.beneficiary_vat_number
),
top_beneficiaries AS (
  SELECT
    bt.beneficiary_vat_number,
    bt.total_amount,
    bt.contract_count,
    bt.start_date,
    bt.end_date
  FROM beneficiary_totals bt
  ORDER BY bt.total_amount DESC, bt.contract_count DESC, bt.beneficiary_vat_number
  LIMIT GREATEST(COALESCE(p_limit, 50), 1)
),
base AS (
  SELECT
    db.procurement_id,
    db.organization_key,
    db.title,
    db.submission_at,
    db.contract_signed_date,
    db.short_descriptions,
    db.procedure_type_value,
    db.reference_number,
    db.contract_number,
    db.contract_budget,
    db.budget,
    db.assign_criteria,
    db.contract_type,
    db.award_procedure,
    db.units_operator,
    db.funding_details_cofund,
    db.funding_details_self_fund,
    db.funding_details_espa,
    db.funding_details_regular_budget,
    db.auction_ref_no,
    db.prev_reference_no,
    db.next_ref_no,
    db.contract_related_ada,
    db.organization_vat_number,
    db.start_date,
    db.end_date,
    db.diavgeia_ada,
    db.amount_without_vat,
    db.amount_with_vat,
    db.beneficiary_vat_number,
    db.beneficiary_gemi,
    db.beneficiary_name,
    db.signers,
    db.payment_ref_no
  FROM dedup_base db
  JOIN top_beneficiaries tb
    ON tb.beneficiary_vat_number = db.beneficiary_vat_number
),
org_lookup AS (
  SELECT DISTINCT ON (o.organization_key)
    o.organization_key,
    COALESCE(o.organization_normalized_value, o.organization_value, o.organization_key, '—') AS organization_value
  FROM public.organization o
  JOIN (
    SELECT DISTINCT b.organization_key
    FROM base b
    WHERE NULLIF(TRIM(b.organization_key), '') IS NOT NULL
  ) bo
    ON bo.organization_key = o.organization_key
  ORDER BY o.organization_key, o.id
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    NULLIF(TRIM(c.cpv_key), '') AS cpv_key,
    NULLIF(TRIM(c.cpv_value), '') AS cpv_value
  FROM public.cpv c
  JOIN (
    SELECT DISTINCT b.procurement_id
    FROM base b
  ) bp
    ON bp.procurement_id = c.procurement_id
  WHERE NULLIF(TRIM(c.cpv_key), '') IS NOT NULL
     OR NULLIF(TRIM(c.cpv_value), '') IS NOT NULL
),
cpv_agg AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', COALESCE(cd.cpv_key, '—'),
        'label', COALESCE(cd.cpv_value, '—')
      )
      ORDER BY COALESCE(cd.cpv_value, ''), COALESCE(cd.cpv_key, '')
    ) AS cpv_items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
base_enriched AS (
  SELECT
    b.procurement_id,
    b.organization_key,
    COALESCE(ol.organization_value, b.organization_key, '—') AS organization_value,
    b.title,
    b.submission_at,
    b.contract_signed_date,
    b.short_descriptions,
    b.procedure_type_value,
    b.reference_number,
    b.contract_number,
    b.contract_budget,
    b.budget,
    b.assign_criteria,
    b.contract_type,
    b.award_procedure,
    b.units_operator,
    b.funding_details_cofund,
    b.funding_details_self_fund,
    b.funding_details_espa,
    b.funding_details_regular_budget,
    b.auction_ref_no,
    b.prev_reference_no,
    b.next_ref_no,
    b.contract_related_ada,
    b.organization_vat_number,
    b.start_date,
    b.end_date,
    b.diavgeia_ada,
    b.amount_without_vat,
    b.amount_with_vat,
    b.beneficiary_vat_number,
    b.beneficiary_gemi,
    b.beneficiary_name,
    b.signers,
    b.payment_ref_no,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items
  FROM base b
  LEFT JOIN org_lookup ol
    ON ol.organization_key = b.organization_key
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = b.procurement_id
),
beneficiary_name_latest AS (
  SELECT
    b.beneficiary_vat_number,
    b.beneficiary_name,
    ROW_NUMBER() OVER (
      PARTITION BY b.beneficiary_vat_number
      ORDER BY b.contract_signed_date DESC NULLS LAST, b.procurement_id DESC, b.beneficiary_name
    ) AS rn
  FROM base_enriched b
),
beneficiary_name_ranked AS (
  SELECT
    bnl.beneficiary_vat_number,
    bnl.beneficiary_name,
    bnl.rn
  FROM beneficiary_name_latest bnl
),
beneficiary_org_totals AS (
  SELECT
    b.beneficiary_vat_number,
    b.organization_value,
    SUM(b.amount_without_vat) AS total_amount
  FROM base_enriched b
  GROUP BY b.beneficiary_vat_number, b.organization_value
),
beneficiary_org_ranked AS (
  SELECT
    bot.beneficiary_vat_number,
    bot.organization_value,
    ROW_NUMBER() OVER (
      PARTITION BY bot.beneficiary_vat_number
      ORDER BY bot.total_amount DESC, bot.organization_value
    ) AS rn
  FROM beneficiary_org_totals bot
),
beneficiary_signer_counts AS (
  SELECT
    b.beneficiary_vat_number,
    b.signers,
    COUNT(*) AS signer_count
  FROM base_enriched b
  WHERE NULLIF(TRIM(b.signers), '') IS NOT NULL
  GROUP BY b.beneficiary_vat_number, b.signers
),
beneficiary_signer_ranked AS (
  SELECT
    bsc.beneficiary_vat_number,
    bsc.signers,
    ROW_NUMBER() OVER (
      PARTITION BY bsc.beneficiary_vat_number
      ORDER BY bsc.signer_count DESC, bsc.signers
    ) AS rn
  FROM beneficiary_signer_counts bsc
),
beneficiary_cpv_counts AS (
  SELECT
    b.beneficiary_vat_number,
    cpv_item ->> 'label' AS cpv_label,
    COUNT(*) AS cpv_count
  FROM base_enriched b
  CROSS JOIN LATERAL jsonb_array_elements(b.cpv_items) cpv_item
  WHERE NULLIF(TRIM(cpv_item ->> 'label'), '') IS NOT NULL
  GROUP BY b.beneficiary_vat_number, cpv_item ->> 'label'
),
beneficiary_cpv_ranked AS (
  SELECT
    bcc.beneficiary_vat_number,
    bcc.cpv_label,
    ROW_NUMBER() OVER (
      PARTITION BY bcc.beneficiary_vat_number
      ORDER BY bcc.cpv_count DESC, bcc.cpv_label
    ) AS rn
  FROM beneficiary_cpv_counts bcc
),
relevant_ranked AS (
  SELECT
    b.beneficiary_vat_number,
    b.beneficiary_gemi,
    b.procurement_id,
    jsonb_build_object(
      'id', b.procurement_id,
      'organization', b.organization_value,
      'title', b.title,
      'submission_at', b.submission_at,
      'short_description', split_part(COALESCE(b.short_descriptions, ''), ' | ', 1),
      'procedure_type_value', b.procedure_type_value,
      'amount_without_vat', b.amount_without_vat,
      'amount_with_vat', b.amount_with_vat,
      'reference_number', b.reference_number,
      'contract_number', b.contract_number,
      'cpv_items', b.cpv_items,
      'contract_signed_date', b.contract_signed_date,
      'start_date', b.start_date,
      'end_date', b.end_date,
      'organization_vat_number', b.organization_vat_number,
      'beneficiary_vat_number', b.beneficiary_vat_number,
      'beneficiary_gemi', b.beneficiary_gemi,
      'beneficiary_name', b.beneficiary_name,
      'signers', b.signers,
      'assign_criteria', b.assign_criteria,
      'contract_type', b.contract_type,
      'award_procedure', b.award_procedure,
      'units_operator', b.units_operator,
      'funding_details_cofund', b.funding_details_cofund,
      'funding_details_self_fund', b.funding_details_self_fund,
      'funding_details_espa', b.funding_details_espa,
      'funding_details_regular_budget', b.funding_details_regular_budget,
      'auction_ref_no', b.auction_ref_no,
      'prev_reference_no', b.prev_reference_no,
      'next_ref_no', b.next_ref_no,
      'contract_related_ada', b.contract_related_ada,
      'payment_ref_no', b.payment_ref_no,
      'budget', b.budget,
      'contract_budget', b.contract_budget,
      'diavgeia_ada', b.diavgeia_ada
    ) AS contract_json,
    ROW_NUMBER() OVER (
      PARTITION BY b.beneficiary_vat_number
      ORDER BY b.amount_without_vat DESC, b.contract_signed_date DESC NULLS LAST, b.procurement_id DESC
    ) AS rn
  FROM base_enriched b
),
relevant_agg AS (
  SELECT
    rr.beneficiary_vat_number,
    jsonb_agg(rr.contract_json ORDER BY rr.rn) AS relevant_contracts
  FROM relevant_ranked rr
  WHERE rr.rn <= 5
  GROUP BY rr.beneficiary_vat_number
)
SELECT
  tb.beneficiary_vat_number,
  NULLIF(TRIM(bg.gemi), '') AS beneficiary_gemi,
  COALESCE(bnr.beneficiary_name, tb.beneficiary_vat_number) AS beneficiary_name,
  COALESCE(bor.organization_value, '—') AS organization,
  tb.total_amount,
  tb.contract_count,
  COALESCE(bcr.cpv_label, '—') AS cpv,
  tb.start_date,
  tb.end_date,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date < tb.start_date THEN NULL
    ELSE (tb.end_date - tb.start_date + 1)
  END::integer AS duration_days,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date <= tb.start_date THEN NULL
    WHEN CURRENT_DATE <= tb.start_date THEN 0
    WHEN CURRENT_DATE >= tb.end_date THEN 100
    ELSE ROUND((((CURRENT_DATE - tb.start_date)::numeric / NULLIF((tb.end_date - tb.start_date)::numeric, 0)) * 100)::numeric, 2)
  END AS progress_pct,
  COALESCE(bsr.signers, '—') AS signer,
  COALESCE(ra.relevant_contracts, '[]'::jsonb) AS relevant_contracts
FROM top_beneficiaries tb
LEFT JOIN beneficiary_name_ranked bnr
  ON bnr.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bnr.rn = 1
LEFT JOIN public.beneficiary bg
  ON bg.beneficiary_vat_number = tb.beneficiary_vat_number
LEFT JOIN beneficiary_org_ranked bor
  ON bor.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bor.rn = 1
LEFT JOIN beneficiary_signer_ranked bsr
  ON bsr.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bsr.rn = 1
LEFT JOIN beneficiary_cpv_ranked bcr
  ON bcr.beneficiary_vat_number = tb.beneficiary_vat_number
 AND bcr.rn = 1
LEFT JOIN relevant_agg ra
  ON ra.beneficiary_vat_number = tb.beneficiary_vat_number
ORDER BY tb.total_amount DESC, tb.contract_count DESC, tb.beneficiary_vat_number;
$$;

GRANT EXECUTE ON FUNCTION public.get_featured_beneficiaries(integer, integer)
TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- E) Municipality map spend RPC
-- Source: sql/029_municipality_map_spend_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_municipality_map_spend_per_100k(integer);

CREATE OR REPLACE FUNCTION public.get_municipality_map_spend_per_100k(
  p_year integer
)
RETURNS TABLE (
  municipality_key text,
  municipality_name text,
  population_total numeric,
  total_amount_without_vat numeric,
  amount_per_100k numeric,
  signed_current_count bigint,
  active_previous_count bigint
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(py.amount_without_vat) AS amount_without_vat
  FROM public.payment py
  WHERE py.amount_without_vat IS NOT NULL
  GROUP BY py.procurement_id
),
direct_municipality AS (
  SELECT
    p.id AS procurement_id,
    p.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(TRIM(p.reference_number), ''),
        NULLIF(TRIM(p.diavgeia_ada), ''),
        NULLIF(TRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date IS NOT NULL
    AND p.contract_signed_date <= make_date(p_year, 12, 31)
    AND (
      p.contract_signed_date >= make_date(p_year, 1, 1)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
    AND p.municipality_key IS NOT NULL
    AND (
      p.canonical_owner_scope = 'municipality'
      OR COALESCE(org.authority_scope, 'other') = 'municipality'
    )
),
attributed_organization AS (
  SELECT
    p.id AS procurement_id,
    p.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(TRIM(p.reference_number), ''),
        NULLIF(TRIM(p.diavgeia_ada), ''),
        NULLIF(TRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date IS NOT NULL
    AND p.contract_signed_date <= make_date(p_year, 12, 31)
    AND (
      p.contract_signed_date >= make_date(p_year, 1, 1)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
    AND p.municipality_key IS NOT NULL
    AND p.organization_key IS NOT NULL
    AND COALESCE(p.canonical_owner_scope, '') = 'organization'
    AND COALESCE(org.authority_scope, 'other') = 'municipality'
),
coverage_organization AS (
  SELECT
    p.id AS procurement_id,
    omc.municipality_key AS target_municipality_key,
    pa.amount_without_vat,
    p.reference_number,
    p.diavgeia_ada,
    p.title,
    p.contract_signed_date,
    p.organization_key,
    ROW_NUMBER() OVER (
      PARTITION BY
        omc.municipality_key,
        COALESCE(
          NULLIF(TRIM(p.reference_number), ''),
          NULLIF(TRIM(p.diavgeia_ada), ''),
          NULLIF(TRIM(p.contract_number), ''),
          CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
        )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN public.org_municipality_coverage omc
    ON omc.organization_key = p.organization_key
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(TRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(TRIM(p2.prev_reference_no), '') = p.reference_number
    )
    AND p.contract_signed_date IS NOT NULL
    AND p.contract_signed_date <= make_date(p_year, 12, 31)
    AND (
      p.contract_signed_date >= make_date(p_year, 1, 1)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
    AND COALESCE(org.authority_scope, 'other') = 'municipality'
),
unioned AS (
  SELECT *, 0 AS source_priority FROM direct_municipality WHERE rn = 1
  UNION ALL
  SELECT *, 1 AS source_priority FROM coverage_organization WHERE rn = 1
  UNION ALL
  SELECT *, 2 AS source_priority FROM attributed_organization WHERE rn = 1
),
dedup AS (
  SELECT *
  FROM (
    SELECT
      u.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          u.target_municipality_key,
          COALESCE(
            NULLIF(TRIM(u.reference_number), ''),
            NULLIF(TRIM(u.diavgeia_ada), ''),
            CONCAT_WS('|', COALESCE(u.organization_key, ''), COALESCE(u.title, ''), COALESCE(u.contract_signed_date::text, ''))
          )
        ORDER BY u.source_priority ASC, u.procurement_id DESC
      ) AS keep_rn
    FROM unioned u
  ) ranked
  WHERE keep_rn = 1
),
municipality_stats AS (
  SELECT
    d.target_municipality_key AS municipality_key,
    SUM(COALESCE(d.amount_without_vat, 0)) AS total_amount_without_vat,
    COUNT(*) FILTER (
      WHERE d.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
    )::bigint AS signed_current_count,
    COUNT(*) FILTER (
      WHERE d.contract_signed_date < make_date(p_year, 1, 1)
    )::bigint AS active_previous_count
  FROM dedup d
  GROUP BY d.target_municipality_key
)
SELECT
  mfpd.municipality_key,
  mfpd.dhmos AS municipality_name,
  mfpd.plithismos_synolikos AS population_total,
  COALESCE(ms.total_amount_without_vat, 0) AS total_amount_without_vat,
  ROUND(
    (
      COALESCE(ms.total_amount_without_vat, 0) * 100000.0
    ) / NULLIF(mfpd.plithismos_synolikos, 0),
    2
  ) AS amount_per_100k,
  COALESCE(ms.signed_current_count, 0) AS signed_current_count,
  COALESCE(ms.active_previous_count, 0) AS active_previous_count
FROM public.municipality_fire_protection_data mfpd
LEFT JOIN municipality_stats ms
  ON ms.municipality_key = mfpd.municipality_key
WHERE mfpd.plithismos_synolikos IS NOT NULL
  AND mfpd.plithismos_synolikos > 0
ORDER BY amount_per_100k DESC NULLS LAST, total_amount_without_vat DESC, mfpd.dhmos;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_map_spend_per_100k(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- F) Municipality funding map RPC
-- Source: sql/031_municipality_map_funding_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_municipality_map_funding_per_100k(integer);

CREATE OR REPLACE FUNCTION public.get_municipality_map_funding_per_100k(
  p_year integer
)
RETURNS TABLE (
  municipality_key text,
  municipality_name text,
  population_total numeric,
  total_amount_eur numeric,
  amount_per_100k numeric
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH municipality_funding AS (
  SELECT
    f.municipality_key,
    SUM(COALESCE(f.amount_eur, 0)) AS total_amount_eur
  FROM public.fund f
  WHERE f.year = p_year
    AND NULLIF(TRIM(f.municipality_key), '') IS NOT NULL
  GROUP BY f.municipality_key
)
SELECT
  mfpd.municipality_key,
  mfpd.dhmos AS municipality_name,
  mfpd.plithismos_synolikos AS population_total,
  COALESCE(mf.total_amount_eur, 0) AS total_amount_eur,
  ROUND(
    (
      COALESCE(mf.total_amount_eur, 0) * 100000.0
    ) / NULLIF(mfpd.plithismos_synolikos, 0),
    2
  ) AS amount_per_100k
FROM public.municipality_fire_protection_data mfpd
LEFT JOIN municipality_funding mf
  ON mf.municipality_key = mfpd.municipality_key
WHERE mfpd.plithismos_synolikos IS NOT NULL
  AND mfpd.plithismos_synolikos > 0
ORDER BY amount_per_100k DESC NULLS LAST, total_amount_eur DESC, mfpd.dhmos;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_map_funding_per_100k(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- G) Homepage funding RPC
-- Source: sql/032_homepage_funding_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_homepage_funding(integer, integer);

CREATE OR REPLACE FUNCTION public.get_homepage_funding(
  p_year_main integer,
  p_year_start integer DEFAULT 2016
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH filtered_funding AS (
  SELECT
    f.year::int AS year,
    COALESCE(f.amount_eur, 0) AS amount_eur,
    LOWER(BTRIM(COALESCE(f.allocation_type, ''))) AS allocation_type_norm,
    LOWER(BTRIM(COALESCE(f.recipient_type, ''))) AS recipient_type_norm
  FROM public.fund f
  WHERE f.year IS NOT NULL
    AND f.year BETWEEN p_year_start AND p_year_main
    AND LOWER(BTRIM(COALESCE(f.recipient_type, ''))) IN ('δήμος', 'σύνδεσμος', 'σύνδεσμος δήμων')
),
latest_year AS (
  SELECT COALESCE(MAX(ff.year), p_year_main) AS year_main
  FROM filtered_funding ff
),
aggregated_funding AS (
  SELECT
    ff.year,
    ROUND(SUM(CASE WHEN ff.allocation_type_norm = 'τακτική' THEN ff.amount_eur ELSE 0 END), 2) AS regular_amount,
    ROUND(SUM(CASE WHEN ff.allocation_type_norm = 'τακτική' THEN 0 ELSE ff.amount_eur END), 2) AS emergency_amount,
    ROUND(SUM(CASE WHEN ff.recipient_type_norm = 'δήμος' THEN ff.amount_eur ELSE 0 END), 2) AS municipality_amount,
    ROUND(SUM(CASE WHEN ff.recipient_type_norm IN ('σύνδεσμος', 'σύνδεσμος δήμων') THEN ff.amount_eur ELSE 0 END), 2) AS syndesmos_amount,
    ROUND(SUM(ff.amount_eur), 2) AS total_amount
  FROM filtered_funding ff
  GROUP BY ff.year
),
history AS (
  SELECT
    years.year,
    COALESCE(af.regular_amount, 0) AS regular_amount,
    COALESCE(af.emergency_amount, 0) AS emergency_amount,
    COALESCE(af.municipality_amount, 0) AS municipality_amount,
    COALESCE(af.syndesmos_amount, 0) AS syndesmos_amount,
    COALESCE(af.total_amount, 0) AS total_amount
  FROM generate_series(p_year_start, p_year_main) AS years(year)
  LEFT JOIN aggregated_funding af
    ON af.year = years.year
  ORDER BY years.year
)
SELECT jsonb_build_object(
  'year_main', (SELECT ly.year_main FROM latest_year ly),
  'year_previous', (SELECT ly.year_main - 1 FROM latest_year ly),
  'history_start_year', p_year_start,
  'current_total', COALESCE((SELECT h.total_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'previous_total', COALESCE((SELECT h.total_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main - 1), 0),
  'current_regular_amount', COALESCE((SELECT h.regular_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'current_emergency_amount', COALESCE((SELECT h.emergency_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'current_municipality_amount', COALESCE((SELECT h.municipality_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'current_syndesmos_amount', COALESCE((SELECT h.syndesmos_amount FROM history h JOIN latest_year ly ON h.year = ly.year_main), 0),
  'history', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'year', h.year,
        'regular_amount', h.regular_amount,
        'emergency_amount', h.emergency_amount,
        'municipality_amount', h.municipality_amount,
        'syndesmos_amount', h.syndesmos_amount,
        'total_amount', h.total_amount
      )
      ORDER BY h.year
    )
    FROM history h
  ), '[]'::jsonb)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_homepage_funding(integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- H) Environment Ministry dashboard RPC
-- Source: sql/030_environment_ministry_dashboard_rpc.sql
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_environment_ministry_dashboard(
  p_year integer DEFAULT 2026
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH ministry_org_keys AS (
  SELECT DISTINCT organization_key
  FROM public.organization
  WHERE NULLIF(TRIM(organization_key), '') IS NOT NULL
    AND (
      organization_key = '100015996'
      OR UPPER(BTRIM(COALESCE(organization_normalized_value, organization_value, ''))) = 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'
    )
  UNION
  SELECT '100015996'
),
ministry_name AS (
  SELECT COALESCE(
    MAX(NULLIF(BTRIM(organization_normalized_value), '')),
    MAX(NULLIF(BTRIM(organization_value), '')),
    'Υπουργείο Περιβάλλοντος και Ενέργειας'
  ) AS value
  FROM public.organization
  WHERE organization_key IN (SELECT organization_key FROM ministry_org_keys)
),
payment_agg AS (
  SELECT
    py.procurement_id,
    MAX(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat,
    MAX(COALESCE(py.amount_with_vat, 0)) AS amount_with_vat,
    COALESCE(
      STRING_AGG(
        DISTINCT COALESCE(NULLIF(BTRIM(b.beneficiary_name), ''), NULLIF(BTRIM(pb.beneficiary_vat_number), '')),
        ' | '
        ORDER BY COALESCE(NULLIF(BTRIM(b.beneficiary_name), ''), NULLIF(BTRIM(pb.beneficiary_vat_number), ''))
      ),
      STRING_AGG(DISTINCT NULLIF(BTRIM(py.beneficiary_name), ''), ' | ' ORDER BY NULLIF(BTRIM(py.beneficiary_name), ''))
    ) AS beneficiary_name,
    COALESCE(
      STRING_AGG(DISTINCT NULLIF(BTRIM(pb.beneficiary_vat_number), ''), ' | ' ORDER BY NULLIF(BTRIM(pb.beneficiary_vat_number), '')),
      STRING_AGG(DISTINCT NULLIF(BTRIM(py.beneficiary_vat_number), ''), ' | ' ORDER BY NULLIF(BTRIM(py.beneficiary_vat_number), ''))
    ) AS beneficiary_vat_number,
    STRING_AGG(
      DISTINCT NULLIF(BTRIM(COALESCE(b.gemi, b_py.gemi)), ''),
      ' | '
      ORDER BY NULLIF(BTRIM(COALESCE(b.gemi, b_py.gemi)), '')
    ) AS beneficiary_gemi,
    STRING_AGG(DISTINCT NULLIF(BTRIM(py.signers), ''), ' | ' ORDER BY NULLIF(BTRIM(py.signers), '')) AS signers,
    STRING_AGG(DISTINCT NULLIF(BTRIM(py.payment_ref_no), ''), ' | ' ORDER BY NULLIF(BTRIM(py.payment_ref_no), '')) AS payment_ref_no,
    MAX(py.fiscal_year) AS fiscal_year
  FROM public.payment py
  LEFT JOIN public.payment_beneficiary pb
    ON pb.payment_id = py.id
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = pb.beneficiary_vat_number
  LEFT JOIN public.beneficiary b_py
    ON b_py.beneficiary_vat_number = py.beneficiary_vat_number
  GROUP BY py.procurement_id
),
proc_ranked AS (
  SELECT
    p.id,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.short_descriptions,
    public.normalize_procedure_type(p.procedure_type_value) AS procedure_type_value,
    p.reference_number,
    p.contract_number,
    p.contract_budget,
    p.budget,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.contract_related_ada,
    p.prev_reference_no,
    p.next_ref_no,
    p.diavgeia_ada,
    p.organization_vat_number,
    p.organization_key,
    pa.amount_without_vat,
    pa.amount_with_vat,
    pa.beneficiary_name,
    pa.beneficiary_vat_number,
    pa.beneficiary_gemi,
    pa.signers,
    pa.payment_ref_no,
    pa.fiscal_year,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN payment_agg pa
    ON pa.procurement_id = p.id
  WHERE p.organization_key IN (SELECT organization_key FROM ministry_org_keys)
    AND COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
proc_base AS (
  SELECT pr.*
  FROM proc_ranked pr
  WHERE pr.rn = 1
),
proc_app_window AS (
  SELECT *
  FROM proc_base
  WHERE contract_signed_date IS NOT NULL
    AND contract_signed_date >= DATE '2024-01-01'
    AND contract_signed_date <= LEAST(CURRENT_DATE, make_date(p_year, 12, 31))
),
signed_current_contracts AS (
  SELECT *
  FROM proc_base
  WHERE contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
),
current_year_beneficiaries AS (
  SELECT DISTINCT
    NULLIF(BTRIM(pb.beneficiary_vat_number), '') AS beneficiary_key
  FROM signed_current_contracts sc
  JOIN public.payment py
    ON py.procurement_id = sc.id
  JOIN public.payment_beneficiary pb
    ON pb.payment_id = py.id
  WHERE NULLIF(BTRIM(pb.beneficiary_vat_number), '') IS NOT NULL
),
relevant_contracts AS (
  SELECT
    pb.*,
    (
      pb.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
    ) AS signed_current,
    (
      pb.contract_signed_date < make_date(p_year, 1, 1)
    ) AS active_previous
  FROM proc_base pb
  WHERE (
    pb.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
  ) OR (
    pb.contract_signed_date < make_date(p_year, 1, 1)
    AND pb.end_date >= make_date(p_year, 1, 1)
  )
),
active_contracts AS (
  SELECT *
  FROM proc_base pb
  WHERE (
    pb.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
  ) OR (
    pb.contract_signed_date < make_date(p_year, 1, 1)
    AND pb.end_date >= make_date(p_year, 1, 1)
  )
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    COALESCE(NULLIF(BTRIM(c.cpv_key), ''), '—') AS cpv_key,
    COALESCE(NULLIF(BTRIM(c.cpv_value), ''), '—') AS cpv_value
  FROM public.cpv c
  JOIN proc_base pb
    ON pb.id = c.procurement_id
),
cpv_items AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', cd.cpv_key,
        'label', cd.cpv_value
      )
      ORDER BY cd.cpv_value, cd.cpv_key
    ) AS items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
current_year_top_cpvs AS (
  SELECT
    cd.cpv_value AS label,
    MIN(cd.cpv_key) AS code,
    COUNT(DISTINCT cd.procurement_id)::int AS count
  FROM cpv_dedup cd
  JOIN signed_current_contracts sc
    ON sc.id = cd.procurement_id
  WHERE cd.cpv_value <> '—'
  GROUP BY cd.cpv_value
  ORDER BY count DESC, label
  LIMIT 3
),
active_contract_top_cpvs AS (
  SELECT
    cd.cpv_value AS label,
    MIN(cd.cpv_key) AS code,
    COUNT(DISTINCT cd.procurement_id)::int AS count
  FROM cpv_dedup cd
  JOIN active_contracts ac
    ON ac.id = cd.procurement_id
  WHERE cd.cpv_value <> '—'
  GROUP BY cd.cpv_value
  ORDER BY count DESC, label
  LIMIT 3
),
work_points AS (
  SELECT DISTINCT
    w.id,
    w.procurement_id,
    w.lat,
    w.lon,
    COALESCE(NULLIF(BTRIM(w.work), ''), 'Εργασία πυροπροστασίας') AS work,
    COALESCE(
      NULLIF(BTRIM(w.point_name_canonical), ''),
      NULLIF(BTRIM(w.point_name_raw), ''),
      NULLIF(BTRIM(w.formatted_address), ''),
      'Χωρίς τοπωνύμιο'
    ) AS point_name,
    COALESCE(NULLIF(BTRIM(rc.title), ''), '—') AS contract_title,
    COALESCE(rc.amount_without_vat, COALESCE(rc.contract_budget, rc.budget, 0)) AS amount_without_vat,
    COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_name, ''), '|', 1)), ''), '—') AS beneficiary,
    COALESCE(NULLIF(BTRIM(rc.procedure_type_value), ''), '—') AS assignment_type
  FROM public.works_enriched w
  JOIN relevant_contracts rc
    ON rc.id = w.procurement_id
  WHERE w.lat IS NOT NULL
    AND w.lon IS NOT NULL
),
flow_source AS (
  SELECT *
  FROM relevant_contracts
  WHERE fiscal_year = p_year
  UNION ALL
  SELECT *
  FROM relevant_contracts
  WHERE NOT EXISTS (
    SELECT 1
    FROM relevant_contracts rx
    WHERE rx.fiscal_year = p_year
  )
),
flow_grouped AS (
  SELECT
    COALESCE(NULLIF(BTRIM(split_part(COALESCE(signers, ''), '|', 1)), ''), 'Χωρίς υπογράφοντα') AS signer,
    COALESCE(NULLIF(BTRIM(split_part(COALESCE(beneficiary_name, ''), '|', 1)), ''), 'Χωρίς δικαιούχο') AS beneficiary,
    SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0))) AS total_amount,
    COUNT(*)::int AS contract_count,
    (
      ARRAY_AGG(id ORDER BY COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)) DESC, contract_signed_date DESC NULLS LAST, id DESC)
    )[1] AS lead_procurement_id
  FROM flow_source
  GROUP BY 1, 2
),
featured_contracts AS (
  SELECT
    rc.id,
    jsonb_build_object(
      'id', rc.id,
      'who', (SELECT value FROM ministry_name),
      'what', COALESCE(NULLIF(BTRIM(rc.title), ''), '—'),
      'when', rc.submission_at,
      'why', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.short_descriptions, ''), '|', 1)), ''), '—'),
      'beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_name, ''), '|', 1)), ''), '—'),
      'contract_type', COALESCE(NULLIF(BTRIM(rc.procedure_type_value), ''), '—'),
      'amount_without_vat', COALESCE(rc.amount_without_vat, COALESCE(rc.contract_budget, rc.budget, 0)),
      'amount_with_vat', rc.amount_with_vat,
      'reference_number', COALESCE(NULLIF(BTRIM(rc.reference_number), ''), '—'),
      'contract_number', COALESCE(NULLIF(BTRIM(rc.contract_number), ''), '—'),
      'cpv_items', COALESCE(ci.items, '[]'::jsonb),
      'contract_signed_date', rc.contract_signed_date,
      'start_date', rc.start_date,
      'end_date', rc.end_date,
      'no_end_date', COALESCE(rc.no_end_date, FALSE),
      'organization_vat_number', COALESCE(NULLIF(BTRIM(rc.organization_vat_number), ''), '—'),
      'beneficiary_vat_number', COALESCE(NULLIF(BTRIM(rc.beneficiary_vat_number), ''), '—'),
      'beneficiary_gemi', NULLIF(BTRIM(rc.beneficiary_gemi), ''),
      'signers', COALESCE(NULLIF(BTRIM(rc.signers), ''), '—'),
      'assign_criteria', COALESCE(NULLIF(BTRIM(rc.assign_criteria), ''), '—'),
      'contract_kind', COALESCE(NULLIF(BTRIM(rc.contract_type), ''), '—'),
      'award_procedure', COALESCE(NULLIF(BTRIM(rc.award_procedure), ''), '—'),
      'units_operator', COALESCE(NULLIF(BTRIM(rc.units_operator), ''), '—'),
      'funding_cofund', COALESCE(NULLIF(BTRIM(rc.funding_details_cofund), ''), '—'),
      'funding_self', COALESCE(NULLIF(BTRIM(rc.funding_details_self_fund), ''), '—'),
      'funding_espa', COALESCE(NULLIF(BTRIM(rc.funding_details_espa), ''), '—'),
      'funding_regular', COALESCE(NULLIF(BTRIM(rc.funding_details_regular_budget), ''), '—'),
      'auction_ref_no', COALESCE(NULLIF(BTRIM(rc.auction_ref_no), ''), '—'),
      'payment_ref_no', COALESCE(NULLIF(BTRIM(rc.payment_ref_no), ''), '—'),
      'short_description', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.short_descriptions, ''), '|', 1)), ''), '—'),
      'raw_budget', rc.budget,
      'contract_budget', rc.contract_budget,
      'contract_related_ada', COALESCE(NULLIF(BTRIM(rc.contract_related_ada), ''), '—'),
      'previous_reference_number', COALESCE(NULLIF(BTRIM(rc.prev_reference_no), ''), '—'),
      'next_reference_number', COALESCE(NULLIF(BTRIM(rc.next_ref_no), ''), '—'),
      'diavgeia_ada', COALESCE(NULLIF(BTRIM(rc.diavgeia_ada), ''), '—'),
      'payment_fiscal_year', rc.fiscal_year,
      'primary_signer', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.signers, ''), '|', 1)), ''), 'Χωρίς υπογράφοντα'),
      'primary_beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_name, ''), '|', 1)), ''), 'Χωρίς δικαιούχο'),
      'primary_beneficiary_vat_number', COALESCE(NULLIF(BTRIM(split_part(COALESCE(rc.beneficiary_vat_number, ''), '|', 1)), ''), '—')
    ) AS payload,
    COALESCE(rc.amount_without_vat, COALESCE(rc.contract_budget, rc.budget, 0)) AS sort_amount,
    rc.contract_signed_date
  FROM relevant_contracts rc
  LEFT JOIN cpv_items ci
    ON ci.procurement_id = rc.id
),
recent_active_contracts AS (
  SELECT
    ac.id,
    jsonb_build_object(
      'id', ac.id,
      'who', (SELECT value FROM ministry_name),
      'what', COALESCE(NULLIF(BTRIM(ac.title), ''), '—'),
      'when', ac.submission_at,
      'why', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.short_descriptions, ''), '|', 1)), ''), '—'),
      'beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.beneficiary_name, ''), '|', 1)), ''), '—'),
      'contract_type', COALESCE(NULLIF(BTRIM(ac.procedure_type_value), ''), '—'),
      'amount_without_vat', COALESCE(ac.amount_without_vat, COALESCE(ac.contract_budget, ac.budget, 0)),
      'amount_with_vat', ac.amount_with_vat,
      'reference_number', COALESCE(NULLIF(BTRIM(ac.reference_number), ''), '—'),
      'contract_number', COALESCE(NULLIF(BTRIM(ac.contract_number), ''), '—'),
      'cpv_items', COALESCE(ci.items, '[]'::jsonb),
      'contract_signed_date', ac.contract_signed_date,
      'start_date', ac.start_date,
      'end_date', ac.end_date,
      'no_end_date', COALESCE(ac.no_end_date, FALSE),
      'organization_vat_number', COALESCE(NULLIF(BTRIM(ac.organization_vat_number), ''), '—'),
      'beneficiary_vat_number', COALESCE(NULLIF(BTRIM(ac.beneficiary_vat_number), ''), '—'),
      'beneficiary_gemi', NULLIF(BTRIM(ac.beneficiary_gemi), ''),
      'signers', COALESCE(NULLIF(BTRIM(ac.signers), ''), '—'),
      'assign_criteria', COALESCE(NULLIF(BTRIM(ac.assign_criteria), ''), '—'),
      'contract_kind', COALESCE(NULLIF(BTRIM(ac.contract_type), ''), '—'),
      'award_procedure', COALESCE(NULLIF(BTRIM(ac.award_procedure), ''), '—'),
      'units_operator', COALESCE(NULLIF(BTRIM(ac.units_operator), ''), '—'),
      'funding_cofund', COALESCE(NULLIF(BTRIM(ac.funding_details_cofund), ''), '—'),
      'funding_self', COALESCE(NULLIF(BTRIM(ac.funding_details_self_fund), ''), '—'),
      'funding_espa', COALESCE(NULLIF(BTRIM(ac.funding_details_espa), ''), '—'),
      'funding_regular', COALESCE(NULLIF(BTRIM(ac.funding_details_regular_budget), ''), '—'),
      'auction_ref_no', COALESCE(NULLIF(BTRIM(ac.auction_ref_no), ''), '—'),
      'payment_ref_no', COALESCE(NULLIF(BTRIM(ac.payment_ref_no), ''), '—'),
      'short_description', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.short_descriptions, ''), '|', 1)), ''), '—'),
      'raw_budget', ac.budget,
      'contract_budget', ac.contract_budget,
      'contract_related_ada', COALESCE(NULLIF(BTRIM(ac.contract_related_ada), ''), '—'),
      'previous_reference_number', COALESCE(NULLIF(BTRIM(ac.prev_reference_no), ''), '—'),
      'next_reference_number', COALESCE(NULLIF(BTRIM(ac.next_ref_no), ''), '—'),
      'diavgeia_ada', COALESCE(NULLIF(BTRIM(ac.diavgeia_ada), ''), '—'),
      'payment_fiscal_year', ac.fiscal_year,
      'primary_signer', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.signers, ''), '|', 1)), ''), 'Χωρίς υπογράφοντα'),
      'primary_beneficiary', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.beneficiary_name, ''), '|', 1)), ''), 'Χωρίς δικαιούχο'),
      'primary_beneficiary_vat_number', COALESCE(NULLIF(BTRIM(split_part(COALESCE(ac.beneficiary_vat_number, ''), '|', 1)), ''), '—')
    ) AS payload,
    ac.contract_signed_date
  FROM active_contracts ac
  LEFT JOIN cpv_items ci
    ON ci.procurement_id = ac.id
),
recent_diavgeia_decisions AS (
  SELECT
    d.id,
    jsonb_build_object(
      'id', d.id,
      'org_type', COALESCE(NULLIF(BTRIM(d.org_type), ''), 'ΥΠΟΥΡΓΕΙΟ'),
      'org_name_clean', COALESCE(NULLIF(BTRIM(d.org_name_clean), ''), (SELECT value FROM ministry_name)),
      'subject', COALESCE(NULLIF(BTRIM(d.subject), ''), '—'),
      'decision_date', d.decision_date,
      'ada', COALESCE(NULLIF(BTRIM(d.ada), ''), '—'),
      'diavgeia_document_type_decision_uid', COALESCE(NULLIF(BTRIM(d.diavgeia_document_type_decision_uid), ''), '—'),
      'document_url', NULLIF(BTRIM(d.document_url), ''),
      'spending_contractors_value', NULLIF(BTRIM(d.spending_contractors_value), ''),
      'municipality_key', NULLIF(BTRIM(d.municipality_key), ''),
      'protocol_number', NULLIF(BTRIM(d.protocol_number), ''),
      'thematic_categories', NULLIF(BTRIM(d.thematic_categories), ''),
      'spending_signers', NULLIF(BTRIM(d.spending_signers), ''),
      'spending_contractors_name', NULLIF(BTRIM(d.spending_contractors_name), ''),
      'spending_contractors_afm', NULLIF(BTRIM(d.spending_contractors_afm), '')
    ) AS payload,
    d.decision_date
  FROM (
    SELECT
      dv.*,
      COALESCE(dv.publish_timestamp, dv.submission_timestamp) AS decision_date
    FROM public.diavgeia dv
    WHERE (
        dv.organization_key IN (SELECT organization_key FROM ministry_org_keys)
        OR UPPER(BTRIM(COALESCE(dv.org, ''))) = 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'
        OR UPPER(BTRIM(COALESCE(dv.org_name_clean, ''))) = 'ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'
        OR COALESCE(dv.organization, '') LIKE '%100015996%'
        OR UPPER(COALESCE(dv.organization, '')) LIKE '%ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ%'
      )
      AND COALESCE(dv.publish_timestamp, dv.submission_timestamp)::date
        BETWEEN GREATEST(make_date(p_year, 1, 1), (CURRENT_DATE - INTERVAL '30 days')::date)
        AND LEAST(CURRENT_DATE, make_date(p_year, 12, 31))
    ORDER BY COALESCE(dv.publish_timestamp, dv.submission_timestamp) DESC NULLS LAST, dv.id DESC
    LIMIT 40
  ) d
),
featured_contracts_limited AS (
  SELECT *
  FROM featured_contracts
  ORDER BY sort_amount DESC, contract_signed_date DESC NULLS LAST, id DESC
  LIMIT 8
)
SELECT jsonb_build_object(
  'identification', jsonb_build_object(
    'organization_keys', (
      SELECT jsonb_agg(organization_key ORDER BY organization_key)
      FROM ministry_org_keys
    ),
    'rule', 'procurement.organization_key matches the canonical Environment Ministry organization key set'
  ),
  'ministry_name', (SELECT value FROM ministry_name),
  'total_spend', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
  ), 0),
  'signed_2026_count', COALESCE((
    SELECT COUNT(*)::int
    FROM signed_current_contracts
  ), 0),
  'signed_current_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
  ), 0),
  'active_carryover_count', COALESCE((
    SELECT COUNT(*)::int
    FROM relevant_contracts
    WHERE active_previous
  ), 0),
  'payment_flow_total', COALESCE((
    SELECT SUM(total_amount)
    FROM flow_grouped
  ), 0),
  'direct_award_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
  ), 0),
  'direct_award_with_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NOT NULL
  ), 0),
  'direct_award_without_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM proc_app_window
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NULL
  ), 0),
  'current_year_direct_award_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
  ), 0),
  'current_year_direct_award_with_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NOT NULL
  ), 0),
  'current_year_direct_award_without_auction_amount', COALESCE((
    SELECT SUM(COALESCE(amount_without_vat, COALESCE(contract_budget, budget, 0)))
    FROM signed_current_contracts
    WHERE procedure_type_value = 'Απευθείας ανάθεση'
      AND NULLIF(BTRIM(auction_ref_no), '') IS NULL
  ), 0),
  'current_year_beneficiary_count', COALESCE((
    SELECT COUNT(*)::int
    FROM current_year_beneficiaries
  ), 0),
  'work_points', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'id', CONCAT(wp.id::text, '-', wp.procurement_id::text),
        'procurement_id', wp.procurement_id,
        'lat', wp.lat,
        'lon', wp.lon,
        'work', wp.work,
        'point_name', wp.point_name,
        'contract_title', wp.contract_title,
        'amount_without_vat', wp.amount_without_vat,
        'beneficiary', wp.beneficiary,
        'assignment_type', wp.assignment_type
      )
      ORDER BY wp.procurement_id DESC, wp.id DESC
    )
    FROM work_points wp
  ), '[]'::jsonb),
  'top_cpvs', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'label', tc.label,
        'code', tc.code,
        'count', tc.count,
        'share', CASE
          WHEN (SELECT COUNT(*) FROM active_contracts) = 0 THEN 0
          ELSE ROUND((tc.count::numeric / (SELECT COUNT(*)::numeric FROM active_contracts)), 4)
        END
      )
      ORDER BY tc.count DESC, tc.label
    )
    FROM active_contract_top_cpvs tc
  ), '[]'::jsonb),
  'current_year_top_cpvs', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'label', tc.label,
        'code', tc.code,
        'count', tc.count,
        'share', CASE
          WHEN (SELECT COUNT(*) FROM signed_current_contracts) = 0 THEN 0
          ELSE ROUND((tc.count::numeric / (SELECT COUNT(*)::numeric FROM signed_current_contracts)), 4)
        END
      )
      ORDER BY tc.count DESC, tc.label
    )
    FROM current_year_top_cpvs tc
  ), '[]'::jsonb),
  'active_contract_top_cpvs', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'label', tc.label,
        'code', tc.code,
        'count', tc.count,
        'share', CASE
          WHEN (SELECT COUNT(*) FROM active_contracts) = 0 THEN 0
          ELSE ROUND((tc.count::numeric / (SELECT COUNT(*)::numeric FROM active_contracts)), 4)
        END
      )
      ORDER BY tc.count DESC, tc.label
    )
    FROM active_contract_top_cpvs tc
  ), '[]'::jsonb),
  'flow_rows', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_object(
        'signer', fg.signer,
        'beneficiary', fg.beneficiary,
        'total_amount', fg.total_amount,
        'contract_count', fg.contract_count,
        'ratio', CASE
          WHEN COALESCE((SELECT SUM(total_amount) FROM flow_grouped), 0) = 0 THEN 0
          ELSE ROUND(fg.total_amount / (SELECT SUM(total_amount) FROM flow_grouped), 4)
        END,
        'lead_contract', fc.payload
      )
      ORDER BY fg.total_amount DESC, fg.signer
    )
    FROM flow_grouped fg
    LEFT JOIN featured_contracts fc
      ON fc.id = fg.lead_procurement_id
  ), '[]'::jsonb),
  'featured_contracts', COALESCE((
    SELECT jsonb_agg(payload ORDER BY sort_amount DESC, contract_signed_date DESC NULLS LAST, id DESC)
    FROM featured_contracts_limited
  ), '[]'::jsonb),
  'recent_active_contracts', COALESCE((
    SELECT jsonb_agg(payload ORDER BY contract_signed_date DESC NULLS LAST, id DESC)
    FROM recent_active_contracts
  ), '[]'::jsonb),
  'recent_diavgeia_decisions', COALESCE((
    SELECT jsonb_agg(payload ORDER BY decision_date DESC NULLS LAST, id DESC)
    FROM recent_diavgeia_decisions
  ), '[]'::jsonb)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_environment_ministry_dashboard(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- I) Latest funding-year municipality + syndesmos spend RPC
-- Source: sql/033_latest_funding_year_spend_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_latest_funding_year_municipality_spend();

CREATE OR REPLACE FUNCTION public.get_latest_funding_year_municipality_spend()
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
WITH latest_funding_year AS (
  SELECT MAX(f.year)::int AS year_main
  FROM public.fund f
  WHERE f.year IS NOT NULL
),
payment_agg AS (
  SELECT
    py.procurement_id,
    SUM(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat
  FROM public.payment py
  JOIN latest_funding_year ly
    ON ly.year_main IS NOT NULL
   AND py.fiscal_year = ly.year_main
  WHERE py.procurement_id IS NOT NULL
    AND py.amount_without_vat IS NOT NULL
  GROUP BY py.procurement_id
),
proc_ranked AS (
  SELECT
    p.id AS procurement_id,
    pa.amount_without_vat,
    COALESCE(p.canonical_owner_scope, '') AS canonical_owner_scope,
    COALESCE(org.authority_scope, 'other') AS authority_scope,
    COALESCE(org.organization_normalized_value, org.organization_value, '') AS organization_name,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  JOIN payment_agg pa
    ON pa.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
classified AS (
  SELECT
    pr.procurement_id,
    pr.amount_without_vat,
    (
      pr.canonical_owner_scope = 'municipality'
      OR pr.authority_scope = 'municipality'
    ) AS is_municipality,
    (
      NULLIF(BTRIM(pr.organization_name), '') IS NOT NULL
      AND (
        pr.organization_name ILIKE '%συνδεσμ%'
        OR pr.organization_name ILIKE '%σύνδεσμ%'
      )
    ) AS is_syndesmos
  FROM proc_ranked pr
  WHERE pr.rn = 1
)
SELECT jsonb_build_object(
  'latest_funding_year', (SELECT ly.year_main FROM latest_funding_year ly),
  'municipality_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_municipality
  ), 0),
  'syndesmos_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_syndesmos
  ), 0),
  'total_amount', COALESCE((
    SELECT ROUND(SUM(c.amount_without_vat), 2)
    FROM classified c
    WHERE c.is_municipality OR c.is_syndesmos
  ), 0),
  'municipality_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_municipality
  ), 0),
  'syndesmos_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_syndesmos
  ), 0),
  'total_procurement_count', COALESCE((
    SELECT COUNT(*)
    FROM classified c
    WHERE c.is_municipality OR c.is_syndesmos
  ), 0)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_latest_funding_year_municipality_spend() TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- J) Homepage latest contracts RPC
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_latest_contract_cards(integer);

CREATE OR REPLACE FUNCTION public.get_latest_contract_cards(
  p_limit integer DEFAULT 15
)
RETURNS TABLE (
  procurement_id bigint,
  who text,
  title text,
  submission_at timestamptz,
  contract_signed_date date,
  short_description text,
  procedure_type_value text,
  beneficiary_name text,
  beneficiary_vat_number text,
  beneficiary_gemi text,
  amount_without_vat numeric,
  amount_with_vat numeric,
  reference_number text,
  contract_number text,
  cpv_items jsonb,
  organization_vat_number text,
  signers text,
  assign_criteria text,
  contract_type text,
  award_procedure text,
  units_operator text,
  funding_details_cofund text,
  funding_details_self_fund text,
  funding_details_espa text,
  funding_details_regular_budget text,
  auction_ref_no text,
  payment_ref_no text,
  budget numeric,
  contract_budget numeric,
  contract_related_ada text,
  prev_reference_no text,
  next_ref_no text,
  diavgeia_ada text,
  start_date date,
  end_date date,
  municipality_key text
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $frontend_latest_contracts$
WITH payment_first AS (
  SELECT DISTINCT ON (py.procurement_id)
    py.procurement_id,
    NULLIF(BTRIM(py.beneficiary_name), '') AS beneficiary_name,
    NULLIF(BTRIM(py.beneficiary_vat_number), '') AS beneficiary_vat_number,
    NULLIF(BTRIM(b.gemi), '') AS beneficiary_gemi,
    NULLIF(BTRIM(py.signers), '') AS signers,
    NULLIF(BTRIM(py.payment_ref_no), '') AS payment_ref_no,
    py.amount_without_vat,
    py.amount_with_vat
  FROM public.payment py
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = py.beneficiary_vat_number
  ORDER BY py.procurement_id, py.id
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    COALESCE(NULLIF(BTRIM(c.cpv_key), ''), '—') AS cpv_key,
    COALESCE(NULLIF(BTRIM(c.cpv_value), ''), '—') AS cpv_value
  FROM public.cpv c
),
cpv_agg AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', cd.cpv_key,
        'label', cd.cpv_value
      )
      ORDER BY cd.cpv_value, cd.cpv_key
    ) AS cpv_items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
base AS (
  SELECT
    p.id AS procurement_id,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    NULLIF(BTRIM(split_part(COALESCE(p.short_descriptions, ''), '|', 1)), '') AS short_description,
    p.procedure_type_value,
    pf.beneficiary_name,
    pf.beneficiary_vat_number,
    pf.beneficiary_gemi,
    COALESCE(pf.amount_without_vat, p.contract_budget, p.budget) AS amount_without_vat,
    pf.amount_with_vat,
    p.reference_number,
    p.contract_number,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items,
    p.organization_vat_number,
    pf.signers,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    pf.payment_ref_no,
    p.budget,
    p.contract_budget,
    p.contract_related_ada,
    p.prev_reference_no,
    p.next_ref_no,
    p.diavgeia_ada,
    p.start_date,
    p.end_date,
    p.canonical_owner_scope,
    COALESCE(org.organization_normalized_value, org.organization_value, p.organization_key) AS organization_name,
    COALESCE(org.authority_scope, 'other') AS organization_scope,
    p.municipality_key,
    NULLIF(BTRIM(mun.municipality_normalized_value), '') AS municipality_label,
    COALESCE(
      NULLIF(BTRIM(reg.region_normalized_value), ''),
      NULLIF(BTRIM(reg.region_value), '')
    ) AS region_label
  FROM public.procurement p
  LEFT JOIN payment_first pf
    ON pf.procurement_id = p.id
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = p.id
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      m.municipality_normalized_value
    FROM public.municipality_normalized_name m
    WHERE m.municipality_key = p.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) mun ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      r.region_normalized_value,
      r.region_value
    FROM public.region r
    WHERE r.region_key = p.region_key
    ORDER BY r.id
    LIMIT 1
  ) reg ON TRUE
  WHERE p.submission_at IS NOT NULL
),
enriched AS (
  SELECT
    b.*,
    CASE
      WHEN lower(COALESCE(b.canonical_owner_scope, '')) = 'municipality' THEN COALESCE(
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Δήμος —'
      )
      WHEN lower(COALESCE(b.canonical_owner_scope, '')) = 'region' THEN COALESCE(
        CASE
          WHEN b.region_label IS NULL THEN NULL
          WHEN b.region_label LIKE 'ΠΕΡΙΦΕΡΕΙΑ %' THEN b.region_label
          ELSE CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', b.region_label)
        END,
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Περιφέρεια —'
      )
      WHEN lower(COALESCE(b.organization_scope, '')) = 'municipality' THEN COALESCE(
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Δήμος —'
      )
      WHEN lower(COALESCE(b.organization_scope, '')) IN ('region', 'decentralized') THEN COALESCE(
        CASE
          WHEN b.region_label IS NULL THEN NULL
          WHEN b.region_label LIKE 'ΠΕΡΙΦΕΡΕΙΑ %' THEN b.region_label
          ELSE CONCAT('ΠΕΡΙΦΕΡΕΙΑ ', b.region_label)
        END,
        CASE
          WHEN b.municipality_label IS NULL THEN NULL
          WHEN b.municipality_label LIKE 'ΔΗΜΟΣ %' THEN b.municipality_label
          ELSE CONCAT('ΔΗΜΟΣ ', b.municipality_label)
        END,
        b.organization_name,
        'Περιφέρεια —'
      )
      ELSE COALESCE(b.organization_name, b.municipality_label, b.region_label, '—')
    END AS who
  FROM base b
),
ranked AS (
  SELECT
    e.*,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(e.reference_number), ''),
        NULLIF(BTRIM(e.contract_number), ''),
        NULLIF(BTRIM(COALESCE(e.contract_related_ada, e.diavgeia_ada)), ''),
        CONCAT_WS(
          '|',
          COALESCE(e.who, ''),
          COALESCE(e.title, ''),
          COALESCE(e.contract_signed_date::text, ''),
          COALESCE(e.amount_without_vat::text, '')
        )
      )
      ORDER BY e.submission_at DESC NULLS LAST, e.procurement_id DESC
    ) AS rn
  FROM enriched e
)
SELECT
  r.procurement_id,
  r.who,
  r.title,
  r.submission_at,
  r.contract_signed_date,
  r.short_description,
  r.procedure_type_value,
  r.beneficiary_name,
  r.beneficiary_vat_number,
  r.beneficiary_gemi,
  r.amount_without_vat,
  r.amount_with_vat,
  r.reference_number,
  r.contract_number,
  r.cpv_items,
  r.organization_vat_number,
  r.signers,
  r.assign_criteria,
  r.contract_type,
  r.award_procedure,
  r.units_operator,
  r.funding_details_cofund,
  r.funding_details_self_fund,
  r.funding_details_espa,
  r.funding_details_regular_budget,
  r.auction_ref_no,
  r.payment_ref_no,
  r.budget,
  r.contract_budget,
  r.contract_related_ada,
  r.prev_reference_no,
  r.next_ref_no,
  r.diavgeia_ada,
  r.start_date,
  r.end_date,
  r.municipality_key
FROM ranked r
WHERE r.rn = 1
ORDER BY r.submission_at DESC NULLS LAST, r.procurement_id DESC
LIMIT GREATEST(COALESCE(p_limit, 15), 1);
$frontend_latest_contracts$;

GRANT EXECUTE ON FUNCTION public.get_latest_contract_cards(integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- K) Municipality featured beneficiaries RPC
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_municipality_featured_beneficiaries(text, integer, integer);

CREATE OR REPLACE FUNCTION public.get_municipality_featured_beneficiaries(
  p_municipality_key text,
  p_year integer,
  p_limit integer DEFAULT 12
)
RETURNS TABLE (
  beneficiary_name text,
  beneficiary_vat_number text,
  beneficiary_gemi text,
  organization text,
  total_amount numeric,
  contract_count integer,
  cpv text,
  start_date date,
  end_date date,
  duration_days integer,
  progress_pct numeric,
  signer text,
  relevant_contracts jsonb
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH municipality_lookup AS (
  SELECT COALESCE(
    NULLIF(BTRIM(m.municipality_normalized_value), ''),
    NULLIF(BTRIM(m.municipality_value), ''),
    p_municipality_key
  ) AS municipality_label
  FROM public.municipality m
  WHERE m.municipality_key = p_municipality_key
  ORDER BY m.id
  LIMIT 1
),
payment_agg AS (
  SELECT
    py.procurement_id,
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      CONCAT('name:', COALESCE(NULLIF(BTRIM(py.beneficiary_name), ''), '—'))
    ) AS beneficiary_key,
    NULLIF(BTRIM(py.beneficiary_vat_number), '') AS beneficiary_vat_number,
    NULLIF(BTRIM(b.gemi), '') AS beneficiary_gemi,
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_name), ''),
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      '—'
    ) AS beneficiary_name,
    SUM(COALESCE(py.amount_without_vat, 0)) AS amount_without_vat,
    SUM(COALESCE(py.amount_with_vat, 0)) AS amount_with_vat,
    (array_agg(NULLIF(BTRIM(py.signers), '') ORDER BY py.id) FILTER (WHERE NULLIF(BTRIM(py.signers), '') IS NOT NULL))[1] AS signers,
    (array_agg(NULLIF(BTRIM(py.payment_ref_no), '') ORDER BY py.id) FILTER (WHERE NULLIF(BTRIM(py.payment_ref_no), '') IS NOT NULL))[1] AS payment_ref_no
  FROM public.payment py
  LEFT JOIN public.beneficiary b
    ON b.beneficiary_vat_number = py.beneficiary_vat_number
  GROUP BY
    py.procurement_id,
    NULLIF(BTRIM(b.gemi), ''),
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      CONCAT('name:', COALESCE(NULLIF(BTRIM(py.beneficiary_name), ''), '—'))
    ),
    NULLIF(BTRIM(py.beneficiary_vat_number), ''),
    COALESCE(
      NULLIF(BTRIM(py.beneficiary_name), ''),
      NULLIF(BTRIM(py.beneficiary_vat_number), ''),
      '—'
    )
),
cpv_dedup AS (
  SELECT DISTINCT
    c.procurement_id,
    COALESCE(NULLIF(BTRIM(c.cpv_key), ''), '—') AS cpv_key,
    COALESCE(NULLIF(BTRIM(c.cpv_value), ''), '—') AS cpv_value
  FROM public.cpv c
),
cpv_agg AS (
  SELECT
    cd.procurement_id,
    jsonb_agg(
      jsonb_build_object(
        'code', cd.cpv_key,
        'label', cd.cpv_value
      )
      ORDER BY cd.cpv_value, cd.cpv_key
    ) AS cpv_items
  FROM cpv_dedup cd
  GROUP BY cd.procurement_id
),
base AS (
  SELECT
    p.id AS procurement_id,
    p.title,
    p.submission_at,
    p.contract_signed_date,
    NULLIF(BTRIM(split_part(COALESCE(p.short_descriptions, ''), '|', 1)), '') AS short_description,
    p.procedure_type_value,
    p.reference_number,
    p.contract_number,
    p.contract_budget,
    p.budget,
    p.assign_criteria,
    p.contract_type,
    p.award_procedure,
    p.units_operator,
    p.funding_details_cofund,
    p.funding_details_self_fund,
    p.funding_details_espa,
    p.funding_details_regular_budget,
    p.auction_ref_no,
    p.prev_reference_no,
    p.next_ref_no,
    p.contract_related_ada,
    p.organization_vat_number,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.diavgeia_ada,
    COALESCE(org.organization_normalized_value, org.organization_value, ml.municipality_label, '—') AS organization_value,
    CASE
      WHEN COALESCE(p.canonical_owner_scope, '') = 'municipality' THEN 'municipality'
      ELSE COALESCE(org.authority_scope, 'other')
    END AS authority_scope,
    pa.beneficiary_key,
    pa.beneficiary_vat_number,
    pa.beneficiary_gemi,
    pa.beneficiary_name,
    pa.amount_without_vat,
    pa.amount_with_vat,
    pa.signers,
    pa.payment_ref_no,
    COALESCE(ca.cpv_items, '[]'::jsonb) AS cpv_items
  FROM public.procurement p
  JOIN payment_agg pa
    ON pa.procurement_id = p.id
  CROSS JOIN municipality_lookup ml
  LEFT JOIN LATERAL (
    SELECT
      o.organization_normalized_value,
      o.organization_value,
      o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = p.organization_key
    ORDER BY o.id
    LIMIT 1
  ) org ON TRUE
  LEFT JOIN cpv_agg ca
    ON ca.procurement_id = p.id
  WHERE p.municipality_key = p_municipality_key
    AND COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND p.contract_signed_date IS NOT NULL
    AND (
      p.contract_signed_date BETWEEN make_date(p_year, 1, 1) AND make_date(p_year, 12, 31)
      OR (
        p.contract_signed_date < make_date(p_year, 1, 1)
        AND COALESCE(p.no_end_date, FALSE) = FALSE
        AND p.end_date IS NOT NULL
        AND p.end_date >= make_date(p_year, 1, 1)
      )
    )
),
filtered AS (
  SELECT *
  FROM base
  WHERE authority_scope = 'municipality'
),
beneficiary_totals AS (
  SELECT
    f.beneficiary_key,
    SUM(f.amount_without_vat) AS total_amount,
    COUNT(DISTINCT f.procurement_id)::integer AS contract_count,
    MIN(f.start_date) AS start_date,
    MAX(f.end_date) AS end_date
  FROM filtered f
  GROUP BY f.beneficiary_key
),
top_beneficiaries AS (
  SELECT
    bt.beneficiary_key,
    bt.total_amount,
    bt.contract_count,
    bt.start_date,
    bt.end_date
  FROM beneficiary_totals bt
  ORDER BY bt.total_amount DESC, bt.contract_count DESC, bt.beneficiary_key
  LIMIT GREATEST(COALESCE(p_limit, 12), 1)
),
name_ranked AS (
  SELECT
    f.beneficiary_key,
    f.beneficiary_name,
    f.beneficiary_vat_number,
    ROW_NUMBER() OVER (
      PARTITION BY f.beneficiary_key
      ORDER BY f.contract_signed_date DESC NULLS LAST, f.procurement_id DESC
    ) AS rn
  FROM filtered f
),
org_totals AS (
  SELECT
    f.beneficiary_key,
    f.organization_value,
    SUM(f.amount_without_vat) AS total_amount
  FROM filtered f
  GROUP BY f.beneficiary_key, f.organization_value
),
org_ranked AS (
  SELECT
    ot.beneficiary_key,
    ot.organization_value,
    ROW_NUMBER() OVER (
      PARTITION BY ot.beneficiary_key
      ORDER BY ot.total_amount DESC, ot.organization_value
    ) AS rn
  FROM org_totals ot
),
signer_counts AS (
  SELECT
    f.beneficiary_key,
    f.signers,
    COUNT(*) AS signer_count
  FROM filtered f
  WHERE NULLIF(BTRIM(f.signers), '') IS NOT NULL
  GROUP BY f.beneficiary_key, f.signers
),
signer_ranked AS (
  SELECT
    sc.beneficiary_key,
    sc.signers,
    ROW_NUMBER() OVER (
      PARTITION BY sc.beneficiary_key
      ORDER BY sc.signer_count DESC, sc.signers
    ) AS rn
  FROM signer_counts sc
),
cpv_counts AS (
  SELECT
    f.beneficiary_key,
    cpv_item ->> 'label' AS cpv_label,
    COUNT(*) AS cpv_count
  FROM filtered f
  CROSS JOIN LATERAL jsonb_array_elements(f.cpv_items) cpv_item
  WHERE NULLIF(BTRIM(cpv_item ->> 'label'), '') IS NOT NULL
    AND cpv_item ->> 'label' <> '—'
  GROUP BY f.beneficiary_key, cpv_item ->> 'label'
),
cpv_ranked AS (
  SELECT
    cc.beneficiary_key,
    cc.cpv_label,
    ROW_NUMBER() OVER (
      PARTITION BY cc.beneficiary_key
      ORDER BY cc.cpv_count DESC, cc.cpv_label
    ) AS rn
  FROM cpv_counts cc
),
relevant_ranked AS (
  SELECT
    f.beneficiary_key,
    jsonb_build_object(
      'id', f.procurement_id,
      'organization', f.organization_value,
      'title', f.title,
      'submission_at', f.submission_at,
      'short_description', f.short_description,
      'procedure_type_value', f.procedure_type_value,
      'amount_without_vat', f.amount_without_vat,
      'amount_with_vat', f.amount_with_vat,
      'reference_number', f.reference_number,
      'contract_number', f.contract_number,
      'cpv_items', f.cpv_items,
      'contract_signed_date', f.contract_signed_date,
      'start_date', f.start_date,
      'end_date', f.end_date,
      'organization_vat_number', f.organization_vat_number,
      'beneficiary_vat_number', f.beneficiary_vat_number,
      'beneficiary_gemi', f.beneficiary_gemi,
      'beneficiary_name', f.beneficiary_name,
      'signers', f.signers,
      'assign_criteria', f.assign_criteria,
      'contract_type', f.contract_type,
      'award_procedure', f.award_procedure,
      'units_operator', f.units_operator,
      'funding_details_cofund', f.funding_details_cofund,
      'funding_details_self_fund', f.funding_details_self_fund,
      'funding_details_espa', f.funding_details_espa,
      'funding_details_regular_budget', f.funding_details_regular_budget,
      'auction_ref_no', f.auction_ref_no,
      'payment_ref_no', f.payment_ref_no,
      'budget', f.budget,
      'contract_budget', f.contract_budget,
      'contract_related_ada', f.contract_related_ada,
      'prev_reference_no', f.prev_reference_no,
      'next_ref_no', f.next_ref_no,
      'diavgeia_ada', f.diavgeia_ada
    ) AS contract_json,
    ROW_NUMBER() OVER (
      PARTITION BY f.beneficiary_key
      ORDER BY f.amount_without_vat DESC, f.contract_signed_date DESC NULLS LAST, f.procurement_id DESC
    ) AS rn
  FROM filtered f
  JOIN top_beneficiaries tb
    ON tb.beneficiary_key = f.beneficiary_key
),
relevant_agg AS (
  SELECT
    rr.beneficiary_key,
    jsonb_agg(rr.contract_json ORDER BY rr.rn) AS relevant_contracts
  FROM relevant_ranked rr
  WHERE rr.rn <= 5
  GROUP BY rr.beneficiary_key
)
SELECT
  COALESCE(nr.beneficiary_name, '—') AS beneficiary_name,
  nr.beneficiary_vat_number,
  NULLIF(BTRIM(bg.gemi), '') AS beneficiary_gemi,
  COALESCE(orx.organization_value, '—') AS organization,
  tb.total_amount,
  tb.contract_count,
  COALESCE(cr.cpv_label, '—') AS cpv,
  tb.start_date,
  tb.end_date,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date < tb.start_date THEN NULL
    ELSE (tb.end_date - tb.start_date + 1)
  END::integer AS duration_days,
  CASE
    WHEN tb.start_date IS NULL OR tb.end_date IS NULL OR tb.end_date <= tb.start_date THEN NULL
    WHEN CURRENT_DATE <= tb.start_date THEN 0
    WHEN CURRENT_DATE >= tb.end_date THEN 100
    ELSE ROUND((((CURRENT_DATE - tb.start_date)::numeric / NULLIF((tb.end_date - tb.start_date)::numeric, 0)) * 100)::numeric, 2)
  END AS progress_pct,
  COALESCE(sr.signers, '—') AS signer,
  COALESCE(ra.relevant_contracts, '[]'::jsonb) AS relevant_contracts
FROM top_beneficiaries tb
LEFT JOIN name_ranked nr
  ON nr.beneficiary_key = tb.beneficiary_key
 AND nr.rn = 1
LEFT JOIN public.beneficiary bg
  ON bg.beneficiary_vat_number = nr.beneficiary_vat_number
LEFT JOIN org_ranked orx
  ON orx.beneficiary_key = tb.beneficiary_key
 AND orx.rn = 1
LEFT JOIN signer_ranked sr
  ON sr.beneficiary_key = tb.beneficiary_key
 AND sr.rn = 1
LEFT JOIN cpv_ranked cr
  ON cr.beneficiary_key = tb.beneficiary_key
 AND cr.rn = 1
LEFT JOIN relevant_agg ra
  ON ra.beneficiary_key = tb.beneficiary_key
ORDER BY tb.total_amount DESC, tb.contract_count DESC, COALESCE(nr.beneficiary_name, tb.beneficiary_key);
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_featured_beneficiaries(text, integer, integer) TO anon, authenticated, service_role;

-- -------------------------------------------------------------
-- K2) News fire article geocoding table
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.news_fires (
  id                  BIGSERIAL PRIMARY KEY,
  article_title       TEXT NOT NULL,
  source              TEXT NOT NULL,
  article_url         TEXT NOT NULL,
  image_url           TEXT,
  published_at        TIMESTAMPTZ,
  scraped_at          TIMESTAMPTZ NOT NULL,
  municipality_key    TEXT,
  municipality_name   TEXT,
  area                TEXT,
  geocode_query       TEXT,
  lat                 NUMERIC(10, 6),
  lon                 NUMERIC(10, 6),
  CONSTRAINT uq_news_fires_article_url UNIQUE (article_url),
  CONSTRAINT fk_news_fires_municipality_key
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  CONSTRAINT chk_news_fires_article_url
    CHECK (BTRIM(article_url) <> ''),
  CONSTRAINT chk_news_fires_source
    CHECK (BTRIM(source) <> ''),
  CONSTRAINT chk_news_fires_lat
    CHECK (lat IS NULL OR (lat >= -90 AND lat <= 90)),
  CONSTRAINT chk_news_fires_lon
    CHECK (lon IS NULL OR (lon >= -180 AND lon <= 180))
);

CREATE INDEX IF NOT EXISTS idx_news_fires_published_at
  ON public.news_fires (published_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_news_fires_scraped_at
  ON public.news_fires (scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_fires_municipality_key
  ON public.news_fires (municipality_key);

CREATE INDEX IF NOT EXISTS idx_news_fires_lat_lon
  ON public.news_fires (lat, lon);

-- Homepage RPC performance indexes.
CREATE INDEX IF NOT EXISTS idx_payment_procurement_id_id_cover
  ON public.payment (procurement_id, id)
  INCLUDE (
    amount_without_vat,
    amount_with_vat,
    beneficiary_name,
    beneficiary_vat_number,
    signers,
    payment_ref_no,
    fiscal_year
  );

CREATE INDEX IF NOT EXISTS idx_payment_fiscal_year_procurement_amount
  ON public.payment (fiscal_year, procurement_id)
  INCLUDE (amount_without_vat)
  WHERE amount_without_vat IS NOT NULL
    AND procurement_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_payment_beneficiary_payment_vat
  ON public.payment_beneficiary (payment_id, beneficiary_vat_number);

CREATE INDEX IF NOT EXISTS idx_payment_beneficiary_vat_payment
  ON public.payment_beneficiary (beneficiary_vat_number, payment_id);

CREATE INDEX IF NOT EXISTS idx_cpv_procurement_value_key
  ON public.cpv (procurement_id, cpv_value, cpv_key);

CREATE INDEX IF NOT EXISTS idx_procurement_submission_id_not_null
  ON public.procurement (submission_at DESC NULLS LAST, id DESC)
  WHERE submission_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_procurement_contract_signed_id_active
  ON public.procurement (contract_signed_date DESC NULLS LAST, id DESC)
  WHERE contract_signed_date IS NOT NULL
    AND COALESCE(cancelled, FALSE) = FALSE;

CREATE INDEX IF NOT EXISTS idx_procurement_prev_reference_no_trim
  ON public.procurement ((NULLIF(BTRIM(prev_reference_no), '')))
  WHERE NULLIF(BTRIM(prev_reference_no), '') IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_procurement_next_ref_no_trim
  ON public.procurement ((NULLIF(BTRIM(next_ref_no), '')))
  WHERE NULLIF(BTRIM(next_ref_no), '') IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_procurement_reference_contract_dedup
  ON public.procurement (
    reference_number,
    diavgeia_ada,
    contract_number,
    organization_key,
    contract_signed_date,
    id DESC
  );

CREATE INDEX IF NOT EXISTS idx_organization_key_id_cover
  ON public.organization (organization_key, id)
  INCLUDE (organization_normalized_value, organization_value, authority_scope);

CREATE INDEX IF NOT EXISTS idx_municipality_normalized_key_id_cover
  ON public.municipality_normalized_name (municipality_key, id)
  INCLUDE (municipality_normalized_value);

CREATE INDEX IF NOT EXISTS idx_region_key_id_cover
  ON public.region (region_key, id)
  INCLUDE (region_normalized_value, region_value);

-- -------------------------------------------------------------
-- K2) Copernicus fires spanning multiple municipalities
-- Source: sql/063_create_copernicus_municipality.sql
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.copernicus_municipality (
  copernicus_id     BIGINT NOT NULL,
  municipality_key TEXT NOT NULL,
  match_method      TEXT NOT NULL,
  overlap_ratio     NUMERIC(10, 6) NOT NULL,
  overlap_area_ha   NUMERIC(12, 2),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (copernicus_id, municipality_key),
  CONSTRAINT fk_copernicus_municipality_fire
    FOREIGN KEY (copernicus_id)
    REFERENCES public.copernicus(copernicus_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_copernicus_municipality_municipality
    FOREIGN KEY (municipality_key)
    REFERENCES public.municipality_normalized_name(municipality_key)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT chk_copernicus_municipality_overlap_ratio
    CHECK (overlap_ratio > 0 AND overlap_ratio <= 1),
  CONSTRAINT chk_copernicus_municipality_overlap_area
    CHECK (overlap_area_ha IS NULL OR overlap_area_ha >= 0)
);

CREATE INDEX IF NOT EXISTS idx_copernicus_municipality_key
  ON public.copernicus_municipality (municipality_key, copernicus_id);

INSERT INTO public.copernicus_municipality (
  copernicus_id,
  municipality_key,
  match_method,
  overlap_ratio,
  overlap_area_ha
)
SELECT
  copernicus_id,
  municipality_key,
  COALESCE(municipality_match_method, 'legacy_primary'),
  COALESCE(NULLIF(municipality_overlap_ratio, 0), 1),
  CASE
    WHEN area_ha IS NULL THEN NULL
    ELSE ROUND(area_ha * COALESCE(NULLIF(municipality_overlap_ratio, 0), 1), 2)
  END
FROM public.copernicus
WHERE municipality_key IS NOT NULL
ON CONFLICT (copernicus_id, municipality_key) DO NOTHING;

CREATE OR REPLACE VIEW public.copernicus_municipality_fire
WITH (security_invoker = true) AS
SELECT
  cm.copernicus_id,
  cm.municipality_key,
  cm.match_method,
  cm.overlap_ratio,
  cm.overlap_area_ha,
  c.firedate,
  c.area_ha AS total_area_ha,
  c.centroid,
  c.shape
FROM public.copernicus_municipality cm
JOIN public.copernicus c
  ON c.copernicus_id = cm.copernicus_id;

-- -------------------------------------------------------------
-- L) Table/function grants for frontend reads
-- -------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated, service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT EXECUTE ON FUNCTIONS TO service_role;

-- -------------------------------------------------------------
-- M) Enable RLS and keep public frontend reads working
-- -------------------------------------------------------------
DO $frontend_enable_rls$
DECLARE
  table_name text;
BEGIN
  FOR table_name IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
  LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', table_name);
  END LOOP;
END
$frontend_enable_rls$;

DROP POLICY IF EXISTS public_read_procurement ON public.procurement;
DROP POLICY IF EXISTS anon_select_procurement ON public.procurement;
CREATE POLICY public_read_procurement
ON public.procurement
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_payment ON public.payment;
CREATE POLICY public_read_payment
ON public.payment
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_beneficiary ON public.beneficiary;
CREATE POLICY public_read_beneficiary
ON public.beneficiary
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_cpv ON public.cpv;
CREATE POLICY public_read_cpv
ON public.cpv
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_organization ON public.organization;
CREATE POLICY public_read_organization
ON public.organization
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_region ON public.region;
CREATE POLICY public_read_region
ON public.region
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality ON public.municipality;
CREATE POLICY public_read_municipality
ON public.municipality
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality_normalized_name ON public.municipality_normalized_name;
CREATE POLICY public_read_municipality_normalized_name
ON public.municipality_normalized_name
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_municipality_fire_protection_data ON public.municipality_fire_protection_data;
CREATE POLICY public_read_municipality_fire_protection_data
ON public.municipality_fire_protection_data
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_forest_fire ON public.forest_fire;
CREATE POLICY public_read_forest_fire
ON public.forest_fire
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_fund ON public.fund;
CREATE POLICY public_read_fund
ON public.fund
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_copernicus ON public.copernicus;
CREATE POLICY public_read_copernicus
ON public.copernicus
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_copernicus_municipality
  ON public.copernicus_municipality;
CREATE POLICY public_read_copernicus_municipality
ON public.copernicus_municipality
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_works ON public.works;
CREATE POLICY public_read_works
ON public.works
FOR SELECT
TO anon, authenticated
USING (true);

DROP POLICY IF EXISTS public_read_news_fires ON public.news_fires;
CREATE POLICY public_read_news_fires
ON public.news_fires
FOR SELECT
TO anon, authenticated
USING (true);

GRANT SELECT ON public.news_fires TO anon, authenticated, service_role;

ALTER TABLE public.current_fires
  ADD COLUMN IF NOT EXISTS lat NUMERIC(9, 6),
  ADD COLUMN IF NOT EXISTS lon NUMERIC(9, 6),
  ADD COLUMN IF NOT EXISTS formatted_address TEXT,
  ADD COLUMN IF NOT EXISTS place_id TEXT,
  ADD COLUMN IF NOT EXISTS geocode_query TEXT,
  ADD COLUMN IF NOT EXISTS geocoded_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'fireservice_live_page',
  ADD COLUMN IF NOT EXISTS source_account TEXT,
  ADD COLUMN IF NOT EXISTS source_location TEXT,
  ADD COLUMN IF NOT EXISTS source_post_id TEXT,
  ADD COLUMN IF NOT EXISTS source_url TEXT;

UPDATE public.current_fires
SET source = 'fireservice_live_page'
WHERE source IS NULL OR btrim(source) = '';

UPDATE public.current_fires
SET source_location = NULLIF(
  raw::jsonb -> 'posts' -> -1 -> 'extracted' ->> 'location_name',
  ''
)
WHERE source = 'fireservice_x'
  AND source_location IS NULL
  AND raw IS NOT NULL
  AND raw ~ '^\s*\{';

ALTER TABLE public.current_fires
  DROP COLUMN IF EXISTS is_112_notice,
  DROP COLUMN IF EXISTS citizen_instructions_112,
  DROP COLUMN IF EXISTS citizen_instructions_112_geocoded;

CREATE INDEX IF NOT EXISTS idx_current_fires_lat_lon
  ON public.current_fires (lat, lon);

CREATE INDEX IF NOT EXISTS idx_current_fires_source
  ON public.current_fires (source);

DROP POLICY IF EXISTS public_read_current_fires ON public.current_fires;
CREATE POLICY public_read_current_fires
ON public.current_fires
FOR SELECT
TO anon, authenticated
USING (true);

-- -------------------------------------------------------------
-- M1) News ticker RPC
-- Source: sql/059_news_ticker_articles_rpc.sql
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_news_ticker_articles();

CREATE OR REPLACE FUNCTION public.get_news_ticker_articles()
RETURNS TABLE (
  id bigint,
  article_title text,
  source text,
  article_url text,
  image_url text,
  published_at timestamptz,
  municipality_name text,
  area text,
  active_fire_incident_key text
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '10s'
AS $news_ticker_articles$
WITH today_rows AS (
  SELECT
    nf.id,
    nf.article_title,
    nf.source,
    nf.article_url,
    nf.image_url,
    nf.published_at,
    nf.scraped_at,
    nf.municipality_key,
    nf.municipality_name,
    nf.area
  FROM public.news_fires nf
  WHERE nf.published_at IS NOT NULL
    AND (nf.published_at AT TIME ZONE 'Europe/Athens')::date = (NOW() AT TIME ZONE 'Europe/Athens')::date
),
today_count AS (
  SELECT COUNT(*) AS article_count
  FROM today_rows
),
latest_rows AS (
  SELECT
    nf.id,
    nf.article_title,
    nf.source,
    nf.article_url,
    nf.image_url,
    nf.published_at,
    nf.scraped_at,
    nf.municipality_key,
    nf.municipality_name,
    nf.area
  FROM public.news_fires nf
  ORDER BY nf.published_at DESC NULLS LAST, nf.scraped_at DESC, nf.id DESC
  LIMIT 10
),
selected_rows AS (
  SELECT tr.*
  FROM today_rows tr
  WHERE (SELECT article_count FROM today_count) >= 5

  UNION ALL

  SELECT lr.*
  FROM latest_rows lr
  WHERE (SELECT article_count FROM today_count) < 5
),
active_fire_by_municipality AS (
  SELECT DISTINCT ON (cf.municipality_key)
    cf.municipality_key,
    cf.incident_key
  FROM public.current_fires cf
  WHERE cf.is_current IS TRUE
    AND cf.municipality_key IS NOT NULL
    AND (cf.status IS NULL OR cf.status <> 'ΛΗΞΗ')
  ORDER BY
    cf.municipality_key,
    cf.status_updated_at DESC NULLS LAST,
    cf.last_seen_at DESC NULLS LAST,
    cf.incident_key
)
SELECT
  sr.id,
  sr.article_title,
  sr.source,
  sr.article_url,
  sr.image_url,
  sr.published_at,
  sr.municipality_name,
  sr.area,
  af.incident_key AS active_fire_incident_key
FROM selected_rows sr
LEFT JOIN active_fire_by_municipality af
  ON af.municipality_key = sr.municipality_key
ORDER BY sr.published_at DESC NULLS LAST, sr.scraped_at DESC, sr.id DESC;
$news_ticker_articles$;

GRANT EXECUTE ON FUNCTION public.get_news_ticker_articles() TO anon, authenticated, service_role;

CREATE TABLE IF NOT EXISTS public."112_notice" (
  notice_id TEXT PRIMARY KEY,
  current_fire_incident_key TEXT REFERENCES public.current_fires (incident_key)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
  source TEXT NOT NULL DEFAULT 'x',
  account TEXT NOT NULL DEFAULT '112Greece',
  post_id TEXT NOT NULL UNIQUE,
  post_url TEXT NOT NULL,
  posted_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  notice_type TEXT,
  notice_text TEXT NOT NULL,
  instructions_geocoded JSONB NOT NULL,
  municipality_keys TEXT[],
  matched_municipality_key TEXT,
  raw JSONB NOT NULL
);

ALTER TABLE public."112_notice"
  ADD COLUMN IF NOT EXISTS municipality_keys TEXT[],
  ADD COLUMN IF NOT EXISTS matched_municipality_key TEXT,
  DROP COLUMN IF EXISTS match_score,
  DROP COLUMN IF EXISTS match_reasons,
  DROP COLUMN IF EXISTS match_distance_km;

ALTER TABLE public."112_notice" ENABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS idx_112_notice_current_fire_incident_key
  ON public."112_notice" (current_fire_incident_key);

CREATE INDEX IF NOT EXISTS idx_112_notice_matched_municipality_key
  ON public."112_notice" (matched_municipality_key);

CREATE INDEX IF NOT EXISTS idx_112_notice_posted_at
  ON public."112_notice" (posted_at DESC);

CREATE INDEX IF NOT EXISTS idx_112_notice_instructions_geocoded_gin
  ON public."112_notice"
  USING GIN (instructions_geocoded);

DROP POLICY IF EXISTS public_read_112_notice ON public."112_notice";
CREATE POLICY public_read_112_notice
ON public."112_notice"
FOR SELECT
TO anon, authenticated
USING (true);

CREATE OR REPLACE FUNCTION public.sync_current_fire_coordinates_from_firms()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_updated_count integer;
BEGIN
  WITH current_fire_counts AS (
    SELECT
      municipality_key,
      COUNT(*) AS current_fire_count
    FROM public.current_fires
    WHERE is_current IS TRUE
      AND municipality_key IS NOT NULL
      AND (status IS NULL OR status <> 'ΛΗΞΗ')
    GROUP BY municipality_key
  ),
  eligible_current_fires AS (
    SELECT
      current_fires.incident_key,
      current_fires.municipality_key
    FROM public.current_fires
    INNER JOIN current_fire_counts
      ON current_fire_counts.municipality_key = current_fires.municipality_key
    WHERE current_fires.is_current IS TRUE
      AND current_fire_counts.current_fire_count = 1
      AND (current_fires.status IS NULL OR current_fires.status <> 'ΛΗΞΗ')
  ),
  ranked_firms_detections AS (
    SELECT
      firms_active_fire_detections.municipality_key,
      firms_active_fire_detections.latitude,
      firms_active_fire_detections.longitude,
      ROW_NUMBER() OVER (
        PARTITION BY firms_active_fire_detections.municipality_key
        ORDER BY
          firms_active_fire_detections.acquired_at DESC,
          firms_active_fire_detections.frp DESC NULLS LAST,
          firms_active_fire_detections.id DESC
      ) AS detection_rank
    FROM public.firms_active_fire_detections
    INNER JOIN current_fire_counts
      ON current_fire_counts.municipality_key = firms_active_fire_detections.municipality_key
    WHERE firms_active_fire_detections.is_in_greece IS TRUE
      AND firms_active_fire_detections.municipality_key IS NOT NULL
      AND firms_active_fire_detections.acquired_at >= NOW() - INTERVAL '24 hours'
      AND current_fire_counts.current_fire_count = 1
  ),
  matched_coordinates AS (
    SELECT
      eligible_current_fires.incident_key,
      ranked_firms_detections.latitude,
      ranked_firms_detections.longitude
    FROM eligible_current_fires
    INNER JOIN ranked_firms_detections
      ON ranked_firms_detections.municipality_key = eligible_current_fires.municipality_key
    WHERE ranked_firms_detections.detection_rank = 1
  )
  UPDATE public.current_fires
  SET
    lat = matched_coordinates.latitude,
    lon = matched_coordinates.longitude
  FROM matched_coordinates
  WHERE current_fires.incident_key = matched_coordinates.incident_key
    AND (
      current_fires.lat IS DISTINCT FROM matched_coordinates.latitude
      OR current_fires.lon IS DISTINCT FROM matched_coordinates.longitude
    );

  GET DIAGNOSTICS v_updated_count = ROW_COUNT;
  RETURN v_updated_count;
END;
$$;

REVOKE ALL ON FUNCTION public.sync_current_fire_coordinates_from_firms()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.sync_current_fire_coordinates_from_firms()
TO service_role;

DO $$
BEGIN
  IF to_regclass('public.current_fires') IS NOT NULL
     AND to_regclass('public.firms_active_fire_detections') IS NOT NULL THEN
    PERFORM public.sync_current_fire_coordinates_from_firms();
  END IF;
END $$;

-- -------------------------------------------------------------
-- N) Contract analysis RPC
-- Source: sql/044_contract_analysis_rpc.sql
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_contract_analysis(
  p_year_start integer DEFAULT 2024
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH first_payment AS (
  SELECT DISTINCT ON (py.procurement_id)
    py.procurement_id,
    COALESCE(py.amount_without_vat, 0) AS amount_without_vat
  FROM public.payment py
  ORDER BY py.procurement_id, py.id
),
cpv_by_proc AS (
  SELECT
    c.procurement_id,
    jsonb_agg(DISTINCT COALESCE(NULLIF(BTRIM(c.cpv_value), ''), NULLIF(BTRIM(c.cpv_key), ''))) FILTER (
      WHERE COALESCE(NULLIF(BTRIM(c.cpv_value), ''), NULLIF(BTRIM(c.cpv_key), '')) IS NOT NULL
    ) AS cpvs
  FROM public.cpv c
  GROUP BY c.procurement_id
),
proc_ranked AS (
  SELECT
    p.id,
    p.contract_signed_date,
    p.start_date,
    p.end_date,
    p.no_end_date,
    p.title,
    p.reference_number,
    p.contract_number,
    p.diavgeia_ada,
    p.contract_type,
    p.procedure_type_value,
    p.organization_key,
    p.municipality_key,
    p.region_key,
    p.canonical_owner_scope,
    COALESCE(fp.amount_without_vat, 0) AS amount_without_vat,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN first_payment fp
    ON fp.procurement_id = p.id
  WHERE COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = p.reference_number
    )
),
base AS (
  SELECT
    pr.id,
    pr.contract_signed_date,
    COALESCE(pr.start_date, pr.contract_signed_date) AS effective_start,
    CASE
      WHEN COALESCE(pr.no_end_date, FALSE) THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
      ELSE COALESCE(pr.end_date, COALESCE(pr.start_date, pr.contract_signed_date))
    END AS effective_end,
    COALESCE(o.organization_normalized_value, o.organization_value, NULLIF(BTRIM(pr.organization_key), ''), '—') AS org_name,
    CASE
      WHEN pr.canonical_owner_scope = 'municipality' THEN COALESCE('ΔΗΜΟΣ ' || NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), ''), COALESCE(o.organization_normalized_value, o.organization_value), 'Δήμος —')
      WHEN pr.canonical_owner_scope = 'region' THEN COALESCE('ΠΕΡΙΦΕΡΕΙΑ ' || NULLIF(BTRIM(COALESCE(r.region_normalized_value, r.region_value)), ''), 'ΔΗΜΟΣ ' || NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), ''), COALESCE(o.organization_normalized_value, o.organization_value), 'Περιφέρεια —')
      WHEN o.authority_scope = 'municipality' AND NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), '') IS NOT NULL THEN 'ΔΗΜΟΣ ' || BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value))
      WHEN o.authority_scope IN ('region', 'decentralized') AND NULLIF(BTRIM(COALESCE(r.region_normalized_value, r.region_value)), '') IS NOT NULL THEN 'ΠΕΡΙΦΕΡΕΙΑ ' || BTRIM(COALESCE(r.region_normalized_value, r.region_value))
      ELSE COALESCE(o.organization_normalized_value, o.organization_value, 'ΔΗΜΟΣ ' || NULLIF(BTRIM(COALESCE(m.municipality_normalized_value, m.municipality_value)), ''), 'ΠΕΡΙΦΕΡΕΙΑ ' || NULLIF(BTRIM(COALESCE(r.region_normalized_value, r.region_value)), ''), '—')
    END AS authority_label,
    CASE
      WHEN translate(UPPER(COALESCE(pr.contract_type, '')), 'ΆΈΉΊΪΌΎΫΏ', 'ΑΕΗΙΙΟΥΥΩ') LIKE '%ΥΠΗΡΕΣ%' THEN 'Υπηρεσίες'
      WHEN translate(UPPER(COALESCE(pr.contract_type, '')), 'ΆΈΉΊΪΌΎΫΏ', 'ΑΕΗΙΙΟΥΥΩ') LIKE '%ΠΡΟΜΗΘΕΙ%' THEN 'Προμήθειες'
      WHEN translate(UPPER(COALESCE(pr.contract_type, '')), 'ΆΈΉΊΪΌΎΫΏ', 'ΑΕΗΙΙΟΥΥΩ') LIKE '%ΕΡΓ%' THEN 'Έργα'
      ELSE 'Λοιπές'
    END AS contract_type,
    CASE
      WHEN LOWER(COALESCE(pr.procedure_type_value, '')) LIKE '%απευθείας ανάθεση%' THEN 'Απευθείας Ανάθεση'
      WHEN LOWER(COALESCE(pr.procedure_type_value, '')) LIKE '%ανοιχτή%' THEN 'Ανοιχτή Διαδικασία'
      WHEN LOWER(COALESCE(pr.procedure_type_value, '')) LIKE '%διαπραγ%' THEN 'Διαπραγμάτευση'
      ELSE 'Άλλη'
    END AS procedure,
    pr.amount_without_vat AS amount,
    COALESCE(cpv.cpvs, '[]'::jsonb) AS cpvs
  FROM proc_ranked pr
  LEFT JOIN LATERAL (
    SELECT o.organization_normalized_value, o.organization_value, o.authority_scope
    FROM public.organization o
    WHERE o.organization_key = pr.organization_key
    ORDER BY o.id
    LIMIT 1
  ) o ON TRUE
  LEFT JOIN LATERAL (
    SELECT m.municipality_normalized_value, m.municipality_value
    FROM public.municipality m
    WHERE m.municipality_key = pr.municipality_key
    ORDER BY m.id
    LIMIT 1
  ) m ON TRUE
  LEFT JOIN LATERAL (
    SELECT r.region_normalized_value, r.region_value
    FROM public.region r
    WHERE r.region_key = pr.region_key
    ORDER BY r.id
    LIMIT 1
  ) r ON TRUE
  LEFT JOIN cpv_by_proc cpv
    ON cpv.procurement_id = pr.id
  WHERE pr.rn = 1
),
windowed AS (
  SELECT *
  FROM base
  WHERE effective_start IS NOT NULL
    AND effective_start <= make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
    AND effective_end >= make_date(p_year_start, 1, 1)
),
section_rows AS (
  SELECT jsonb_agg(
    jsonb_build_object(
      'signedDate', contract_signed_date,
      'effectiveStart', effective_start,
      'effectiveEnd', effective_end,
      'orgName', org_name,
      'authorityLabel', authority_label,
      'contractType', contract_type,
      'procedure', procedure,
      'amount', amount,
      'cpvs', cpvs
    )
    ORDER BY COALESCE(contract_signed_date, effective_start), id
  ) AS rows
  FROM windowed
)
SELECT jsonb_build_object(
  'sectionRows', COALESCE((SELECT rows FROM section_rows), '[]'::jsonb)
);
$$;

GRANT EXECUTE ON FUNCTION public.get_contract_analysis(integer)
TO anon, authenticated, service_role;


-- -------------------------------------------------------------
-- O) Direct award value distribution
-- -------------------------------------------------------------
-- Returns log-scale histogram bins for direct award contract values.
-- Matches the same direct-award universe as get_contract_analysis.
DROP FUNCTION IF EXISTS public.get_direct_award_distribution();

CREATE OR REPLACE FUNCTION public.get_direct_award_distribution()
RETURNS TABLE(bin_lo numeric, bin_hi numeric, cnt bigint, total_count bigint)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
WITH first_payment AS (
  SELECT DISTINCT ON (py.procurement_id)
    py.procurement_id,
    COALESCE(py.amount_without_vat, 0) AS amount_without_vat
  FROM public.payment py
  ORDER BY py.procurement_id, py.id
),
base AS (
  SELECT
    COALESCE(fp.amount_without_vat, 0) AS amount,
    CASE
      WHEN LOWER(COALESCE(p.procedure_type_value, '')) LIKE '%απευθείας ανάθεση%' THEN 'Απευθείας Ανάθεση'
      WHEN LOWER(COALESCE(p.procedure_type_value, '')) LIKE '%ανοιχτή%' THEN 'Ανοιχτή Διαδικασία'
      WHEN LOWER(COALESCE(p.procedure_type_value, '')) LIKE '%διαπραγ%' THEN 'Διαπραγμάτευση'
      ELSE 'Άλλη'
    END AS procedure,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(
        NULLIF(BTRIM(p.reference_number), ''),
        NULLIF(BTRIM(p.diavgeia_ada), ''),
        NULLIF(BTRIM(p.contract_number), ''),
        CONCAT_WS('|', COALESCE(p.organization_key, ''), COALESCE(p.title, ''), COALESCE(p.contract_signed_date::text, ''))
      )
      ORDER BY p.id DESC
    ) AS rn
  FROM public.procurement p
  LEFT JOIN first_payment fp ON fp.procurement_id = p.id
  WHERE p.contract_signed_date >= '2024-01-01'
    AND p.contract_signed_date <= make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
    AND COALESCE(p.cancelled, FALSE) = FALSE
    AND NULLIF(BTRIM(p.next_ref_no), '') IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM public.procurement p2
      WHERE NULLIF(BTRIM(p2.prev_reference_no), '') = NULLIF(BTRIM(p.reference_number), '')
    )
),
direct_awards AS (
  SELECT amount
  FROM base
  WHERE rn = 1
    AND procedure = 'Απευθείας Ανάθεση'
),
totals AS (
  SELECT COUNT(*)::bigint AS total_count
  FROM direct_awards
),
valued AS (
  SELECT amount
  FROM direct_awards
  WHERE amount IS NOT NULL
    AND amount >= 100
),
binned AS (
  SELECT
    FLOOR(LOG(amount) * 10)::int AS bin_idx,
    COUNT(*)                      AS cnt
  FROM valued
  GROUP BY FLOOR(LOG(amount) * 10)::int
),
bin_range AS (
  SELECT MIN(bin_idx) AS lo, MAX(bin_idx) AS hi FROM binned
),
all_bins AS (
  SELECT generate_series(lo, hi) AS bin_idx FROM bin_range
)
SELECT
  ROUND(POW(10, ab.bin_idx::numeric / 10), 2)       AS bin_lo,
  ROUND(POW(10, (ab.bin_idx + 1)::numeric / 10), 2) AS bin_hi,
  COALESCE(b.cnt, 0)                                 AS cnt,
  totals.total_count
FROM all_bins ab
CROSS JOIN totals
LEFT JOIN binned b ON b.bin_idx = ab.bin_idx
ORDER BY ab.bin_idx;
$$;

GRANT EXECUTE ON FUNCTION public.get_direct_award_distribution()
TO anon, authenticated, service_role;


-- -------------------------------------------------------------
-- P) NASA FIRMS active fire detections frontend access
-- -------------------------------------------------------------
DO $$
BEGIN
  IF to_regclass('public.firms_active_fire_detections') IS NOT NULL THEN
    ALTER TABLE public.firms_active_fire_detections
      DROP CONSTRAINT IF EXISTS chk_firms_active_fire_detections_confidence;

    ALTER TABLE public.firms_active_fire_detections
      ADD CONSTRAINT chk_firms_active_fire_detections_confidence
      CHECK (
        confidence IN ('l', 'n', 'h')
        OR (
          CASE
            WHEN confidence ~ '^[0-9]+(\.[0-9]+)?$'
            THEN confidence::NUMERIC >= 0 AND confidence::NUMERIC <= 100
            ELSE FALSE
          END
        )
      );

    ALTER TABLE public.firms_active_fire_detections ENABLE ROW LEVEL SECURITY;

    DROP POLICY IF EXISTS public_read_firms_active_fire_detections
      ON public.firms_active_fire_detections;

    CREATE POLICY public_read_firms_active_fire_detections
    ON public.firms_active_fire_detections
    FOR SELECT
    TO anon, authenticated
    USING (true);

    GRANT SELECT ON public.firms_active_fire_detections
    TO anon, authenticated, service_role;
  END IF;
END $$;


-- -------------------------------------------------------------
-- Q) Municipality Diavgeia decisions RPC
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_municipality_diavgeia_decisions(text);
DROP FUNCTION IF EXISTS public.get_municipality_diavgeia_decisions(text, integer);

CREATE OR REPLACE FUNCTION public.get_municipality_diavgeia_decisions(
  p_municipality_key text,
  p_year integer
)
RETURNS TABLE (
  diavgeia_id bigint,
  org_type text,
  org_name_clean text,
  subject text,
  decision_date timestamptz,
  ada text,
  diavgeia_document_type_decision_uid text,
  spending_contractors_value text,
  document_url text,
  municipality_key text,
  protocol_number text,
  thematic_categories text,
  spending_signers text,
  spending_contractors_name text,
  spending_contractors_afm text
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
AS $$
SELECT
  d.id AS diavgeia_id,
  d.org_type,
  d.org_name_clean,
  d.subject,
  COALESCE(d.publish_timestamp, d.submission_timestamp) AS decision_date,
  d.ada,
  d.diavgeia_document_type_decision_uid,
  d.spending_contractors_value,
  d.document_url,
  d.municipality_key,
  d.protocol_number,
  d.thematic_categories,
  d.spending_signers,
  d.spending_contractors_name,
  d.spending_contractors_afm
FROM public.diavgeia d
WHERE d.municipality_key = p_municipality_key
  AND COALESCE(d.publish_timestamp, d.submission_timestamp) >= make_timestamptz(p_year, 1, 1, 0, 0, 0)
  AND COALESCE(d.publish_timestamp, d.submission_timestamp) < make_timestamptz(p_year + 1, 1, 1, 0, 0, 0)
ORDER BY
  COALESCE(d.publish_timestamp, d.submission_timestamp) DESC NULLS LAST,
  d.id DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_municipality_diavgeia_decisions(text, integer) TO anon, authenticated, service_role;


-- -------------------------------------------------------------
-- R) Diavgeia page RPC
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS public.get_diavgeia_page(text, date, date, integer, integer);
DROP FUNCTION IF EXISTS public.get_diavgeia_page(text, date, date, text, integer, integer);

CREATE OR REPLACE FUNCTION public.get_diavgeia_page(
  p_q text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_municipality_key text DEFAULT NULL,
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 50
)
RETURNS TABLE (
  id bigint,
  org_type text,
  org_name_clean text,
  subject text,
  decision_date timestamptz,
  ada text,
  diavgeia_document_type_decision_uid text,
  document_url text,
  spending_contractors_value text,
  municipality_key text,
  protocol_number text,
  thematic_categories text,
  spending_signers text,
  spending_contractors_name text,
  spending_contractors_afm text,
  min_decision_date timestamptz,
  max_decision_date timestamptz,
  total_count bigint
)
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
WITH raw_query_terms AS (
  SELECT parsed.raw_term
  FROM regexp_split_to_table(
    regexp_replace(LEFT(COALESCE(p_q, ''), 500), E'\\s*!\\s*', '&!', 'g'),
    E'\\s*&\\s*'
  ) WITH ORDINALITY AS parsed(raw_term, ordinal)
  WHERE parsed.ordinal <= 10
),
query_terms AS (
  SELECT
    upper(
      translate(
        LEFT(BTRIM(CASE WHEN LEFT(raw_term, 1) = '!' THEN SUBSTRING(raw_term FROM 2) ELSE raw_term END), 100),
        'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
        'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
      )
    ) AS value,
    LEFT(raw_term, 1) = '!' AS excluded
  FROM raw_query_terms
  WHERE BTRIM(CASE WHEN LEFT(raw_term, 1) = '!' THEN SUBSTRING(raw_term FROM 2) ELSE raw_term END) <> ''
),
base AS (
  SELECT
    d.id,
    d.org_type,
    d.org_name_clean,
    d.subject,
    COALESCE(d.publish_timestamp, d.submission_timestamp) AS decision_date,
    d.ada,
    d.diavgeia_document_type_decision_uid,
    d.document_url,
    d.spending_contractors_value,
    d.municipality_key,
    d.protocol_number,
    d.thematic_categories,
    d.spending_signers,
    d.spending_contractors_name,
    d.spending_contractors_afm,
    upper(
      translate(
        CONCAT_WS(
          ' ',
          COALESCE(d.subject, ''),
          COALESCE(d.org_type, ''),
          COALESCE(d.org_name_clean, ''),
          COALESCE(d.ada, ''),
          COALESCE(d.diavgeia_document_type_decision_uid, '')
        ),
        'ΆΈΉΊΪΌΎΫΏάέήίϊΐόύϋΰώ',
        'ΑΕΗΙΙΟΥΥΩΑΕΗΙΙΙΟΥΥΥΩ'
      )
    ) AS searchable_text
  FROM public.diavgeia d
),
filtered AS (
  SELECT *
  FROM base b
  WHERE (p_date_from IS NULL OR b.decision_date::date >= p_date_from)
    AND (p_date_to IS NULL OR b.decision_date::date <= p_date_to)
    AND (p_municipality_key IS NULL OR p_municipality_key = '' OR EXISTS (
      SELECT 1
      FROM public.diavgeia d
      WHERE d.id = b.id
        AND d.municipality_key = p_municipality_key
    ))
    AND NOT EXISTS (
      SELECT 1
      FROM query_terms qt
      WHERE (NOT qt.excluded AND STRPOS(b.searchable_text, qt.value) = 0)
         OR (qt.excluded AND STRPOS(b.searchable_text, qt.value) > 0)
    )
),
counted AS (
  SELECT
    f.*,
    MIN(f.decision_date) OVER () AS min_decision_date,
    MAX(f.decision_date) OVER () AS max_decision_date,
    COUNT(*) OVER () AS total_count
  FROM filtered f
)
SELECT
  id,
  org_type,
  org_name_clean,
  subject,
  decision_date,
  ada,
  diavgeia_document_type_decision_uid,
  document_url,
  spending_contractors_value,
  municipality_key,
  protocol_number,
  thematic_categories,
  spending_signers,
  spending_contractors_name,
  spending_contractors_afm,
  min_decision_date,
  max_decision_date,
  total_count
FROM counted
ORDER BY decision_date DESC NULLS LAST, id DESC
OFFSET (LEAST(GREATEST(COALESCE(p_page, 1), 1), 10000) - 1)
  * LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 50000)
LIMIT LEAST(GREATEST(COALESCE(p_page_size, 50), 1), 50000);
$$;

GRANT EXECUTE ON FUNCTION public.get_diavgeia_page(text, date, date, text, integer, integer) TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.get_diavgeia_page_snapshot(
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL,
  p_municipality_key text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
SECURITY INVOKER
SET search_path = public
SET statement_timeout = '20s'
AS $$
SELECT jsonb_build_object(
  'rows', COALESCE(
    jsonb_agg(
      to_jsonb(r) - 'total_count' - 'min_decision_date' - 'max_decision_date'
      ORDER BY r.decision_date DESC NULLS LAST, r.id DESC
    ),
    '[]'::jsonb
  )
)
FROM public.get_diavgeia_page(
  NULL, p_date_from, p_date_to, p_municipality_key, 1, 50000
) r;
$$;

GRANT EXECUTE ON FUNCTION public.get_diavgeia_page_snapshot(date, date, text) TO anon, authenticated, service_role;


-- -------------------------------------------------------------
-- S) Tighten function execution grants
-- -------------------------------------------------------------
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, anon, authenticated;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT EXECUTE ON FUNCTIONS TO service_role;

DO $frontend_public_rpc_grants$
DECLARE
  function_signature text;
BEGIN
  FOREACH function_signature IN ARRAY ARRAY[
    'public.get_analysis_top_authorities(integer, integer, integer)',
    'public.get_contract_analysis(integer)',
    'public.get_contracts_page(text, text, text, date, date, numeric, integer, integer)',
    'public.get_diavgeia_page(text, date, date, text, integer, integer)',
    'public.get_direct_award_distribution()',
    'public.get_environment_ministry_dashboard(integer)',
    'public.get_featured_beneficiaries(integer, integer)',
    'public.get_hero_section_data(integer, integer)',
    'public.get_homepage_funding(integer, integer)',
    'public.get_latest_contract_cards(integer)',
    'public.get_latest_funding_year_municipality_spend()',
    'public.get_news_ticker_articles()',
    'public.get_municipality_contract_count(text, integer)',
    'public.get_municipality_contract_summary(text, integer)',
    'public.get_municipality_contracts(text, integer, integer, integer)',
    'public.get_municipality_diavgeia_decisions(text, integer)',
    'public.get_municipality_featured_beneficiaries(text, integer, integer)',
    'public.get_municipality_map_funding_per_100k(integer)',
    'public.get_municipality_map_spend_per_100k(integer)',
    'public.get_region_contract_count(text, integer)',
    'public.get_region_contract_summary(text, integer)',
    'public.get_region_contracts(text, integer, integer, integer)',
    'public.normalize_procedure_type(text)'
  ]
  LOOP
    IF to_regprocedure(function_signature) IS NOT NULL THEN
      EXECUTE format('ALTER FUNCTION %s SECURITY INVOKER', function_signature);
      EXECUTE format(
        'GRANT EXECUTE ON FUNCTION %s TO anon, authenticated, service_role',
        function_signature
      );
    END IF;
  END LOOP;
END
$frontend_public_rpc_grants$;

REVOKE ALL ON FUNCTION public.sync_current_fire_coordinates_from_firms()
FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.sync_current_fire_coordinates_from_firms()
TO service_role;

-- -------------------------------------------------------------
-- T) Disable Supabase Realtime database publications
-- -------------------------------------------------------------
DO $disable_supabase_realtime$
DECLARE
  published_table record;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_publication
    WHERE pubname = 'supabase_realtime'
  ) THEN
    FOR published_table IN
      SELECT schemaname, tablename
      FROM pg_publication_tables
      WHERE pubname = 'supabase_realtime'
    LOOP
      EXECUTE format(
        'ALTER PUBLICATION supabase_realtime DROP TABLE %I.%I',
        published_table.schemaname,
        published_table.tablename
      );
    END LOOP;
  END IF;
END
$disable_supabase_realtime$;

-- -------------------------------------------------------------
-- U) Force PostgREST to reload schema cache
-- -------------------------------------------------------------
NOTIFY pgrst, 'reload schema';

-- -------------------------------------------------------------
-- V) Quick verification (run after setup)
-- -------------------------------------------------------------
-- 1) Check RPC functions exist
-- select p.proname, pg_get_function_arguments(p.oid) as args
-- from pg_proc p
-- join pg_namespace n on n.oid = p.pronamespace
-- where n.nspname = 'public'
--   and p.proname in ('get_hero_section_data', 'get_homepage_funding', 'get_contracts_page', 'get_diavgeia_page', 'get_featured_beneficiaries', 'get_municipality_featured_beneficiaries', 'get_municipality_diavgeia_decisions', 'get_latest_contract_cards', 'get_municipality_map_spend_per_100k', 'get_latest_funding_year_municipality_spend', 'get_contract_analysis', 'get_direct_award_distribution', 'normalize_procedure_type');

-- 2) Quick smoke test RPC calls
-- select public.get_hero_section_data(2026, 2024);
-- select public.get_homepage_funding(2026, 2016);
-- select * from public.get_contracts_page(NULL,NULL,NULL,NULL,NULL,NULL,1,5);
-- select * from public.get_diavgeia_page(NULL, current_date - 30, current_date, NULL, 1, 5);
-- select * from public.get_featured_beneficiaries(2026, 5);
-- select * from public.get_municipality_featured_beneficiaries('9061', 2026, 5);
-- select * from public.get_municipality_diavgeia_decisions('9061', 2026) limit 20;
-- select * from public.get_latest_contract_cards(5);
-- select * from public.get_municipality_map_spend_per_100k(2026) limit 20;
-- select public.get_latest_funding_year_municipality_spend();
-- select public.get_contract_analysis(2024);
-- select * from public.get_direct_award_distribution() limit 20;

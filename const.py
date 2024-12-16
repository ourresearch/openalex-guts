PREPRINT_JOURNAL_IDS = {4306401283, 4306402567, 4306507144, 4306402450,
                        4306401687, 4306402587, 4210203035, 4306401693,
                        4306400670, 4306401696, 4306401186, 2883122597,
                        3005989158, 4306402473, 4306402219, 4306400047,
                        4306402352, 4306401713, 4210177074,
                        4306401712, 4306402614, 4306504120, 4306402488,
                        4306402105, 4306401210, 4306400573, 4306401597,
                        4393918269, 4306400194, 4306402499, 4210218052,
                        4210239046, 2764847869, 4306401228, 4306402512,
                        4306401745, 4363605331, 4306401238, 4210186582,
                        4306401879, 4306400732, 4306402530, 4210230499,
                        4306402531, 48744551, 2736351211, 4306402544,
                        4306401271, 4306401405, 4210224510}

REVIEW_JOURNAL_IDS = {206848516, 120397320, 111502347, 163102221, 316438,
                      101036573, 70620193, 160464432, 12874817, 4210178125,
                      169446993, 62674001, 7783507, 26843219, 157499488,
                      112662651, 186543748, 67216020, 2898133657, 129217179,
                      172508320, 201273009, 81141427, 96726197, 157921468,
                      183292605, 19775678, 92651206, 28635856, 116269268,
                      41981144, 18618073, 8553189, 37062379, 156364528,
                      78310130, 51877618, 2491932416, 88857351, 10734860,
                      2764917516, 93373720, 4210174246, 114927911, 4210234670,
                      83493678, 22232893, 90458436, 182717252, 181418318,
                      56802129, 150570842, 10577759, 129765729, 33396077,
                      76837745, 144797041, 124600697, 4632444, 41143188,
                      4210168218, 196724125, 4210201506, 24135080, 55210418,
                      192051125, 78119372, 116952013, 161680845, 2755384784,
                      4210198484, 62841813, 38078933, 118162903, 43764696,
                      4210219483, 173474783, 105347553}

MAX_AFFILIATIONS_PER_AUTHOR = 10
MIN_CHARACTERS_PER_AFFILIATION = 5

# large orgs that will be removed from lineage
SUPER_SYSTEM_INSTITUTIONS = {
    2802694670,  # Academic Center for Education, Culture and Research
    4210123452,  # Anahuac University Network
    2800507078,  # University of Alabama System
    2802090665,  # University of Alaska System
    4210147594,  # Universidade do Estado do Amazonas
    3132585960,  # Arizona's Public Universities
    3211005485,  # Arkansas State University System
    2799691083,  # University of Arkansas System
    4210151215,  # Auburn University System
    919953467,   # California Community Colleges System
    127339247,   # California State University System
    2803209242,  # University of California System
    174216632,   # City University of New York
    872248612,   # Claremont Colleges
    92446798,    # Colorado State University
    2802236040,  # University of Colorado System
    5996616,     # De La Salle University
    40372098,    # DeVry University
    64728393,    # Farhangian University
    1283613182,  # Universidade Tecnológica Federal do Paraná
    17866349,    # Université de Toulouse
    3132770690,  # Florida College System
    4210095101,  # Hue University
    154851008,   # Indian Institute of Technology Roorkee
    185665358,   # InterAmerican University of Puerto Rico
    110525433,   # Islamic Azad University, Tehran
    1293386609,  # Ivy Tech Community College of Indiana
    2802646605,  # Kentucky Community and Technical College System
    2799601127,  # Louisiana Community and Technical College System
    107341208,   # Louisiana State University System
    4210128516,  # Maine Community College System
    2802089280,  # Maricopa County Community College District
    4210106167,  # Marine Corps University
    2801213986,  # Montana University System
    181231927,   # National University of Ireland
    4210151660,  # Nebraska State College System
    68260882,    # Nevada System of Higher Education
    4210127926,  # North Dakota University System
    2800216316,  # Oklahoma State University System
    2800918300,  # Oregon University System
    2746051580,  # Université Paris Sciences et Lettres
    55547365,    # Payame Noor University
    29957033,    # Pennsylvania State System of Higher Education
    111350863,   # Pontifical Catholic University of Puerto Rico
    2801333002,  # Purdue University System
    4210163361,  # Rajabhat University System
    4210147856,  # Saint Mary's University of Minnesota
    4210159985,  # Sakon Nakhon Rajabhat University
    2800204061,  # Southern University System
    2801649442,  # State University System of Florida
    1327163397,  # State University of New York
    193876067,   # Universidade Estadual do Oeste do Paraná
    3132885913,  # Taiwan Comprehensive University System
    2800756461,  # Technical College System of Georgia
    4210089766,  # Technical and Vocational University
    2801273398,  # Texas State University System
    4210088475,  # Texas Tech University System
    4210115718,  # Thai Nguyen University
    4210138650,  # Universiti Teknologi MARA System
    1289702989,  # University System of Georgia
    1317227900,  # University System of Maryland
    2800453862,  # University System of New Hampshire
    2802096936,  # University System of Ohio
    2802348453,  # University System of Taiwan
    2801365484,  # Utah System of Higher Education
    2802841742,  # University of Massachusetts System
    2802450327,  # University of Nebraska System
    4210158053,  # University of North Carolina System
    2802090120,  # University of North Texas System
    1331384533,  # University of Hawaii System
    2801525821,  # University of Houston System
    1304256225,  # University of Wisconsin System
    2799495847,  # University of Tennessee System
    2887101046,  # University of South Carolina System
    2800421839,  # Government of the United States of America
    177233841,   # Vietnam National University, Hanoi
    123565023,   # Vietnam National University Ho Chi Minh City
    184840846,   # Virginia Commonwealth University
    56066814,    # Virginia Community College System
    16452829,    # The University of Texas System
    2801919071,  # University of Illinois System
    2800403580,  # University of Minnesota System
    173268674,   # Texas A&M University System
    4390039267,  # Tokai National Higher Education and Research System
    4777552,     # University of Missouri System
}

BAD_TITLES = ['INDEX TO ADVERTISERS',
              'Questions and Answers',
              'Business and Personal',
              'LIST OF ILLUSTRATIONS',
              'high_reported_cases_plot.png',
              'Peer review statement',
              'Note by the Secretariat',
              'Data for EMSL Project 51126 from September 2020',
              'Editorial Board/Aims and Scope',
              'Preliminary material',
              'Data for EMSL Project 51127 from August 2021',
              'Verein deutscher Chemiker',
              '"Births, Marriages, and Deaths."',
              'Data for EMSL Project 60066 from August 2022',
              'Data for EMSL Project 51127 from August 2020',
              'Autorinnen und Autoren',
              'Editorial Introduction',
              'cases_by_infection.tab',
              'Nominations and Elections',
              'Instructions for Authors',
              'NOTICES OF RECENT PUBLICATIONS',
              'Notices to correspondents',
              'OUP accepted manuscript',
              'List of Abbreviations',
              'Editorial Board/Publication Information',
              'Papers to appear in forthcoming issues',
              'Taxonomic Abstract for the families.',
              'INSTRUCTIONS TO AUTHORS',
              'Publications Received',
              'Information for Contributors',
              'Data for EMSL Project 51287 from September 2022',
              'Abstracts from Current Literature',
              'Data for EMSL Project 51287 from May 2020',
              'Forthcoming meetings',
              'Data for EMSL Project 51127 from February 2022',
              'Editorials and Medical Intelligence',
              'Data for EMSL Project 51126 from January 2021',
              '[Publisher\'s information]',
              'Data for EMSL Project 60255 from July 2022',
              'Index to advertisers',
              'This Week in Science',
              '[NO TITLE AVAILABLE]',
              'Editorial Advisory Board',
              'General Introduction',
              'IFC - Editorial Board',
              'Preface to the First Edition',
              'University and Educational Intelligence',
              'Massachusetts Medical Society',
              'NOTES ON CONTRIBUTORS',
              'Letters to the Editor',
              'Letters to the editor',
              'Natural history specimens collected and/or identified and deposited.',
              'Graphical abstract TOC',
              'MEDICAL SOCIETY OF LONDON.',
              'Data for EMSL Project 50717 from January 2020',
              'Data for EMSL Project 51127 from July 2021',
              'Letter from the Editor',
              'Data for EMSL Project 51127 from November 2021',
              'Data for EMSL Project 51467 from July 2021',
              'Data for EMSL Project 51127 from July 2020',
              'Preface and Acknowledgements',
              'Data for EMSL Project 51127 from August 2022',
              'Data for EMSL Project 51287 from May 2023',
              'Data for EMSL Project 51127 from September 2021',
              'Meet Our Editorial Board Member',
              'Inside Front Cover - Editorial Board',
              'All PDFs of this category',
              'Data for EMSL Project 51127 from March 2021',
              'Data for EMSL Project 51287 from February 2023',
              'LETTERS TO THE EDITOR',
              'Contributors to this Issue',
              'Data for EMSL Project 51127 from May 2022',
              '"Letters, Notes, and Answers"',
              'Zusammenfassung und Ausblick',
              'Editorial introductions',
              'Partial Page Advertisements',
              '"Notes, Short Comments, and Answers to Correspondents."',
              'Data for EMSL Project 51287 from April 2022',
              'Data for EMSL Project 51241 from December 2022',
              'Guidelines for Authors',
              'Data for EMSL Project 51240 from October 2022',
              'Reviews and Notices of Books.',
              'Editorial: Software survey section',
              'Contributors to This Issue',
              'Data for EMSL Project 48784 from December 2019',
              'Data for EMSL Project 49510 from April 2021',
              'Data for EMSL Project 51287 from March 2023',
              'Issue Editorial Masthead',
              'Editors\' Introduction',
              'Data for EMSL Project 51287 from January 2021',
              'Data for EMSL Project 51127 from December 2021',
              'Data for EMSL Project 51287 from May 2022',
              'Queries and Minor Notes',
              'Data for EMSL Project 51287 from August 2022',
              'Data for EMSL Project 51287 from January 2022',
              'List of Contributors',
              'Data for EMSL Project 50717 from August 2020',
              'Abbildungsverzeichnis',
              'List of participants',
              'Data for EMSL Project 50686 from July 2021',
              'Data for EMSL Project 51127 from June 2020',
              'Abstracts of Current Literature',
              'Current World Literature',
              'Notes on the Contributors',
              'Other Books Received',
              'Editors’ Introduction',
              'News and Announcements',
              'Data for EMSL Project 51127 from September 2020',
              'Graphical contents list',
              'Data for EMSL Project 51287 from July 2023',
              'Instructions to authors',
              'Data for EMSL Project 60240 from April 2022',
              'Summary and Conclusions',
              'Data for EMSL Project 51127 from July 2022',
              'Data for EMSL Project 51287 from March 2022',
              'Forthcoming Articles',
              'Parliamentary Intelligence.',
              'Illustration Credits',
              'Data for EMSL Project 51287 from October 2022',
              'Introduction and Overview',
              'Data for EMSL Project 51287 from April 2020',
              'Data for EMSL Project 51127 from May 2020',
              'What Is Your Diagnosis?',
              'Data for EMSL Project 51287 from September 2021',
              'Data for EMSL Project 51287 from May 2021',
              'Data for EMSL Project 51287 from June 2023',
              'Issue Publication Information',
              'Current Publications Received',
              'Taxonomic Abstract for the genera.',
              'Preface to the Second Edition',
              'SELECTED BIBLIOGRAPHY',
              'List of Illustrations',
              '[Front cover and table of contents]',
              'Data for EMSL Project 51127 from October 2021',
              'Data for EMSL Project 51127 from February 2020',
              'Data for EMSL Project 51127 from June 2022',
              'Data for EMSL Project 60242 from August 2022',
              'Classified Advertising',
              'PUBLICATIONS RECEIVED',
              'List of figures and tables',
              'LETTER TO THE EDITOR',
              'Contributors to this issue',
              'University and Educational News',
              'List of abbreviations',
              'About the Contributors',
              'Instructions to Authors',
              'Data for EMSL Project 51240 from April 2021',
              'Abkürzungsverzeichnis',
              'Data for EMSL Project 51127 from March 2020',
              'Data for EMSL Project 51287 from April 2023',
              'Forthcoming articles',
              'Data for EMSL Project 49499 from April 2021',
              'Data for EMSL Project 51241 from May 2021',
              'Medical Notes in Parliament',
              'Matière préliminaire',
              'Recently Patented Inventions',
              'Editorial advisory board',
              'Information for Readers',
              'List of illustrations',
              'ProteomeXchange dataset',
              'Data for EMSL Project 51130 from August 2021',
              '"Letters, Notes, and Answers to Correspondents"',
              'Literaturverzeichnis',
              'Data for EMSL Project 51287 from July 2022',
              'Taxonomic Abstract for the species.',
              'Selected Bibliography',
              'Issue Information ‐ TOC',
              'Index to Advertisers',
              'Data for EMSL Project 51287 from June 2022',
              'Advertisement/Classifieds',
              'Current Medical Literature',
              'Publisher\'s Information',
              'Data for EMSL Project 51287 from December 2022',
              'Problems and Solutions',
              'Stichwortverzeichnis',
              'List of Figures and Tables',
              'Notices to Correspondents',
              'Information for readers',
              'Data for EMSL Project 51287 from January 2023',
              'Online Labour Index: Measuring the Online Gig Economy for Policy and Research',
              'Editor’s Introduction',
              'Authors of quotations wanted',
              'Letter to the Editor',
              '"Births, Marriages, and Deaths"',
              'Data for EMSL Project 51287 from November 2021',
              'Data for EMSL Project 60066 from September 2022',
              'EMPLOYMENT INFORMATION',
              'Data for EMSL Project 51287 from February 2020',
              'Title Page / Table of Contents',
              'Data for EMSL Project 51132 from March 2022',
              'Data for EMSL Project 51287 from March 2020',
              'Data for EMSL Project 51127 from October 2022',
              'Front & Back Matter',
              'Medical Appointments: Important Notice',
              'Data for EMSL Project 51127 from March 2022',
              'IFC (Editorial Board)',
              'Data for EMSL Project 47643 from January 2020',
              'Data for EMSL Project 51127 from June 2021',
              'Our Astronomical Column',
              'Preface and Acknowledgments',
              'Data for EMSL Project 60066 from June 2022',
              'Editor\'s Introduction',
              'Data for EMSL Project 50717 from September 2019',
              'Scientific Notes and News',
              'Organizing Committee',
              'Medical Appointments',
              'Publications received',
              'Letter to the editor',
              'Notes for Contributors',
              'Reports and other Publications',
              'Data for EMSL Project 51127 from January 2021',
              'Data for EMSL Project 51287 from August 2021',
              '"Syrie, Jebel Sem\'an, Barad"',
              'Data for EMSL Project 51240 from August 2022',
              '"Pflegewissenschaft, Hungen"',
              'Data for EMSL Project 51127 from November 2020',
              'Information for Authors',
              'Data for EMSL Project 60066 from January 2023',
              'British Medical Journal',
              'Verhandlungen Ärztlicher Gesellschaften',
              'Peer review declaration',
              'Data for EMSL Project 51287 from July 2021',
              'Notes on contributors',
              'Medical Diary for the ensuing Week.',
              'Data for EMSL Project 51287 from December 2021',
              'Data for EMSL Project 51127 from October 2020',
              'Reports of Societies',
              'Data for EMSL Project 51287 from December 2020',
              'Introduction générale',
              'Data for EMSL Project 51241 from September 2021',
              'Societies and Academies',
              'List of contributors',
              'Special Correspondence',
              'Peer Review Statement',
              'Titelei/Inhaltsverzeichnis',
              'Information for authors',
              'Data for EMSL Project 51287 from October 2021',
              'Data for EMSL Project 51241 from October 2021',
              'GBIF Occurrence Download',
              'Data for EMSL Project 51127 from April 2020',
              'Data for EMSL Project 51287 from November 2022',
              'Data for EMSL Project 51241 from October 2022',
              'Inside front cover (Editorial Board)',
              'Data for EMSL Project 51287 from June 2021',
              'Quellen- und Literaturverzeichnis',
              'Introducing IEEE Collabratec',
              'Data for EMSL Project 60214 from April 2023',
              'Books and Pamphlets Received',
              'Editorial Board Page',
              'Personal‐ und Hochschulnachrichten',
              'Data for EMSL Project 51287 from July 2020',
              'Public Health and Poor-Law Medical Services',
              'Data for EMSL Project 51127 from December 2020',
              'Data for EMSL Project 51287 from April 2021',
              'Data for EMSL Project 51127 from January 2020',
              'Preliminary Material',
              'Notes on Contributors']

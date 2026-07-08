export type FundingHistoryEntry = {
  year: number
  regularAmount: number
  emergencyAmount: number
  municipalityAmount: number
  syndesmosAmount: number
  totalAmount: number
}

export type FundingSummary = {
  yearMain: number
  yearPrevious: number
  historyStartYear: number
  currentTotal: number
  currentSpendAmount: number
  currentSpendYear: number
  previousTotal: number
  currentRegularAmount: number
  currentEmergencyAmount: number
  currentMunicipalityAmount: number
  currentSyndesmosAmount: number
  history: FundingHistoryEntry[]
}

export const fundingSummary: FundingSummary = {
  yearMain: 2026,
  yearPrevious: 2025,
  historyStartYear: 2016,
  currentTotal: 50000000,
  currentSpendAmount: 21897593.17,
  currentSpendYear: 2026,
  previousTotal: 37750000,
  currentRegularAmount: 50000000,
  currentEmergencyAmount: 0,
  currentMunicipalityAmount: 47500000,
  currentSyndesmosAmount: 2500000,
  history: [
    {
      year: 2016,
      regularAmount: 18400000,
      emergencyAmount: 0,
      municipalityAmount: 16890000,
      syndesmosAmount: 1510000,
      totalAmount: 18400000,
    },
    {
      year: 2017,
      regularAmount: 18400000,
      emergencyAmount: 10000,
      municipalityAmount: 16920000,
      syndesmosAmount: 1490000,
      totalAmount: 18410000,
    },
    {
      year: 2018,
      regularAmount: 18400000,
      emergencyAmount: 0,
      municipalityAmount: 16910000,
      syndesmosAmount: 1490000,
      totalAmount: 18400000,
    },
    {
      year: 2019,
      regularAmount: 18400000,
      emergencyAmount: 0,
      municipalityAmount: 16910000,
      syndesmosAmount: 1490000,
      totalAmount: 18400000,
    },
    {
      year: 2020,
      regularAmount: 18400000,
      emergencyAmount: 0,
      municipalityAmount: 16910000,
      syndesmosAmount: 1490000,
      totalAmount: 18400000,
    },
    {
      year: 2021,
      regularAmount: 18400000,
      emergencyAmount: 0,
      municipalityAmount: 16910000,
      syndesmosAmount: 1490000,
      totalAmount: 18400000,
    },
    {
      year: 2022,
      regularAmount: 16910000,
      emergencyAmount: 5880000,
      municipalityAmount: 22400000,
      syndesmosAmount: 390000,
      totalAmount: 22790000,
    },
    {
      year: 2023,
      regularAmount: 23140000,
      emergencyAmount: 0,
      municipalityAmount: 23140000,
      syndesmosAmount: 0,
      totalAmount: 23140000,
    },
    {
      year: 2024,
      regularAmount: 27010000,
      emergencyAmount: 4700000,
      municipalityAmount: 31710000,
      syndesmosAmount: 0,
      totalAmount: 31710000,
    },
    {
      year: 2025,
      regularAmount: 37750000,
      emergencyAmount: 0,
      municipalityAmount: 37750000,
      syndesmosAmount: 0,
      totalAmount: 37750000,
    },
    {
      year: 2026,
      regularAmount: 50000000,
      emergencyAmount: 0,
      municipalityAmount: 47500000,
      syndesmosAmount: 2500000,
      totalAmount: 50000000,
    },
  ],
}

import { Suspense, lazy } from 'react'
import App from '../App'
import PageLoading from '../components/PageLoading'
import LegacyReportsRedirect from '../features/analysis/LegacyReportsRedirect'
import { localizeHref } from './localizedHref'

const AnalysisPage = lazy(() => import('../features/analysis/catalog/AnalysisPage'))
const StatisticalAnalysisPage = lazy(() => import('../features/analysis/statistics/StatisticalAnalysisPage'))
const AntineroPage = lazy(() => import('../features/analysis/antinero/AntineroPage'))
const YearlyReportPage = lazy(() => import('../features/analysis/yearly-report/YearlyReportPage'))
const MethodologyGuidePage = lazy(() => import('../features/analysis/methodology-guide/MethodologyGuidePage'))
const ContractsPage = lazy(() => import('../pages/ContractsPage'))
const DiavgeiaPage = lazy(() => import('../pages/DiavgeiaPage'))
const EnvironmentMinistryPage = lazy(() => import('../pages/EnvironmentMinistryPage'))
const MunicipalitiesPage = lazy(() => import('../pages/MunicipalitiesPage'))
const MapsPage = lazy(() => import('../pages/MapsPage'))
const MediaPage = lazy(() => import('../pages/MediaPage'))
const PrivacyPage = lazy(() => import('../pages/PrivacyPage'))
const TermsPage = lazy(() => import('../pages/TermsPage'))

const pages = [
  { path: '/', element: <App /> },
  { path: '/analysis', element: <Suspense fallback={<PageLoading />}><AnalysisPage /></Suspense> },
  { path: '/analysis/statistics', element: <Suspense fallback={<PageLoading />}><StatisticalAnalysisPage /></Suspense> },
  { path: '/analysis/antinero-west-attica', element: <Suspense fallback={<PageLoading />}><AntineroPage /></Suspense> },
  { path: '/analysis/yearly_report', element: <Suspense fallback={<PageLoading />}><YearlyReportPage /></Suspense> },
  { path: '/analysis/methodology_guide', element: <Suspense fallback={<PageLoading />}><MethodologyGuidePage /></Suspense> },
  { path: '/reports/*', element: <LegacyReportsRedirect /> },
  { path: '/contracts', element: <Suspense fallback={<PageLoading />}><ContractsPage /></Suspense> },
  { path: '/diavgeia', element: <Suspense fallback={<PageLoading />}><DiavgeiaPage /></Suspense> },
  { path: '/environment-ministry', element: <Suspense fallback={<PageLoading />}><EnvironmentMinistryPage /></Suspense> },
  { path: '/municipalities', element: <Suspense fallback={<PageLoading />}><MunicipalitiesPage /></Suspense> },
  { path: '/maps', element: <Suspense fallback={<PageLoading />}><MapsPage /></Suspense> },
  { path: '/media', element: <Suspense fallback={<PageLoading />}><MediaPage /></Suspense> },
  { path: '/privacy', element: <Suspense fallback={<PageLoading />}><PrivacyPage /></Suspense> },
  { path: '/terms', element: <Suspense fallback={<PageLoading />}><TermsPage /></Suspense> },
]

// Each pair shares the exact same element. Keep a single page implementation
// and a stable component hierarchy while only the path/context language changes.
export const pageRoutes = pages.flatMap(page => [page, { ...page, path: localizeHref(page.path, 'en') }])

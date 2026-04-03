import { getCompanyUrlByAfm, normalizeAfm } from '../../server/gemiCompanyUrl.js'

export default async function handler(request, response) {
  response.setHeader('Cache-Control', 's-maxage=86400, stale-while-revalidate=604800')

  if (request.method !== 'GET') {
    response.status(405).json({ error: 'Method not allowed' })
    return
  }

  const afm = normalizeAfm(request.query?.afm)
  if (!afm) {
    response.status(400).json({ error: 'Missing or invalid AFM' })
    return
  }

  try {
    const payload = await getCompanyUrlByAfm(afm)
    response.status(200).json(payload)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown GEMI lookup error'
    const statusCode = message.includes('No exact AFM match') ? 404 : 502
    response.status(statusCode).json({ error: message })
  }
}

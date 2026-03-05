import type { VercelRequest, VercelResponse } from '@vercel/node'
import { getPool } from './_db.js'

/**
 * GET /api/arb-dashboard?range=7d
 *
 * Architecture:
 *   - LIVE data (positions, inventory, summary): from Upstash Redis
 *     (publisher.py pushes the full enriched payload every 30s)
 *   - HISTORICAL data (pnl_series, portfolio trends): from TimescaleDB
 *   - Fallback: if Redis unavailable, reconstruct from Timescale snapshots
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  const { range = '7d' } = req.query
  const intervalMap: Record<string, string> = {
    '1d': '1 day', '7d': '7 days', '30d': '30 days', 'all': '10 years'
  }
  const interval = intervalMap[range as string] || '7 days'

  try {
    // ── Try Redis first (real-time, full payload) ──
    const live = await readFromRedis()

    // ── Historical data from Timescale ──
    const historical = await readHistorical(interval)

    if (live) {
      // Normalize Redis payload to dashboard shape
      const inv = live.inventory || {} as any
      const bin = inv.binance || {}
      const hl = inv.hl || {}
      const sum = live.summary || {} as any

      const summary = {
        position_count: sum.position_count || 0,
        total_notional: sum.total_notional || 0,
        weighted_apy: sum.weighted_apy || 0,
        daily_usd: sum.daily_usd_total || sum.daily_usd || 0,
        total_realized: sum.realized_total || sum.total_realized || 0,
        leverage: sum.total_notional && inv.total_capital
          ? sum.total_notional / inv.total_capital : 0,
      }

      const inventory = {
        bin_balance: bin.wallet_balance || 0,
        bin_margin: bin.initial_margin || 0,
        bin_upnl: bin.unrealized_pnl || 0,
        hl_equity: hl.equity || 0,
        hl_margin: hl.margin_used || 0,
        hl_upnl: hl.unrealized_pnl || 0,
        total_capital: inv.total_capital || 0,
      }

      const positions = (live.positions || []).map((p: any) => ({
        symbol: p.symbol,
        bin_side: p.bin_side,
        hl_side: p.hl_side,
        notional: p.notional_usdt || p.notional || 0,
        entry_apy: p.entry_net_apy || p.entry_apy || 0,
        live_apy: p.live_net_apy || p.live_apy || 0,
        daily_usd: p.daily_usd || 0,
        realized: p.realized_total || p.funding_realized_bin + p.funding_realized_hl || 0,
        accruing: p.accruing_usd || p.accruing || 0,
        age_hours: p.age_hours || 0,
        status: p.status || 'active',
        legs: p.legs || undefined,
      }))

      res.setHeader('Cache-Control', 's-maxage=10, stale-while-revalidate=30')
      return res.json({
        summary,
        inventory,
        positions,
        alerts: live.alerts || [],
        history: historical.events,
        pnl_series: historical.pnl_series,
        portfolio_series: historical.portfolio_series,
        generated_at: live.generated_at,
        bot_alive: live.bot_alive,
      })
    }

    // ── Fallback: reconstruct from Timescale ──
    const fallback = await reconstructFromTimescale(interval)
    res.setHeader('Cache-Control', 's-maxage=30, stale-while-revalidate=120')
    return res.json(fallback)
  } catch (e: any) {
    console.error('arb-dashboard API error:', e.message)
    res.status(500).json({ error: e.message })
  }
}

// ── Redis reader (Upstash REST) ──────────────────────────────────────────────

interface RedisPayload {
  generated_at: string
  generated_ts: number
  bot_alive: boolean
  summary: any
  positions: any[]
  alerts: any[]
  inventory: any
}

async function readFromRedis(): Promise<RedisPayload | null> {
  const url = process.env.UPSTASH_REDIS_REST_URL || process.env.REDIS_KV_REST_API_URL
  const token = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.REDIS_KV_REST_API_TOKEN
  if (!url || !token) return null

  try {
    const res = await fetch(`${url.replace(/\/$/, '')}/get/cross-arb:dashboard`, {
      headers: { Authorization: `Bearer ${token}` },
      cache: 'no-store',
    })
    if (!res.ok) return null
    const body = await res.json() as { result: string | null }
    if (!body.result) return null
    return JSON.parse(body.result)
  } catch {
    return null
  }
}

// ── Historical data from Timescale ───────────────────────────────────────────

async function readHistorical(interval: string) {
  const events: any[] = []
  const pnl_series: any[] = []
  const portfolio_series: any[] = []

  try {
    const pool = getPool()

    // Closed positions for history + PnL
    const closed = await pool.query(`
      SELECT * FROM arb_positions
      WHERE opened_at > NOW() - $1::interval
      ORDER BY opened_at DESC LIMIT 100
    `, [interval])

    for (const p of closed.rows) {
      events.push({
        ts: new Date(p.closed_at || p.opened_at).getTime(),
        type: p.closed_at ? 'close' : 'open',
        symbol: p.symbol,
        notional: Number(p.notional) || 0,
        entry_apy: Number(p.entry_apy) || 0,
        exit_apy: Number(p.exit_apy) || 0,
        realized_total: Number(p.realized_total) || 0,
        age_hours: Number(p.age_hours) || 0,
      })
    }
    events.sort((a, b) => b.ts - a.ts)

    // Cumulative PnL from closed trades
    const closedSorted = closed.rows
      .filter((p: any) => p.closed_at)
      .sort((a: any, b: any) => new Date(a.closed_at).getTime() - new Date(b.closed_at).getTime())
    let cumPnl = 0
    for (const p of closedSorted) {
      cumPnl += Number(p.realized_total) || 0
      pnl_series.push({ ts: new Date(p.closed_at).getTime(), value: cumPnl })
    }

    // Portfolio value over time (downsampled)
    const bucketSize = interval === '1 day' ? '5 minutes'
      : interval === '7 days' ? '30 minutes'
      : interval === '30 days' ? '2 hours'
      : '1 day'

    const snapshots = await pool.query(`
      SELECT
        time_bucket($2::interval, ts) AS bucket,
        AVG(total_notional)::numeric(12,2) AS total_notional,
        AVG(weighted_apy)::numeric(10,2) AS weighted_apy,
        AVG(daily_usd)::numeric(10,4) AS daily_usd,
        LAST(total_realized, ts)::numeric(10,4) AS total_realized
      FROM arb_portfolio_snapshots
      WHERE ts > NOW() - $1::interval
      GROUP BY bucket
      ORDER BY bucket ASC
    `, [interval, bucketSize])

    for (const s of snapshots.rows) {
      portfolio_series.push({
        ts: new Date(s.bucket).getTime(),
        total_notional: Number(s.total_notional),
        weighted_apy: Number(s.weighted_apy),
        daily_usd: Number(s.daily_usd),
        total_realized: Number(s.total_realized),
      })
    }
  } catch (e) {
    console.warn('Historical query failed (non-fatal):', e)
  }

  return { events, pnl_series, portfolio_series }
}

// ── Timescale-only fallback (when Redis unavailable) ─────────────────────────

async function reconstructFromTimescale(interval: string) {
  const pool = getPool()

  // Latest portfolio snapshot
  const portfolio = await pool.query(
    `SELECT * FROM arb_portfolio_snapshots ORDER BY ts DESC LIMIT 1`
  )
  const snap = portfolio.rows[0]

  // Latest funding snapshot per symbol
  const funding = await pool.query(`
    SELECT DISTINCT ON (symbol) symbol, bin_rate, hl_rate, net_apy, notional, realized, accruing
    FROM arb_funding_snapshots
    ORDER BY symbol, ts DESC
  `)

  // Open positions
  const openPos = await pool.query(
    `SELECT * FROM arb_positions WHERE closed_at IS NULL ORDER BY opened_at DESC`
  )

  const summary = snap ? {
    position_count: snap.position_count || 0,
    total_notional: Number(snap.total_notional) || 0,
    weighted_apy: Number(snap.weighted_apy) || 0,
    daily_usd: Number(snap.daily_usd) || 0,
    total_realized: Number(snap.total_realized) || 0,
    leverage: Number(snap.leverage) || 0,
  } : { position_count: 0, total_notional: 0, weighted_apy: 0, daily_usd: 0, total_realized: 0, leverage: 0 }

  const inventory = snap ? {
    hl_equity: Number(snap.hl_equity) || 0,
    hl_margin: 0, hl_upnl: 0,
    bin_balance: Number(snap.bin_balance) || 0,
    bin_margin: 0, bin_upnl: 0,
    total_capital: (Number(snap.hl_equity) || 0) + (Number(snap.bin_balance) || 0),
  } : { hl_equity: 0, hl_margin: 0, hl_upnl: 0, bin_balance: 0, bin_margin: 0, bin_upnl: 0, total_capital: 0 }

  const fundingMap = new Map<string, any>(funding.rows.map((f: any) => [f.symbol, f]))
  const positions = openPos.rows.map((p: any) => {
    const f = fundingMap.get(p.symbol)
    return {
      symbol: p.symbol,
      bin_side: p.bin_side, hl_side: p.hl_side,
      notional: Number(f?.notional || p.notional) || 0,
      entry_apy: Number(p.entry_apy) || 0,
      live_apy: f ? Number(f.net_apy) || 0 : Number(p.entry_apy) || 0,
      daily_usd: f ? (Number(f.net_apy) / 100 / 365) * (Number(f.notional) || 0) : 0,
      realized: f ? Number(f.realized) || 0 : 0,
      accruing: f ? Number(f.accruing) || 0 : 0,
      age_hours: p.opened_at ? (Date.now() - new Date(p.opened_at).getTime()) / 3600000 : 0,
      status: 'active',
    }
  })

  const historical = await readHistorical(interval)

  return {
    summary, inventory, positions,
    alerts: [],
    history: historical.events,
    pnl_series: historical.pnl_series,
    portfolio_series: historical.portfolio_series,
    generated_at: snap?.ts || new Date().toISOString(),
    bot_alive: snap ? (Date.now() - new Date(snap.ts).getTime()) < 120000 : false,
  }
}

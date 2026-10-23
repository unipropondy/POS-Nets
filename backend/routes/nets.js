// backend/routes/nets.js
// NETS ECR3 LAN payment routes
// Mirrors the structure of /routes/yeahpay.js for consistency.
//
// Endpoints:
//   POST /api/nets/sale          — initiate a NETS payment on the terminal
//   POST /api/nets/last-txn      — query last transaction status (timeout recovery)
//   GET  /api/nets/ping          — connectivity check (no card involved)

'use strict';

const express = require('express');
const router  = express.Router();
const sql     = require('mssql');
const { v4: uuidv4 } = require('uuid');

const netsService  = require('../services/nets.service');
const { poolPromise } = require('../config/db');

// ─── Helper: Broadcast terminal status via Socket.io ─────────────────────────
function broadcastNetsStatus(io, tableId, amount, result) {
  if (!io) return;
  const targetTableId = tableId || 'COUNTER';

  let status  = 'failed';
  let message = '';

  if (
  result.success === true &&
  result.approved === true &&
  result.isSimulation !== true &&
  result.isSimulation !== "true" &&
  result.code !== "LIVE_PAYMENT_DISABLED"
) {
  status = 'success';
  message = `✅ $${parseFloat(amount).toFixed(2)} paid via NETS`;
} else if (result.code === '02') {
    status  = 'cancelled';
    message = '❌ Transaction cancelled on terminal';
  } else if (result.code === '03' || result.code === 'SOCKET_ERR') {
    status  = 'failed';
    message = '⏰ Transaction timed out';
  } else {
    status  = 'failed';
    message = `❌ ${result.msg || 'Payment declined'}`;
  }

  console.log(`[NETS] Broadcasting status for table ${targetTableId}: ${status}`);

  io.emit('terminal_payment_sync', {
    tableId: targetTableId,
    session: {
      tableId: targetTableId,
      status,
      message,
      method: 'NETS',
      total:  parseFloat(amount) || 0,
    },
  });
}

// ─── Helper: Persist NETS transaction to DB ───────────────────────────────────
async function persistNetsTransaction({
  pool,
  sessionId,
  settlementId,
  orderId,
  amount,
  result,
}) {
  try {
    const amountCents = Math.round(parseFloat(amount) * 100);
 const isNetsApproved =
  result.success === true &&
  result.approved === true &&
  result.isSimulation !== true &&
  result.isSimulation !== "true" &&
  result.code !== "LIVE_PAYMENT_DISABLED";

const status = isNetsApproved
  ? 'APPROVED'
  : result.code === '02'
    ? 'CANCELLED'
    : result.code === '03'
      ? 'TIMEOUT'
      : 'DECLINED';

    await pool.request()
      .input('NetsTransId',    sql.UniqueIdentifier, uuidv4())
      .input('SettlementId',   sql.UniqueIdentifier, settlementId || null)
      .input('OrderId',        sql.UniqueIdentifier, orderId      || null)
      .input('Amount',         sql.Decimal(18, 2),   parseFloat(amount))
      .input('AmountCents',    sql.BigInt,            amountCents)
      .input('Status',         sql.NVarChar(20),      status)
      .input('ResponseCode',   sql.NVarChar(10),      result.code ? String(result.code).slice(0, 10) : null)
      .input('ApprovalCode',   sql.NVarChar(20),      result.approvalCode || null)
      .input('CardType',       sql.NVarChar(50),      result.cardType     || null)
      .input('CardNo',         sql.NVarChar(20),      result.cardLastFour ? `****${result.cardLastFour}` : null)
      .input('TransactionRef', sql.NVarChar(50),      result.transRef || null)
      .input('BatchNo',        sql.NVarChar(20),      result.batchNo  || null)
      .input('TerminalId',     sql.NVarChar(20),      process.env.NETS_TERMINAL_ID || null)
      .input('RequestPayload', sql.NVarChar(sql.MAX),
        JSON.stringify({ amount, amountCents, terminalIp: process.env.NETS_TERMINAL_IP }))
      .input('ResponsePayload', sql.NVarChar(sql.MAX), JSON.stringify(result))
      .input('SessionId',      sql.NVarChar(100),     sessionId || null)
      .query(`
        INSERT INTO [dbo].[NetsTransactions] (
          NetsTransId, SettlementId, OrderId, Amount, AmountCents, Status,
          ResponseCode, ApprovalCode, CardType, CardNo, TransactionRef, BatchNo,
          TerminalId, RequestPayload, ResponsePayload, CreatedAt, SessionId
        ) VALUES (
          @NetsTransId, @SettlementId, @OrderId, @Amount, @AmountCents, @Status,
          @ResponseCode, @ApprovalCode, @CardType, @CardNo, @TransactionRef, @BatchNo,
          @TerminalId, @RequestPayload, @ResponsePayload, GETDATE(), @SessionId
        )
      `);
    console.log(`[NETS] Transaction persisted to DB — Status: ${status}`);
  } catch (dbErr) {
    // Log but do not throw — DB logging failure must not block the payment response
    console.error('[NETS] Failed to persist transaction to DB:', dbErr.message);
  }
}

// ─── Duplicate / in-flight request guard ─────────────────────────────────────
// Prevents the same POS table/session from triggering two concurrent NETS calls.
const activeNetsRequests = new Map(); // key → promise

// ─── POST /api/nets/sale ──────────────────────────────────────────────────────
router.post('/sale', async (req, res) => {
  try {
  const {
  amount,
  tableId,
  settlementId,
  orderId,
  sessionId,
  terminalId,
} = req.body;

// Validate POS terminal code
if (
  typeof terminalId !== 'string' ||
  !terminalId.trim() ||
  terminalId.trim().length > 10
) {
  return res.status(400).json({
    success: false,
    code: 'INVALID_TERMINAL',
    msg: 'Valid POS terminal code is required.'
  });
}

const posTerminalCode = terminalId.trim();

const pool = await poolPromise;

const terminalCheck = await pool.request()
  .input('TerminalCode', sql.Char(10), posTerminalCode)
  .query(`
    SELECT TerminalCode
    FROM dbo.TerminalMaster
    WHERE TerminalCode = @TerminalCode
  `);

if (terminalCheck.recordset.length !== 1) {
  return res.status(400).json({
    success: false,
    code: 'TERMINAL_NOT_FOUND',
    msg: 'POS terminal is not registered.'
  });
}

    // ── Input validation ────────────────────────────────────────────────────
    const parsedAmount = parseFloat(amount);
    if (isNaN(parsedAmount) || parsedAmount <= 0) {
      return res.status(400).json({ success: false, code: 'INVALID_AMOUNT', msg: 'Amount must be a positive number' });
    }

    // ── Server-side amount validation against DB ────────────────────────────
    // If orderId is provided, validate the amount matches what the DB says is owed.
    // This prevents the frontend from sending a tampered lower amount.
//     if (orderId) {
//       try {
//         const pool = await poolPromise;
//         const check = await pool.request()
//           .input('OrderId', sql.UniqueIdentifier, orderId)
//           .query(`
//             SELECT TOP 1 TotalAmount
//             FROM RestaurantOrderCur
//             WHERE OrderId = @OrderId AND isOrderClosed = 0
//           `);
//         if (check.recordset.length > 0) {
//           const dbTotal = parseFloat(check.recordset[0].TotalAmount || 0);
//           if (
//   !Number.isFinite(dbTotal) ||
//   dbTotal <= 0 ||
//   Math.round(dbTotal * 100) !== Math.round(parsedAmount * 100)
// ) {
//             console.warn(`[NETS] Amount mismatch: frontend $${parsedAmount}, DB $${dbTotal}`);
//             // Allow ±$1 tolerance for rounding; reject large discrepancies
//             return res.status(400).json({
//               success: false,
//               code: 'AMOUNT_MISMATCH',
//               msg: `Amount mismatch: expected $${dbTotal.toFixed(2)}, received $${parsedAmount.toFixed(2)}`,
//             });
//           }
//         }
//       } catch (validErr) {
//         // Non-critical: validation DB query failed; log and continue
//         console.warn('[NETS] Amount validation DB query failed:', validErr.message);
//       }
//     }

  if (
  typeof orderId !== 'string' ||
  !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(orderId)
) {
  return res.status(400).json({
    success: false,
    approved: false,
    code: 'INVALID_ORDER_ID',
    msg: 'A valid Order ID is required for NETS payment.'
  });
}

{
  try {
    const pool = await poolPromise;

    const check = await pool.request()
      .input('OrderId', sql.UniqueIdentifier, orderId)
      .query(`
        SELECT TOP 1 TotalAmount
        FROM RestaurantOrderCur
        WHERE OrderId = @OrderId
          AND isOrderClosed = 0
      `);

    // Reject missing or closed orders
    if (check.recordset.length === 0) {
      return res.status(400).json({
        success: false,
        code: 'ORDER_NOT_FOUND',
        msg: 'Order not found or already closed.'
      });
    }

    const dbTotal = Number(check.recordset[0].TotalAmount);

    if (!Number.isFinite(dbTotal) || dbTotal <= 0) {
      return res.status(400).json({
        success: false,
        code: 'INVALID_ORDER_AMOUNT',
        msg: 'Invalid order amount in database.'
      });
    }

    // Temporary upper-bound check.
    // Full payment and partial/split payment are both permitted.
    // Outstanding balance validation must be added before LIVE mode.
    if (Math.round(parsedAmount * 100) > Math.round(dbTotal * 100)) {
      return res.status(400).json({
        success: false,
        code: 'AMOUNT_EXCEEDS_ORDER',
        msg: 'NETS payment exceeds the order total.'
      });
    }

  } catch (validErr) {
    console.error(
      '[NETS] Order validation failed:',
      validErr.message
    );

    return res.status(503).json({
      success: false,
      code: 'ORDER_VALIDATION_FAILED',
      msg: 'Unable to validate order. Please retry.'
    });
  }
}

    // ── Duplicate in-flight check ───────────────────────────────────────────
    const lockKey = tableId ? String(tableId) : (sessionId || `amt_${parsedAmount}`);
if (activeNetsRequests.has(lockKey)) {
  console.warn(`[NETS] Duplicate in-flight request blocked: ${lockKey}`);

  return res.status(409).json({
    success: false,
    approved: false,
    code: 'IN_FLIGHT',
    msg: 'A NETS payment is already processing for this table/session. Please wait for its result.'
  });
}

    // ── Send to terminal ────────────────────────────────────────────────────
    console.log(`[NETS] Sale request — amount: $${parsedAmount}, table: ${tableId}, session: ${sessionId}`);

const promise = netsService.processSalePayment({
  amount: parsedAmount,
  orderId: orderId,
  terminalId: terminalId
});
    activeNetsRequests.set(lockKey, promise);

    let result;
    try {
      result = await promise;
    } finally {
      activeNetsRequests.delete(lockKey);
    }

    console.log('[NETS] Terminal result:', result);

    // ── Persist to DB ───────────────────────────────────────────────────────
    try {
      const pool = await poolPromise;
      await persistNetsTransaction({ pool, sessionId, settlementId, orderId, amount: parsedAmount, result });
    } catch (dbErr) {
      console.error('[NETS] DB persist error (non-fatal):', dbErr.message);
    }

    // ── Broadcast socket event ──────────────────────────────────────────────
    broadcastNetsStatus(req.io, tableId, parsedAmount, result);

    return res.json(result);

  } catch (err) {
    console.error('[NETS] Unhandled error in /sale:', err);
    const failResult = { success: false, code: 'SERVER_ERR', msg: err.message };
    broadcastNetsStatus(req.io, req.body?.tableId, req.body?.amount, failResult);
    return res.status(500).json(failResult);
  }
});

// ─── POST /api/nets/last-txn ─────────────────────────────────────────────────
// Query the last transaction status from the terminal.
// Use this after a timeout to determine if a payment went through.
router.post('/last-txn', async (req, res) => {
  try {
    const result = await netsService.queryLastTransaction();
    console.log('[NETS] Last transaction query result:', result);
    return res.json(result);
  } catch (err) {
    console.error('[NETS] last-txn error:', err.message);
    return res.status(500).json({ success: false, code: 'SERVER_ERR', msg: err.message });
  }
});

// ─── GET /api/nets/bridge-status ─────────────────────────────────────────────
// Checks if the Windows NETS Bridge Service is online and if the official DLL is detected
router.get('/bridge-status', async (req, res) => {
  try {
    const status = await netsService.getBridgeStatus();
    return res.json(status);
  } catch (err) {
    return res.status(500).json({ success: false, code: 'BRIDGE_ERROR', msg: err.message });
  }
});

// ─── GET /api/nets/ping ───────────────────────────────────────────────────────
// Lightweight connectivity check — opens a TCP socket to the terminal and closes it.
// Does NOT initiate a payment.
router.get('/ping', async (req, res) => {
  const net  = require('net');
  const host = process.env.NETS_TERMINAL_IP;
  const port = parseInt(process.env.NETS_TERMINAL_PORT || '9742', 10);

  if (!host) {
    return res.status(503).json({
      success: false,
      code: 'TERMINAL_IP_NOT_CONFIGURED',
      msg: 'NETS DESK5000 terminal IP is not configured in backend/.env. Confirm the physical terminal IP from the DESK5000 screen.',
      terminalIp: null,
      terminalPort: port,
    });
  }

  const socket = new net.Socket();
  socket.setTimeout(5000);

  socket.connect(port, host, () => {
    socket.destroy();
    return res.json({
      success: true,
      msg: `Terminal reachable at ${host}:${port}`,
      terminalIp:   host,
      terminalPort: port,
    });
  });

  socket.on('error', (err) => {
    return res.status(503).json({
      success: false,
      msg: `Cannot reach terminal at ${host}:${port} — ${err.message}`,
      terminalIp:   host,
      terminalPort: port,
    });
  });

  socket.on('timeout', () => {
    socket.destroy();
    return res.status(503).json({
      success: false,
      msg: `Connection to terminal timed out at ${host}:${port}`,
      terminalIp:   host,
      terminalPort: port,
    });
  });
});


 // Simulation-only endpoint. Never records a real payment.
router.post('/payment/simulate', async (req, res) => {
  try {
    if (
      process.env.NETS_MODE !== 'SIMULATION' ||
      process.env.NODE_ENV === 'production'
    ) {
      return res.status(403).json({
        success: false,
        code: 'SIMULATION_DISABLED',
        msg: 'Simulation is disabled'
      });
    }

    // const { orderId, amount, terminalId } = req.body || {};
    // Sumu

const { orderId, amount } = req.body || {};
const terminalId = process.env.NETS_TERMINAL_ID;

const parsedAmount = Number(amount);

const validAmount =
  typeof amount === 'number' &&
  Number.isFinite(parsedAmount) &&
  parsedAmount > 0 &&
  Math.abs(parsedAmount * 100 - Math.round(parsedAmount * 100)) < 0.000001;

if (
  !String(orderId || '').trim() ||
  !String(terminalId || '').trim() ||
  !validAmount
) {
  console.error('[NETS SIMULATION] Invalid request:', {
    orderId,
    amount,
    terminalIdConfigured: Boolean(terminalId),
  });

  return res.status(400).json({
    success: false,
    code: 'INVALID_REQUEST',
    msg: 'Valid orderId, amount and terminalId are required',
  });
}

const normalizedAmount = Math.round(parsedAmount * 100) / 100;

const result = await netsService.simulateBridgePayment({
  orderId,
  amount: normalizedAmount,
  terminalId
});


    // Do not call persistNetsTransaction or
    // broadcastNetsStatus for simulation.
    return res.json({
      ...result,
      success: false,
      approved: false,
      isSimulation: true,
      simulatedApproved: result.simulatedApproved === true
    });

  } catch (err) {
    console.error('[NETS] Simulation error:', err.message);
    return res.status(502).json({
      success: false,
      code: 'SIMULATION_ERROR',
      msg: 'NETS simulation request failed',
      isSimulation: true
    });
  }
});


module.exports = router;

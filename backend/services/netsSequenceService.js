
const sql = require("mssql");
const { poolPromise } = require("../config/db");

/**
 * Allocates the next NETS sequence number
 * for a POS terminal.
 *
 * IMPORTANT:
 * This function changes the database counter.
 * Do not call it during simulation or testing.
 */
async function getNextNETSSeqNo(terminalCode) {
  if (
    typeof terminalCode !== "string" ||
    !terminalCode.trim() ||
    terminalCode.trim().length > 10
  ) {
    throw new Error("Invalid POS terminal code.");
  }

  const pool = await poolPromise;

  const result = await pool
    .request()
    .input("TerminalCode", sql.Char(10), terminalCode.trim())
    .execute("dbo.Sp_GetNextNETSSeqNo");

  const sequenceNo = result.recordset?.[0]?.NETSSeqNo;

  if (
    !Number.isSafeInteger(sequenceNo) ||
    sequenceNo <= 0
  ) {
    throw new Error(
      "Invalid NETS sequence number returned for terminal: " +
      terminalCode
    );
  }

  return sequenceNo;
}

module.exports = { getNextNETSSeqNo };
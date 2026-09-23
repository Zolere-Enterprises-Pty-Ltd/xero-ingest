using System;
using System.Data;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace XeroIngest
{
    /// <summary>
    /// Loads the Xero chart of accounts into dbo.XroAccount.
    ///
    /// Runs inside the journals timer and reuses the access token that timer has already minted,
    /// so it adds no extra refresh of the single-use XeroTokenRefresh secret.
    ///
    /// The table is what every P&amp;L card in Metabase and OwnerPortal's FinancialKpiService join
    /// through, so an empty XroAccount silently nulls out every one of them. The load therefore
    /// refuses to write anything unless Xero returned a non-empty set, and writes what it did get
    /// inside a single transaction.
    /// </summary>
    public static class AccountsLoader
    {
        private const string AccountsUrl = "https://api.xero.com/api.xro/2.0/Accounts";

        // Every column is nvarchar(120) in dbo.XroAccount.
        private const int MaxLen = 120;

        private const string CreateStageSql = @"
            CREATE TABLE #XroAccountStage (
                AccountId   nvarchar(120) NOT NULL,
                AccountCode nvarchar(120) NULL,
                [Name]      nvarchar(120) NULL,
                [Type]      nvarchar(120) NULL,
                [Class]     nvarchar(120) NULL
            );";

        // Upsert only -- deliberately no WHEN NOT MATCHED BY SOURCE THEN DELETE. An account that
        // disappears from the feed is still referenced by historical journal lines, and dropping it
        // would re-open the very join gap this table exists to close.
        private const string MergeSql = @"
            DECLARE @changes TABLE (Act nvarchar(10));

            MERGE dbo.XroAccount WITH (HOLDLOCK) AS t
            USING #XroAccountStage AS s
              ON t.AccountId = s.AccountId
            WHEN MATCHED AND (
                    ISNULL(t.AccountCode, N'') <> ISNULL(s.AccountCode, N'')
                 OR ISNULL(t.[Name],      N'') <> ISNULL(s.[Name],      N'')
                 OR ISNULL(t.[Type],      N'') <> ISNULL(s.[Type],      N'')
                 OR ISNULL(t.[Class],     N'') <> ISNULL(s.[Class],     N''))
                THEN UPDATE SET
                    t.AccountCode = s.AccountCode,
                    t.[Name]      = s.[Name],
                    t.[Type]      = s.[Type],
                    t.[Class]     = s.[Class]
            WHEN NOT MATCHED BY TARGET
                THEN INSERT (AccountId, AccountCode, [Name], [Type], [Class])
                     VALUES (s.AccountId, s.AccountCode, s.[Name], s.[Type], s.[Class])
            OUTPUT $action INTO @changes;

            SELECT
                (SELECT COUNT(*) FROM @changes) AS Written,
                (SELECT COUNT(*) FROM dbo.XroAccount) AS TotalRows,
                (SELECT COUNT(*) FROM dbo.XroAccount t
                  WHERE NOT EXISTS (SELECT 1 FROM #XroAccountStage s WHERE s.AccountId = t.AccountId)) AS NotInFeed;";

        public static async Task RunAsync(
            HttpClient http,
            string accessToken,
            string tenantId,
            string sqlConnection,
            ILogger logger)
        {
            // ---- 1. Pull the whole chart of accounts first. Xero does not paginate this
            //         endpoint -- one response carries every account -- so there is no partial
            //         result to mistake for a complete one.
            var request = new HttpRequestMessage(HttpMethod.Get, AccountsUrl);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("xero-tenant-id", tenantId);
            request.Headers.Add("Accept", "application/json");

            var response = await http.SendAsync(request);
            var body = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                logger.LogError(
                    "Account ingest: Xero returned {status} ({reason}). Body: {body}. " +
                    "Leaving dbo.XroAccount untouched.",
                    (int)response.StatusCode, response.ReasonPhrase, body);
                return;
            }

            var table = BuildStageTable(body, logger, out var fetched, out var synthesised);
            logger.LogInformation("Account ingest: rows fetched from Xero: {fetched}", fetched);

            if (table == null)
            {
                return; // BuildStageTable has already logged why.
            }

            if (table.Rows.Count == 0)
            {
                logger.LogError(
                    "Account ingest: Xero returned {fetched} accounts but none were usable. " +
                    "Leaving dbo.XroAccount untouched.", fetched);
                return;
            }

            if (synthesised > 0)
            {
                logger.LogInformation(
                    "Account ingest: {synthesised} account(s) had no Code from Xero and were given " +
                    "a stable BANK- code derived from their AccountId.", synthesised);
            }

            // ---- 2. Stage and MERGE in one transaction. Either the whole chart lands or nothing
            //         changes; there is no window in which the table is empty.
            using var conn = new SqlConnection(sqlConnection);
            await conn.OpenAsync();
            using var tx = (SqlTransaction)await conn.BeginTransactionAsync();

            try
            {
                using (var create = new SqlCommand(CreateStageSql, conn, tx))
                {
                    await create.ExecuteNonQueryAsync();
                }

                using (var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tx)
                       {
                           DestinationTableName = "#XroAccountStage",
                           BulkCopyTimeout = 120
                       })
                {
                    foreach (DataColumn c in table.Columns)
                    {
                        bulk.ColumnMappings.Add(c.ColumnName, c.ColumnName);
                    }

                    await bulk.WriteToServerAsync(table);
                }

                int written = 0, totalRows = 0, notInFeed = 0;
                using (var merge = new SqlCommand(MergeSql, conn, tx) { CommandTimeout = 120 })
                using (var reader = await merge.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        written   = reader.GetInt32(0);
                        totalRows = reader.GetInt32(1);
                        notInFeed = reader.GetInt32(2);
                    }
                }

                await tx.CommitAsync();

                logger.LogInformation(
                    "Account ingest: rows fetched {fetched}, rows written {written}, " +
                    "dbo.XroAccount now holds {totalRows} rows ({notInFeed} not in this feed).",
                    fetched, written, totalRows, notInFeed);
            }
            catch (Exception ex)
            {
                await tx.RollbackAsync();
                logger.LogError(ex,
                    "Account ingest: write failed and was rolled back. dbo.XroAccount is unchanged " +
                    "({fetched} rows had been fetched from Xero).", fetched);
            }
        }

        private static DataTable? BuildStageTable(
            string body, ILogger logger, out int fetched, out int synthesised)
        {
            fetched = 0;
            synthesised = 0;

            using var doc = JsonDocument.Parse(body);

            if (!doc.RootElement.TryGetProperty("Accounts", out var accounts)
                || accounts.ValueKind != JsonValueKind.Array)
            {
                logger.LogError(
                    "Account ingest: Xero response had no Accounts array. Leaving dbo.XroAccount untouched.");
                return null;
            }

            fetched = accounts.GetArrayLength();

            if (fetched == 0)
            {
                logger.LogError(
                    "Account ingest: Xero returned 0 accounts. This is never legitimate for a live " +
                    "tenant, so it is treated as a failure -- leaving dbo.XroAccount untouched.");
                return null;
            }

            var table = new DataTable();
            table.Columns.Add("AccountId", typeof(string));
            table.Columns.Add("AccountCode", typeof(string));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("Type", typeof(string));
            table.Columns.Add("Class", typeof(string));

            foreach (var account in accounts.EnumerateArray())
            {
                var accountId = Str(account, "AccountID");
                if (string.IsNullOrWhiteSpace(accountId))
                {
                    continue;
                }

                var code = Str(account, "Code");
                if (string.IsNullOrWhiteSpace(code))
                {
                    // dbo.XroAccount carries a UNIQUE CONSTRAINT on AccountCode that is NOT
                    // filtered, so SQL Server permits exactly one NULL across the whole table.
                    // Xero omits Code on bank accounts, and there is more than one of those, so a
                    // straight copy of NULL would fail the second one and roll back the load.
                    // Derive a stable code from the AccountId -- same shape as the column's own
                    // default, but deterministic, so it does not churn on every run.
                    code = "BANK-" + accountId.Replace("-", "").ToUpperInvariant();
                    synthesised++;
                }

                table.Rows.Add(
                    Trim(accountId),
                    Trim(code),
                    Trim(Str(account, "Name")),
                    // Type and Class are stored exactly as Xero sends them (REVENUE, DIRECTCOSTS,
                    // EXPENSE, OTHERINCOME, ...) because Metabase and FinancialKpiService filter
                    // on the raw values.
                    Trim(Str(account, "Type")),
                    Trim(Str(account, "Class")));
            }

            return table;
        }

        private static string? Str(JsonElement el, string prop) =>
            el.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

        private static object Trim(string? s)
        {
            if (string.IsNullOrEmpty(s)) return DBNull.Value;
            return s.Length <= MaxLen ? s : s.Substring(0, MaxLen);
        }
    }
}

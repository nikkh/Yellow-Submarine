using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace YellowSubmarine.Common
{
    public static class Utils
    {
        private static readonly string sqlConnectionString = Environment.GetEnvironmentVariable("SQLConnectionString");
        public static async Task UpsertResults(ExplorationResult er)
        {
            using (SqlConnection connection = new SqlConnection(sqlConnectionString))
            {
                if (connection.State == ConnectionState.Closed) connection.Open();
                SqlCommand command = connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = connection;
                command.CommandText = "UpsertLog";
                command.Parameters.Add("@PathHash", SqlDbType.NVarChar).Value = CalculateHashPath(er.RequestId, er.Path);
                command.Parameters.Add("@RequestId", SqlDbType.NVarChar).Value = er.RequestId;
                command.Parameters.Add("@Path", SqlDbType.NVarChar).Value = er.Path;
                command.Parameters.Add("@ResultType", SqlDbType.NVarChar).Value = er.Type.ToString();
                command.Parameters.Add("@Acls", SqlDbType.NVarChar).Value = er.Acls;
                command.Parameters.Add("@ETag", SqlDbType.NVarChar).Value = er.ETag;
                command.Parameters.Add("@ContentLength", SqlDbType.BigInt).Value = er.ContentLength;
                await command.ExecuteNonQueryAsync();
            }
        }


        public static string CalculateHashPath(string requestId, string path)
        {
            string requestIdPath = requestId + path;
            byte[] requestIdPathBytes = Encoding.ASCII.GetBytes(requestIdPath);
            byte[] md5hash = null;
            using (var md5 = MD5.Create())
            {
                md5hash = md5.ComputeHash(requestIdPathBytes);
            }
            return Convert.ToBase64String(md5hash);
        }

        private static string BuildPageRequestKey(string requestId, string path, int pageNumber) 
        {
            var pathHash = CalculateHashPath(requestId, path);
            return $"{pathHash}::{pageNumber}";
        }

        public static async Task<bool> PageAlreadyProcessedAsync(string requestId, string path, int pageNumber)
        {
            bool result = false;
            string pageRequestKey = BuildPageRequestKey(requestId, path, pageNumber);
            using (SqlConnection connection = new SqlConnection(sqlConnectionString))
            {
                connection.Open();
                SqlCommand udf = new SqlCommand("SELECT [dbo].[ChkPage](@PageRequestKey)", connection);
                SqlParameter requestKey = new SqlParameter("@PageRequestKey", SqlDbType.NVarChar)
                {
                    Value = pageRequestKey
                };
                udf.Parameters.Add(requestKey);
                var res = await udf.ExecuteScalarAsync();
                int resi = (int)res;
                if (resi > 0) result = true;
            }
            return result;

        }

        public static async Task LogPageCompletionAsync(string requestId, string path, int pageNumber)
        {
            string pageRequestKey = BuildPageRequestKey(requestId, path, pageNumber);
            using (SqlConnection connection = new SqlConnection(sqlConnectionString))
            {
                if (connection.State == ConnectionState.Closed) connection.Open();
                SqlCommand command = connection.CreateCommand();
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = connection;
                command.CommandText = "LogPageCompletion";
                command.Parameters.Add("@PageRequestKey", SqlDbType.NVarChar).Value = pageRequestKey;
                await command.ExecuteNonQueryAsync();
            }
        }

        public static async Task<bool> AlreadyProcessedAsync(DirectoryExplorationRequest dir)
        {
            bool result = false;
            using (SqlConnection connection = new SqlConnection(sqlConnectionString))
            {
                connection.Open();
                SqlCommand udf = new SqlCommand("SELECT [dbo].[ChkPathHash](@PathHash)", connection);
                SqlParameter hash = new SqlParameter("@PathHash", SqlDbType.NVarChar)
                {
                    Value = CalculateHashPath(dir.RequestId, dir.StartPath)
                };
                udf.Parameters.Add(hash);
                var res = await udf.ExecuteScalarAsync(); 
                int resi = (int) res;
                if (resi > 0) result = true;
            }
            return result;
        }
    }
}

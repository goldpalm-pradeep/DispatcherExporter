using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Data.Odbc;

namespace DispatcherExporter
{
    public class GenericDatabase
    {
        protected const int MAX_CONNECTION_RETIRES = 10;
        protected const int MILISECONDS_BETWEEN_RETRIES = 2*60*1000;

        public enum DatabaseType
        {
            None,
            SqlServer,
            MySQL,
            Access
        }

        public enum DatabaseUpdateType
        {
            Update = 1,
            Insert = 2,
            Fail = 3
        }

        protected DatabaseType _databaseType = DatabaseType.None;
        protected string _connectionString;
        protected string _databaseName;

        protected SqlConnection _dbConnection;
        protected OleDbConnection _oleDbConnection;
        protected OdbcConnection _odbcConnection;

        public DatabaseType Type
        {
            get
            {
                return _databaseType;
            }
        }

        //Created by Arjun
        public SqlConnection Connection
        {
            get
            {
                return _dbConnection;
            }
        }

        public void CreateIndex(string tableName, string indexName, List<string> fieldNames, bool isUnique)
        {
            DispatcherExporter.Database.CreateIndex(_oleDbConnection, isUnique, tableName, indexName, fieldNames.ToArray());
        }

        public GenericDatabase(string connectionString, string databaseName)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;

            _databaseType = DatabaseType.SqlServer;
            ConnectToDB();
        }

        public GenericDatabase(string connectionString, string databaseName, DatabaseType dbType)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
            _databaseType = dbType;

            ConnectToDB();
        }

        protected void ConnectToSqlServerDB()
        {
            int currentRetry = 1;
            bool connectionSuccess = false;
            while (!connectionSuccess)
            {
                try
                {
                    _dbConnection = new SqlConnection(_connectionString + _databaseName);
                    _dbConnection.Open();
                    connectionSuccess = true;
                }
                catch (Exception ex)
                {
                    ++currentRetry;
                    if (currentRetry > MAX_CONNECTION_RETIRES) break;                 
                    System.Threading.Thread.Sleep(MILISECONDS_BETWEEN_RETRIES);
                }
            }

            currentRetry = 1;
            connectionSuccess = false;
            while (!connectionSuccess)
            {
                try
                {
                    _oleDbConnection = new OleDbConnection("Provider=SQLOLEDB;" + _connectionString + _databaseName);
                    _oleDbConnection.Open();
                    connectionSuccess = true;
                }
                catch (Exception ex)
                {
                    ++currentRetry;
                    if (currentRetry > MAX_CONNECTION_RETIRES) break;                 
                    System.Threading.Thread.Sleep(MILISECONDS_BETWEEN_RETRIES);
                }
            }
        }

        protected void ConnectToMySqlDB()
        {
            int currentRetry = 1;
            bool connectionSuccess = false;

            while (!connectionSuccess)
            {
                try
                {
                    _odbcConnection = new OdbcConnection(_connectionString + _databaseName);
                    _odbcConnection.Open();
                    connectionSuccess = true;
                }
                catch (Exception ex)
                {
                    ++currentRetry;
                    if (currentRetry > MAX_CONNECTION_RETIRES) break;                
                    System.Threading.Thread.Sleep(MILISECONDS_BETWEEN_RETRIES);
                }
            }
        }

        public OleDbConnection GetDBConnection()
        {
            return _oleDbConnection;
        }

        protected void ConnectToDB()
        {
            if ((_databaseType == DatabaseType.MySQL) || (_databaseType == DatabaseType.Access))
            {
                ConnectToMySqlDB();
            }
            else
            {
                ConnectToSqlServerDB();
            }
        }

        ~GenericDatabase()
        {
            try
            {
                if (_dbConnection != null)
                {
                    _dbConnection.Close();
                }

                if (_oleDbConnection != null)
                {
                    _oleDbConnection.Close();
                }

                if (_odbcConnection != null)
                {
                    _odbcConnection.Close();
                }
            }
            catch (Exception) { }
        }

        public void Disconnect()
        {
            try
            {
                _oleDbConnection.Close();
            }
            catch (Exception) { }
            try
            {
                _dbConnection.Close();
            }
            catch (Exception) { }
            try
            {
                _odbcConnection.Close();
            }
            catch (Exception) { }
        }

        public void AddRecord(string tableName, string[,] tableFields)
        {
            if (_odbcConnection == null) { return; }

            DispatcherExporter.Database.AddRecord(_odbcConnection, tableName, tableFields);
        }

        public int ExecuteNonQuery(string query)
        {
            if (_databaseType == DatabaseType.SqlServer)
            {
                if (_dbConnection.State != ConnectionState.Open)
                {
                    _dbConnection.Open();
                }
                SqlCommand command = new SqlCommand(query, _dbConnection);
                return command.ExecuteNonQuery();
            }
            else if (_databaseType == DatabaseType.MySQL || _databaseType == DatabaseType.Access)
            {
                if (_odbcConnection.State != ConnectionState.Open)
                {
                    _odbcConnection.Open();
                }
                OdbcCommand oldbCommand = new OdbcCommand(query, _odbcConnection);
                int returnValue = oldbCommand.ExecuteNonQuery();
                oldbCommand.Dispose();
                return returnValue;
            }

            return 0;
        }

        public int ExecuteNonQuery(string query, List<SqlParameter> parameters)
        {
            SqlCommand command = new SqlCommand(query, _dbConnection);
            foreach (SqlParameter param in parameters)
            {
                command.Parameters.Add(param);
            }

            return command.ExecuteNonQuery();
        }

       

        /// <summary>
        /// Returns the maximum field value for the given table.
        /// </summary>
        public object GetMaxValue(string tableName, string fieldName, string condition)
        {
            string query = "";

            if (condition != "")
            {
                query = string.Format("SELECT MAX {0} FROM {1} WHERE {2}", fieldName, tableName, condition);
            }
            else
            {
                query = string.Format("SELECT MAX {0} FROM {1}", fieldName, tableName);
            }

            return GetScalar(query);
        }

        /// <summary>
        /// Returns the maximum field value for the given table.
        /// </summary>
        public object GetMaxValue(string tableName, string fieldName)
        {
            return GetMaxValue(tableName, fieldName, "");
        }

        /// <summary>
        /// Returns the minimum field value for the given table.
        /// </summary>
        public object GetMinValue(string tableName, string fieldName, string condition)
        {
            string query = "";

            if (condition != "")
            {
                query = string.Format("SELECT MIN {0} FROM {1} WHERE {2}", fieldName, tableName, condition);
            }
            else
            {
                query = string.Format("SELECT MIN {0} FROM {1}", fieldName, tableName);
            }

            return GetScalar(query);
        }

        /// <summary>
        /// Returns the minimum field value for the given table.
        /// </summary>
        public object GetMinValue(string tableName, string fieldName)
        {
            return GetMinValue(tableName, fieldName, "");
        }

        /// <summary>
        /// Returns the first value of the first column in the query.
        /// </summary>
        public object GetScalar(string query)
        {
            if (_dbConnection != null)
            {
                SqlCommand command = new SqlCommand(query, _dbConnection);
                return command.ExecuteScalar();
            }
            else if (_odbcConnection != null)
            {
                if (_odbcConnection.State != ConnectionState.Open)
                {
                    _odbcConnection.Open();
                }
                OdbcCommand oldbCommand = new OdbcCommand(query, _odbcConnection);
                return oldbCommand.ExecuteScalar();
            }

            return "";
        }

        /// <summary>
        /// Returns an empty DataTable with field names
        /// </summary>
        public DataTable GetTableFields(string tableName)
        {
            return GetDataTable("SELECT TOP 0 * FROM " + tableName);
        }


        public DataTable GetDataTable(string query)
        {
            DataTable table = new DataTable("");
            return GetDataTable(query, table, null);
        }

        public DataTable GetDataTable(string query, List<SqlParameter> parameters)
        {
            DataTable table = new DataTable("");
            return GetDataTable(query, table, parameters);
        }

        public DataTable GetDataTable(string query, DataTable table)
        {
            return GetDataTable(query, table, null);
        }

        public DataTable GetDataTable(string query, DataTable table, List<SqlParameter> parameters)
        {
            SqlCommand command = new SqlCommand(query, _dbConnection);
            SqlDataAdapter dataAdapter = new SqlDataAdapter(command);
            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(dataAdapter);
            command.CommandTimeout = 0;
            if (parameters != null)
            {
                foreach (SqlParameter parameter in parameters)
                {
                    command.Parameters.Add(parameter);
                }
            }

            // Populate a new datatable from the query
            if (_databaseType == DatabaseType.Access || _databaseType == DatabaseType.MySQL)
            {
                if (_odbcConnection.State != ConnectionState.Open)
                {
                    _odbcConnection.Open();
                }
                OdbcDataAdapter da = new OdbcDataAdapter(query, _odbcConnection);
                da.Fill(table);
            }
            else
            {
                dataAdapter.Fill(table);
            }

            return table;
        }

        public DataSet GetDataSet(string query)
        {
            SqlCommand command = new SqlCommand(query, _dbConnection);
            command.CommandTimeout = 300;
            SqlDataAdapter dataAdapter = new SqlDataAdapter(command);

            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(dataAdapter);

            //command.Parameters.Add

            // Populate a new DataSet
            DataSet ds = new DataSet();
            dataAdapter.Fill(ds);

            return ds;
        }

        /// <summary>
        /// Adds a record and populates the ID field.  Assumes the ID field is named "ID".        
        /// </summary>
        public void AddUpdateRecord(string tableName, DataRow data, string[] indexFieldNames)
        {
            List<string> fieldNames = new List<string>();
            List<string> fieldValues = new List<string>();
            DispatcherExporter.Database.GetFieldNamesAndValues(data, fieldNames, fieldValues);

            List<string> indexFieldValues = DispatcherExporter.Database.GetIndexFieldValues(indexFieldNames, data);

            bool hasIndexFieldValues = false;

            foreach (string value in indexFieldValues)
            {
                if (value != "")
                {
                    hasIndexFieldValues = true;
                    break;
                }
            }

            int recordID = 0;
            if (hasIndexFieldValues)
            {
                recordID = Convert.ToInt32(DispatcherExporter.Database.RecordExistsID(_oleDbConnection, tableName, indexFieldNames, indexFieldValues.ToArray()));
            }

            if (recordID > 0)
            {
                string[,] condiations = new string[1, 3];
                condiations[0, 0] = "ID";
                condiations[0, 1] = "=";
                condiations[0, 2] = recordID.ToString();
                DispatcherExporter.Database.UpdateTable(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray(), condiations);
            }
            else
            {
                DispatcherExporter.Database.AddRecord(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray());
                int id = DispatcherExporter.Database.GetRecordID(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray(), false);
                data["ID"] = id.ToString();
            }
        }

        public object GetScalarValue(string query)
        {
            DataTable table = GetDataTable(query);
            if (table.Rows.Count <= 0) return "";

            return table.Rows[0][0];
        }

        /// <summary>
        /// Adds a record and populates the ID field.  Assumes the ID field is named "ID".        
        /// </summary>
        public int AddUpdateRecord(string tableName, List<string> fieldNames, List<string> indexFieldNames, List<string> fieldValues, List<string> indexFieldValues)
        {
            bool hasIndexFieldValues = false;

            foreach (string value in indexFieldValues)
            {
                if (value != "")
                {
                    hasIndexFieldValues = true;
                    break;
                }
            }

            int recordID = 0;
            if (hasIndexFieldValues)
            {
                recordID = Convert.ToInt32(DispatcherExporter.Database.RecordExistsID(_oleDbConnection, tableName, indexFieldNames.ToArray(), indexFieldValues.ToArray()));
            }

            if (recordID > 0)
            {
                string[,] condiations = new string[1, 3];
                condiations[0, 0] = "ID";
                condiations[0, 1] = "=";
                condiations[0, 2] = recordID.ToString();
                DispatcherExporter.Database.UpdateTable(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray(), condiations);
            }
            else
            {
                DispatcherExporter.Database.AddRecord(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray());
                recordID = DispatcherExporter.Database.GetRecordID(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray(), false);
            }

            return recordID;
        }

        public void UpdateRecord(string tableName, DataRow data)
        {
            List<string> fieldNames = new List<string>();
            List<string> fieldValues = new List<string>();
            DispatcherExporter.Database.GetFieldNamesAndValues(data, fieldNames, fieldValues);

            int recordID = (int)data["ID"];

            string[,] condiations = new string[1, 3];
            condiations[0, 0] = "ID";
            condiations[0, 1] = "=";
            condiations[0, 2] = recordID.ToString();
            DispatcherExporter.Database.UpdateTable(_oleDbConnection, tableName, fieldNames.ToArray(), fieldValues.ToArray(), condiations);
        }

        /// <summary>
        /// Deletes from the tableName table any records with ID field value idValue.
        /// </summary>
        public void DeleteRecord(string tableName, string idValue)
        {
            DeleteRecord(tableName, "ID", idValue);
        }

        /// <summary>
        /// Deletes from the tableName table any records with idFieldName equal to idValue.
        /// </summary>
        public void DeleteRecord(string tableName, string idFieldName, string idValue)
        {
            string query = "";

            query += string.Format("DELETE FROM {0} WHERE {1} = {2}", tableName, idFieldName, idValue);
            ExecuteNonQuery(query);
        }

        /// <summary>
        /// Returns the connection string with the database name at the end.
        /// </summary>
        protected string GetFullConnString()
        {
            return _connectionString + _databaseName;
        }

        public void BatchInsert(string tableName, DataTable dTable)
        {
            if (_databaseType == DatabaseType.SqlServer)
            {
                if (_dbConnection.State != ConnectionState.Open)
                {
                    _dbConnection.Open();
                }
                SqlBulkCopy bCopy = new SqlBulkCopy(_dbConnection, SqlBulkCopyOptions.TableLock, null);
                bCopy.DestinationTableName = tableName;
                bCopy.WriteToServer(dTable);
            }
            else
            {
                throw new Exception("No support for bulk inserts in databases other than SQL Server!!");
            }
        }
    }
}

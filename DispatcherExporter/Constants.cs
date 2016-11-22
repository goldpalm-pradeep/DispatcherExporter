using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace DispatcherExporter
{
    static class Constants
    {
        public const string SOURCE_NAME = "DataProfilingValidations";
        public const string SITE_NAME = "DataProfilingValidations";
        public const string ROOT = "LocalRoot";
        public const string ROOT_DIRECTORY = "Poplicus\\TEMP";
        public const string AWS_ACCESSKEYID = "AWS_ACCESSKEYID";
        public const string AWS_SECRETACCESSKEY = "AWS_SECRETACCESSKEY";
        public const string AWS_BUCKET_NAME = "AWS_BUCKET_NAME";
        public const string INVALID_CHAR = "INVALID_CHAR";
        public const string IN_INVALID_CHAR = "IN_INVALID_CHAR";
        public const string START_END_INVALID_CHAR = "START_END_INVALID_CHAR";
        public const string CONNECTION_STRING = "DB_CONNECTION_STRING";
        public const string DATABASE_NAME = "DB_DATABASE_NAME_1";
        public const string XML_TYPE = "XML";
        public const string CSV_TYPE = "CSV";
        public const string XLS_TYPE = "XLS";
        public const string XLSX_TYPE = "XLSX";
        public const string QUERY_GET_VALID_ROWS = "Select " + DB_SOURCE_ID + ", " + DB_SOURCE_NAME + ", " + DB_SQSMESSAGE + "," + DB_CREATED_DATE + " from " + INPUT_TABLE_NAME + " where Status!=1";
        public const string DB_SOURCE_NAME = "SourceName";
        public const string DB_SOURCE_ID = "SourceID";
        public const string DB_SQSMESSAGE = "SQSMessage";
        public const string DB_CREATED_DATE = "CreatedDate";
        public const string DB_DATE = "Date";
        public const string DB_S3PATH = "S3Path";
        public const string DB_FIELD_NAME = "FieldName";
        public const string DB_VALUE = "Value";
        public const string DB_DESCRIPTION = "Description";
        public const string DB_RECID = "RecID";
        public const string INPUT_TABLE_NAME = "PopXMLSQSMessages";
        public const string OUTPUT_TABLE_NAME = "DataProfilingResult";
    }
}

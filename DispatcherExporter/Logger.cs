using System;
using System.Collections.Generic;
using System.Text;
using log4net;
using log4net.Config;

namespace Functions
{
    /// <summary>
    /// added silly letting prefix to make intellisense order in the order of severity
    /// </summary>
    public enum LogMessageLevel
    {
        A_Fatal,
        B_Error,
        C_Warn,
        D_Info,
        E_Debug
    }

    public class Logger
    {
        private log4net.ILog _log;

        private string _clientName = "";
        private string _siteName = "";
        private string _scanID = "";

        protected static Logger _instance = null;

        protected static Logger Instance()
        {
            if (_instance == null)
            {
                throw new Exception("Please call the Init function before trying to use the Logger class");
            }

            return _instance;
        }

        public static void LogMessage(string message, LogMessageLevel level)
        {
            LogMessage(message, level, "", "");
        }

        public static void LogMessage(string message, LogMessageLevel level, string tableName, string recordID)
        {
            log4net.GlobalContext.Properties["ClientName"] = _instance._clientName;
            log4net.GlobalContext.Properties["SiteName"] = _instance._siteName;
            log4net.GlobalContext.Properties["ScanID"] = _instance._scanID;
            log4net.GlobalContext.Properties["TableRef"] = tableName;
            log4net.GlobalContext.Properties["RecordID"] = recordID;

            object msg = (object)message;
            if (level == LogMessageLevel.A_Fatal)
            {                
                _instance._log.Fatal(msg);
            }
            else if (level == LogMessageLevel.B_Error)
            {
                _instance._log.Error(msg);
            }
            else if (level == LogMessageLevel.C_Warn)
            {
                _instance._log.Warn(msg);
            }
            else if (level == LogMessageLevel.D_Info)
            {
                _instance._log.Info(msg);
            }
            else if (level == LogMessageLevel.E_Debug)
            {
                _instance._log.Debug(msg);
            }
        }

        public static void LogException(Exception ex, LogMessageLevel level)
        {
            LogException(ex, level, "");
        }

        public static void LogException(Exception ex, LogMessageLevel level, string additionalMessageContent)
        {
            string message = additionalMessageContent + "\r\n\r\nMessage: " + ex.Message +
                "\r\n\r\nSource: " + ex.Source + "\r\n\r\nStackTrace: " + ex.StackTrace + "\r\n\r\nTargetSite: " + ex.TargetSite;
            message = message.Trim();

            LogMessage(message, level);
        }

        public static void Init(string clientName, string siteName)
        {
            _instance = new Logger(clientName, siteName);
        }

        protected Logger(string clientName, string siteName)
        {
            log4net.GlobalContext.Properties["LogName"] = DispatcherExporter.FileOperation.GetRootPath("Logs") + clientName + "_" + siteName;
            _log = log4net.LogManager.GetLogger(clientName + "_" + siteName);
            log4net.Config.XmlConfigurator.Configure();
            _clientName = clientName;
            _siteName = siteName;
        }

        protected Logger(string clientName, string siteName, string scanID)
        {
            log4net.GlobalContext.Properties["LogName"] = DispatcherExporter.FileOperation.GetRootPath("Logs") + clientName + "_" + siteName + "_" + scanID;
            _log = log4net.LogManager.GetLogger(clientName + "_" + siteName);
            log4net.Config.XmlConfigurator.Configure();
            _clientName = clientName;
            _siteName = siteName;
            _scanID = scanID;
        }

        public static void SetScanID(string scanID)
        {
            Instance()._scanID = scanID;
        }

        private object FormatMessage(object message, string tableName, string id)
        {
            if (tableName == "" && id == "")
            {
                return FormatMessage(message);
            }

            object msg = "[" + _scanID + " - " + tableName + ": " + id.ToString() + "]: " + message;
            return msg;
        }

        private object FormatMessage(object message)
        {
            object msg = "";
            if (_scanID != "")
            {
                msg = "[" + _scanID + "]: " + message;
            }
            else
            {
                msg = message;
            }
            return msg;
        }        
    }
}

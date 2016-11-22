using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Functions;
using System.Data;
using System.Configuration;
using System.IO;
using Functions;
using System.Diagnostics;
using System.Globalization;
using System.Windows.Forms;
using System.Reflection;

namespace DispatcherExporter
{
    class Utility
    {
        #region Variable Declaration
        public static GenericDatabase database = null;
        #endregion

        #region GetDBConnection
        /// <summary>
        /// GetDBConnection
        /// </summary>
        /// <returns></returns>
        private static GenericDatabase GetDBConnection()
        {
            GenericDatabase database = null;
            try
            {
                database = new GenericDatabase(ConfigurationManager.AppSettings.Get(Constants.CONNECTION_STRING), ConfigurationManager.AppSettings.Get(Constants.DATABASE_NAME), GenericDatabase.DatabaseType.SqlServer);
            }
            catch (Exception Ex)
            {
                //Utility.SendErrorMail("", Ex.Message);
                Logger.LogMessage(Ex.Message, LogMessageLevel.B_Error);
            }
            return database;
        }
        #endregion

        #region DispatcherExportering
        /// <summary>
        /// Crawler Monitoring Daily
        /// </summary>
        /// <returns></returns>
        public static bool DispatcherExportering(string inp)
        {
            //DB Connection
            database = Utility.GetDBConnection();
            //Variable Declaration
            string qry = string.Empty;
            try
            {
                //database.ExecuteNonQuery("TRUNCATE TABLE [Utility].[dbo].[Exporter]");
                DataTable useDispatcherSiteName = new DataTable();
                database.GetDataTable("SELECT PC.SiteName,CT.CrawlerType, nc.DispatcherTableName FROM [WebCrawlerConfiguration].[dbo].[ProjectConfiguration] as PC LEFT JOIN [WebCrawlerConfiguration].[dbo].[PopCrawlerType] as CT ON PC.crawlerTypeID = CT.ID LEFT JOIN [WebCrawlerConfiguration].[dbo].[NavigationConfiguration] as NC ON PC.ID = NC.[ProjectConfigurationID] Where NC.UseDispatcher = 1 and pc.sitename not like '%Histori%'", useDispatcherSiteName);
                foreach (DataRow dr in useDispatcherSiteName.Rows)
                {
                    //Variable Declaration
                    string DispatcherTablePath = string.Empty;
                    string parameters = string.Empty;
                    string siteName = string.Empty;
                    string sourceName = string.Empty;
                    string psSourceName = string.Empty;
                    
                    psSourceName = dr[0].ToString();
                    if (psSourceName.ToUpper().Contains("IL_PROCUREMENT_1_DISP"))
                        psSourceName = "IL_Procurement_1_First";
                    else if (psSourceName.ToUpper().Contains("VA_PROCUREMENT_1_DATA"))
                        psSourceName = "VA_Procurement_1_Link";
                    string computerName = System.Windows.Forms.SystemInformation.ComputerName;


                    qry = "SELECT SiteName, Path,Parameters, Time, Schedule, SourceID, Priority, OnlyRunOnMachines, DoNotRunOnMachines FROM [configsetting].[dbo].[ProgramSchedule] WHERE enabled = 1 AND (firstinstanceparameters like '%Disp%' OR Parameters like '%Link%') AND schedule like '%Daily%' AND path != 'Y:\\FTP_DATA\\ReleaseFBO4andFBO2\\DataCrawler.exe' AND SiteName != 'FBO_DocDownload' and SiteName = '" + psSourceName + "' AND (CHARINDEX('[{0}]', OnlyRunOnMachines) > 0 AND CHARINDEX('[{0}]', DoNotRunOnMachines) = 0)";
                    qry = string.Format(qry, computerName);
                    DataTable programSchedule = new DataTable();
                    database.GetDataTable(qry, programSchedule);
                    if (programSchedule.Rows.Count < 1)
                        continue;
                    foreach (DataRow drPS in programSchedule.Rows)
                    {
                        //Getting the Database Table Name from the DATABASES
                        string[] tablePath = drPS[1].ToString().Split('\\');
                        string tableName = string.Empty;
                        if (tablePath.Length > 1)
                        {
                            string[] pathSplit = tablePath[2].ToString().Split('_');
                            if (tablePath[2].ToUpper().Contains("DAILY"))
                            {
                                if (pathSplit[1].ToUpper().ToString() == "DAILY")
                                    pathSplit[1] = "DAILYSET5";
                                tableName = getDBName(pathSplit[1].ToUpper().ToString());
                            }
                            else if (tablePath[2].ToUpper().Contains("AGENCY"))
                            {
                                tableName = "WebCrawlerOutput_AgencyNews";
                            }
                            else if (tablePath[2].ToUpper().Contains("DOCDOWNLOAD"))
                            {
                                tableName = "WebCrawlerOutput_FBODOC";
                            }
                        }
                        if (tableName != string.Empty)
                            DispatcherTablePath = "[" + tableName + "].[DBO].[" + dr[2].ToString() + "]";

                        if (drPS[2].ToString().ToUpper().Contains("LINK") || drPS[2].ToString().ToUpper().Contains("DATA") || drPS[2].ToString().ToUpper().Contains("FIRST"))
                        {
                            qry = string.Empty;
                            qry = "SELECT top 1 sourcename FROM [Utility].[dbo].[PopXMLSQSMessages] where SourceID = '" + drPS[5].ToString() + "' AND sourcename not like '%Documents%' AND sourcename not like '%DocDownload%' order by id desc";
                            siteName = database.GetScalarValue(qry).ToString();
                        }
                        parameters = drPS[2].ToString();
                        if (siteName != string.Empty)
                        {
                            string[] parameterSplit = parameters.Split('=');
                            parameters = parameterSplit[0] + "=" + parameterSplit[1] + "=" + siteName + " resetSearch=1";
                            
                        } else
                            parameters = parameters.Trim() + " resetSearch=1";
                        database.ExecuteNonQuery("INSERT INTO [Utility].[dbo].[Exporter] ([SiteName],[Path],[Parameters],[Time],[Schedule],[SourceID],[Priority],[CrawlerType],[UseDispatcher],[DispatcherTablePath],[OnlyRunOnMachines],[DoNotRunOnMachines]) VALUES ('" + drPS[0].ToString() + "', '" + drPS[1].ToString() + "', '" + parameters + "', '" + drPS[3].ToString() + "', '" + drPS[4].ToString() + "', '" + drPS[5].ToString() + "', '" + drPS[6].ToString() + "', '" + dr[1].ToString() + "', '1', '" + DispatcherTablePath + "', '" + drPS[7].ToString() + "', '" + drPS[8].ToString() + "')");
                        foreach (Process p in Process.GetProcesses("."))
                        {
                            try
                            {
                                if (p.MainWindowTitle.Length > 0)
                                {
                                    if (p.MainWindowTitle.ToString().ToUpper() == "POPLICUS_" + drPS[0].ToString().ToUpper())
                                    {
                                        p.Kill();
                                        Console.Write("\r\n Window Title:" + p.MainWindowTitle.ToString());
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.LogMessage(e.Message.ToString(), LogMessageLevel.B_Error);
                            }
                        }
                        //Exporting 
                        DataTable launchExporter = new DataTable();
                        database.GetDataTable("SELECT path, parameters, DispatcherTablePath FROM [Utility].[dbo].[Exporter] WHERE SITENAME ='" + drPS[0].ToString() + "'", launchExporter);
                        foreach (DataRow drlaunch in launchExporter.Rows)
                        {
                            string valChk = string.Empty;
                            valChk = database.GetScalarValue("IF OBJECT_ID (N'" + drlaunch[2].ToString() + "', N'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res").ToString();
                            if (valChk == "1")
                            {
                                int cntActvie, cntComplete, cntRunning = 0;
                                cntActvie = getActivecount(drlaunch[2].ToString());
                                cntComplete = getCompletedcount(drlaunch[2].ToString());
                                cntRunning = getNotRunningecount(drlaunch[2].ToString());
                                string qryExe = "UPDATE [Utility].[dbo].[Exporter] SET [ActiveCount] = '" + cntActvie + "', [RunningCount] = '" + cntRunning + "', [CompletedCount] ='" + cntComplete + "' WHERE SITENAME ='" + drPS[0].ToString() + "'";
                                database.ExecuteNonQuery(qryExe);
                                string path = drlaunch[0].ToString().Trim();
                                database.ExecuteNonQuery("UPDATE " + drlaunch[2].ToString() + " SET STATUS = 1");
                                //if (drlaunch[0].ToString().ToUpper() == @"Y:\FTP_DATA\RELEASE_DOCDOWNLOAD\DATACRAWLER.EXE")
                                //{
                                //    path = @"Y:\FTP_DATA\Release_FBO_DocDownload\DataCrawler.exe";
                                //}
                                //else if (drlaunch[0].ToString().ToUpper() == @"Y:\FTP_DATA\RELEASE_DAILY\DATACRAWLER.EXE")
                                //{
                                //    path = @"Y:\FTP_DATA\Release_DailySet5\DataCrawler.exe";
                                //}
                                string command = path + " " + drlaunch[1].ToString().Trim();
                                //Launch the Exporter
                                Process proc = new Process();
                                ProcessStartInfo startInfo = new ProcessStartInfo();
                                startInfo.FileName = "cmd.exe";
                                startInfo.Arguments = "/c " + command;
                                proc.StartInfo = startInfo;
                                proc.Start();
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogMessage(e.Message.ToString(), LogMessageLevel.B_Error);
            }
            return false;
        }
        #endregion

        #region getDBName
        /// <summary>
        /// getDBName
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        private static string getDBName(string param)
        {
            //DB Connection
            database = Utility.GetDBConnection();
            DataTable dtDBName = new DataTable();
            database.GetDataTable("select name from sys.sysdatabases where name like '%WebCrawlerOutput%' order by name", dtDBName);

            foreach (DataRow dr in dtDBName.Rows)
            {
                if (dr[0].ToString().ToUpper().Trim().Contains(param.ToUpper()))
                {
                    return dr[0].ToString().Trim();
                    break;
                }
            }
            return "";
        }
        #endregion

        #region Get Dispatcher Status
        private static int getActivecount(string param)
        {
            //DB Connection
            database = Utility.GetDBConnection();
            return Convert.ToInt32(database.GetScalarValue("SELECT COUNT(*) FROM " + param + " WHERE STATUS != 1"));
        }

        
        private static int getCompletedcount(string param)
        {
            //DB Connection
            database = Utility.GetDBConnection();
            return Convert.ToInt32(database.GetScalarValue("SELECT COUNT(*) FROM " + param + " WHERE STATUS = 1"));
        }

        private static int getNotRunningecount(string param)
        {
            //DB Connection
            database = Utility.GetDBConnection();
            return Convert.ToInt32(database.GetScalarValue("SELECT COUNT(*) FROM " + param + " WHERE STATUS = 0"));
        }
        #endregion

    }
}

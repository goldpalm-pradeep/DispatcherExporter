#region Copyright Ficstar Software Inc. 2006-2007
/*
 * Class library name: Functions
 * Solution: class library for Ficstar Web Grabber key function references.
 * Project code: FS-WG0401
 * Start date: April 5, 2006
 * Start version: 1.0.0.1
 * 
 * All rights reserved. Reproduction or transmission of this file, or any part thereof,
 * is forbidden without prior written permission of Ficstar Software Inc.
 * */
#endregion

using System.IO.Compression;
using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Windows.Forms;
using System.Collections;
using System.Text.RegularExpressions;
using System.Data;
using System.Data.OleDb;
using System.Data.Odbc;
using System.Data.Common;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Globalization;
using System.Data.SqlClient;
using System.Net.Mail;
using System.Configuration;


namespace DispatcherExporter
{
    public abstract class ConstantValues
    {
        enum Planets
        {
            Mercury,
            Venus,
            Earth,
            Mars,
            Jupiter,
            Saturn,
            Uranus,
            Neptune,
            Pluto
        }

        public const string UrlShowMyIP = "http://www.ficstar.com/ip.php";

    }

    public static class UnicodeChinese
    {
        public static string TableName = "Unicode_Chinese";
        public static string GB = "GB Code";
        public static string Unicode = "Uni-code";
        public static string UTF8 = "UTF-8";
        public static string ChineseCharacter = "Chinese Character";
    }

    public static class IniFile
    {

        //===============================================================================
        // The API Method that get the list of the Sections List
        //———————————————————– 

        [DllImport("kernel32.dll", EntryPoint = "GetPrivateProfileSectionNamesA")]
        static extern int GetSectionNamesListA(byte[] lpszReturnBuffer, int nSize, string lpFileName);

        //——————————————————————————-
        // The API Method that can read the KeyValue
        //————————————————— 

        [DllImport("kernel32.dll", EntryPoint = "GetPrivateProfileStringA")]
        static extern int GetKeyValueA(string strSection, string strKeyName, string strNull, StringBuilder RetVal, int nSize, string strFileName);

        //——————————————————————————-
        // The API Method that can write the KeyValue
        //————————————————— 

        [DllImport("kernel32.dll", EntryPoint = "WritePrivateProfileStringA")]
        static extern long WriteValueA(string lpApplicationName, string lpKeyName, string lpString, string lpFileName);

        //=============================================================================== 

        public static ArrayList GetSectionsList(string FileName)
        {

            //Get The Sections List Method 

            ArrayList arrSec = new ArrayList();
            byte[] buff = new byte[1024];
            GetSectionNamesListA(buff, buff.Length, FileName);
            String s = Encoding.Default.GetString(buff);
            char[] splitter = { ';' };
            String[] names = s.Split(splitter);
            foreach (String name in names)
            {
                if (name != String.Empty)
                {
                    arrSec.Add(name);
                }
            }
            return arrSec;
        }
        public static string GetKeyValue(string FileName, string Section, string KeyName)
        {

            //Reading The KeyValue Method 

            StringBuilder JStr = new StringBuilder(255);
            int i = GetKeyValueA(Section, KeyName, "", JStr, 255, FileName);
            return JStr.ToString();
        }
        public static void WriteValue(string FileName, string Section, string KeyName, string KeyValue)
        {

            //Writing The KeyValue Method 

            WriteValueA(Section, KeyName, KeyValue, FileName);
        }
    }

    public class IniDataReader
    {
        private string iniFile;

        public IniDataReader(string iniFileName)
        {
            iniFile = iniFileName;
        }

        public string GetStringValue(string keyName)
        {
            string valReturn = "";
            if (File.Exists(iniFile))
            {
                StreamReader sr = new StreamReader(iniFile);
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string search = Regex.Match(line, keyName + ".*").Value;
                    if (!(search == ""))
                    {
                        valReturn = search.Replace(keyName + "=", "");
                        break;
                    }
                }
                sr.Close();
            }
            return valReturn;
        }
    }

    public class CpuUsageTracker
    {
        PerformanceCounter cpuCounter;
        Timer timerCpuUsageMonitor = new Timer();
        float cpuUsage;
        float numberCpuUsage;
        int count;

        public CpuUsageTracker(string processName)
        {
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time", processName);
            timerCpuUsageMonitor.Interval = 1;
            timerCpuUsageMonitor.Tick += new EventHandler(timerCpuUsageMonitor_Tick);
            timerCpuUsageMonitor.Start();
        }

        void timerCpuUsageMonitor_Tick(object sender, EventArgs e)
        {
            if (count < 1000)
            {
                count++;
                numberCpuUsage += cpuCounter.NextValue();
            }
            else
            {
                count = 0;
                cpuUsage = numberCpuUsage / 1000;
                numberCpuUsage = 0;
            }
        }

        public float CpuUsage()
        {
            return cpuUsage;
        }
    }

    public class SystemPerformance
    {
        PerformanceCounter cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
        PerformanceCounter ramCounter = new PerformanceCounter("Memory", "Available MBytes");

        Timer timerCpuUsageMonitor = new Timer();
        float cpuUsage;
        float numberCpuUsage;
        int count;

        public SystemPerformance()
        {
            timerCpuUsageMonitor.Interval = 1;
            timerCpuUsageMonitor.Tick += new EventHandler(timerCpuUsageMonitor_Tick);
            timerCpuUsageMonitor.Start();
        }

        void timerCpuUsageMonitor_Tick(object sender, EventArgs e)
        {
            if (count < 1000)
            {
                count++;
                numberCpuUsage += cpuCounter.NextValue();
            }
            else
            {
                count = 0;
                cpuUsage = numberCpuUsage / 1000;
                numberCpuUsage = 0;
            }
        }

        public float CpuUsage()
        {
            return cpuUsage;
        }

        public float CpuUsageCurrent()
        {
            return cpuCounter.NextValue();
        }

        public float AvailableMemory()
        {
            return ramCounter.NextValue();
        }
    }

    public abstract class FileOperation
    {
        public static bool FileInDirectory(string directoryPath, string fileName)
        {
            string[] subDirectoryNames = Directory.GetDirectories(directoryPath);
            foreach (string directoryName in subDirectoryNames)
            {
                if (File.Exists(directoryName + fileName))
                    return true;
            }
            return false;
        }

        public static bool FileInDirectory(string[] directoryPath, string fileName)
        {
            foreach (string directoryName in directoryPath)
            {
                string st = directoryName;
                if (!st.EndsWith("\\"))
                    st += "\\";
                if (File.Exists(st + fileName))
                    return true;
            }
            return false;
        }

        public static void AppendToTextFile(string fileName, string inputString)
        {
            StreamWriter sw = File.AppendText(fileName);
            sw.WriteLine(inputString);
            sw.Close();
        }

        public static void WriteTextFile(string fileName, string inputString)
        {
            if (!File.Exists(fileName))
            {
                StreamWriter sw = File.CreateText(fileName);
                sw.Close();
            }
            TextWriter tw = new StreamWriter(fileName);
            tw.WriteLine(inputString);
            tw.Close();
        }

        public static ArrayList ReadTextFile(string fileName)
        {
            ArrayList al = new ArrayList();
            if (File.Exists(fileName))
            {
                StreamReader sr = new StreamReader(fileName);
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    al.Add(line);
                }
                sr.Close();
            }
            return al;
        }

        public static void ReadTextFile(string fileName, ListBox listBox, bool noDuplicate)
        {
            ArrayList al = ReadTextFile(fileName);
            Text.ArrayListToListBox(al, listBox, noDuplicate);
        }

        public static void RARDirectory(string rarExecutablePath, string directory, string rarFileName)
        {
            Process process = new Process();

            ProcessStartInfo processStartInfo = new ProcessStartInfo(rarExecutablePath);
            processStartInfo.UseShellExecute = false;

            processStartInfo.Arguments = "a -r " + rarFileName + ".rar " + directory;
            process = Process.Start(processStartInfo);
        }

        public static void RARFile(string rarExecutablePath, string fileName, string rarFileName)
        {
            RunApplication(rarExecutablePath, "a -r " + rarFileName + ".rar " + fileName);
        }

        public static void RunApplication(string applicationPath, string arguments)
        {
            Process process = new Process();
            ProcessStartInfo processStartInfo = new ProcessStartInfo(applicationPath);
            processStartInfo.UseShellExecute = false;
            processStartInfo.Arguments = arguments;
            process = Process.Start(processStartInfo);
        }


        public static bool IsValidFilePath(string path)
        {
            string pattern = @"^(([a-zA-Z]\:)|(\\))(\\{1}|((\\{1})[^\\]([^/:*?<>""|]*))+)$";
            Regex reg = new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
            path = path.Replace("/", "\\");
            return reg.IsMatch(path);
        } 


        public static string GetRootPath(string folderName)
        {
            string localDrive = System.IO.Path.GetPathRoot(System.IO.Path.GetTempFileName());
            string folderPath;
            string POPLICUS_ROOT_DRIVE = ConfigurationManager.AppSettings["POPLICUS_ROOT_DRIVE"]; ;
            string POPLICUS_ROOT_FOLDER = ConfigurationManager.AppSettings["POPLICUS_ROOT_FOLDER"]; ;

            string RootPath = POPLICUS_ROOT_DRIVE + POPLICUS_ROOT_FOLDER;

            if (string.IsNullOrEmpty(RootPath))
            {
                folderPath = localDrive + "Poplicus/";
            }
            else
            {
                if (IsValidFilePath(RootPath))
                {
                    if (RootPath.EndsWith("\\") || RootPath.EndsWith("/"))
                        folderPath = RootPath;
                    else
                        folderPath = RootPath + "/";
                }
                else
                {
                    folderPath = localDrive + "Poplicus/";
                }
            }

            folderPath = folderPath + folderName + "/";

            if (!System.IO.Directory.Exists(folderPath))
            {
                System.IO.Directory.CreateDirectory(folderPath);
            }            

            return folderPath;
        }

    }

    public class Ftp
    {
        private string path;
        private string userName;
        private string password;
        private string filePath;

        public Ftp(string path, string userName, string password, string filePath)
        {
            this.path = path;
            this.userName = userName;
            this.password = password;
            this.filePath = filePath;
        }

        public void CreateDirectory()
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(new Uri(path));
            request.Method = WebRequestMethods.Ftp.MakeDirectory;
            request.Credentials = new NetworkCredential(userName, password);
            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
        }

        public void UploadFile()
        {
            UploadFile(100000);
        }

        public void UploadFile(int timeout)
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(new Uri(path));
            request.Timeout = timeout;
            request.KeepAlive = false;
            request.Method = WebRequestMethods.Ftp.UploadFile;
            request.Credentials = new NetworkCredential(userName, password);

            StreamReader sourceStream = new StreamReader(filePath);
            byte[] fileContents = Encoding.UTF8.GetBytes(sourceStream.ReadToEnd());
            sourceStream.Close();
            request.ContentLength = fileContents.Length;

            Stream requestStream = request.GetRequestStream();
            requestStream.Write(fileContents, 0, fileContents.Length);
            requestStream.Close();

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            response.Close();
        }

        public static void UploadFile(string host, string userName, string password,
            string ftpFilePath, string inputfilePath)
        {
            string ftphost = host;
            //here correct hostname or IP of the ftp server to be given  

            string ftpfullpath = "ftp://" + ftphost + @"/" + Text.TextBeforeTag(ftpFilePath, @"\");
            FtpWebRequest ftp = (FtpWebRequest)FtpWebRequest.Create(ftpfullpath);
            ftp.Credentials = new NetworkCredential(userName, password);
            //userid and password for the ftp server to given  

            ftp.KeepAlive = true;
            ftp.UseBinary = true;
            ftp.Method = WebRequestMethods.Ftp.UploadFile;
            FileStream fs = File.OpenRead(inputfilePath);
            byte[] buffer = new byte[fs.Length];
            fs.Read(buffer, 0, buffer.Length);
            fs.Close();
            Stream ftpstream = ftp.GetRequestStream();
            ftpstream.Write(buffer, 0, buffer.Length);
            ftpstream.Close();
        }

        public static void UploadFile(string host, string userName, string password,
            string ftpFilePath, string inputfilePath, int timeout)
        {
            string ftphost = host;
            //here correct hostname or IP of the ftp server to be given  

            string ftpfullpath = "ftp://" + ftphost + ftpFilePath;
            FtpWebRequest ftp = (FtpWebRequest)FtpWebRequest.Create(ftpfullpath);
            ftp.Timeout = timeout;
            ftp.Credentials = new NetworkCredential(userName, password);
            //userid and password for the ftp server to given  

            ftp.KeepAlive = true;
            ftp.UseBinary = true;
            ftp.Method = WebRequestMethods.Ftp.UploadFile;
            FileStream fs = File.OpenRead(inputfilePath);
            byte[] buffer = new byte[fs.Length];
            fs.Read(buffer, 0, buffer.Length);
            fs.Close();
            Stream ftpstream = ftp.GetRequestStream();
            ftpstream.Write(buffer, 0, buffer.Length);
            ftpstream.Close();
        }

        public static bool FileSizeIsEqual(string host, string userName, string password,
            string ftpFilePath, string inputfilePath, int timeout)
        {
            string ftphost = host;
            //here correct hostname or IP of the ftp server to be given  

            string ftpfullpath = "ftp://" + ftphost + ftpFilePath;
            FtpWebRequest ftp = (FtpWebRequest)FtpWebRequest.Create(ftpfullpath);

            ftp.Method = WebRequestMethods.Ftp.GetFileSize;

            System.IO.FileInfo inf = new FileInfo(inputfilePath);


            ftp.Timeout = timeout;
            ftp.Credentials = new NetworkCredential(userName, password);
            //userid and password for the ftp server to given  

            ftp.KeepAlive = true;
            ftp.UseBinary = true;

            try
            {
                WebResponse resp = ftp.GetResponse();
                return (inf.Length == resp.ContentLength);
            }
            catch (Exception)
            {
                return false;
            }
        }
    }

    public abstract class RegexPattern
    {
        public static Regex NotNaturalNumber = new Regex("[^0-9]");
        public static Regex NumberWithoutZero = new Regex("[1-9]");
        public static Regex NaturalNumber = new Regex("0*[1-9][0-9]*");
        public static Regex NotWholeNumber = new Regex("[^0-9]");
        public static Regex NotInteger = new Regex("[^0-9-]");
        public static Regex Integer = new Regex("^-[0-9]+$|^[0-9]+$");
        public static Regex NotPositiveNumber = new Regex("[^0-9.]");
        public static Regex PositiveNumber = new Regex("^[.][0-9]+$|[0-9]*[.]*[0-9]+$");
        public static Regex NumberWithTwoDots = new Regex("[0-9]*[.][0-9]*[.][0-9]*");
        public static Regex NotNumber = new Regex("[^0-9.-]");
        public static Regex NumberWithTwoMinus = new Regex("[0-9]*[-][0-9]*[-][0-9]*");
        static string strValidReal = "^([-]|[.]|[-.]|[0-9])[0-9]*[.]*[0-9]+$";
        static string strValidInteger = "^([-]|[0-9])[0-9]*$";
        public static Regex Number = new Regex("(" + strValidReal + ")|(" + strValidInteger + ")");
        public static Regex Alpha = new Regex("[^a-zA-Z]");
        public static Regex AlphaNumeric = new Regex("[a-zA-Z0-9]");
        public static Regex NonAlphaNumeric = new Regex("[^a-zA-Z0-9]");

        public static Regex Year = new Regex("^[0-9]{4}$");

        public static Regex USDate = new Regex(@"(10|11|12|0?[1-9])(?<sep>[-/])(30|31|2\d|1\d|0?[1-9])\k<sep>(\d{4})"); //required 4 digits year
        public static Regex ChineseDate = new Regex(@"(\d{4})(?<sep>[-/])(10|11|12|0?[1-9])\k<sep>(30|31|2\d|1\d|0?[1-9])"); //required 4 digits year
        //public static Regex ChineseDate = new Regex(@"(\d{4}(\-|/)\d{2}(\-|/)\d{2})");

        public static Regex PhoneNumber = new Regex(@"(\(\d{3}\)( )?\d{3}\-\d{4})|(\d{3}(\-|\.|/)\d{3}(\-|\.)\d{4})");
        /* possible phone number format:
         * (256) 577-6187
         * (256)577-6187
         * 256-577-6187
         * 256.577.6187
         * 256/577-6187
        */
        public static Regex Email = new Regex(@"([\w-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([\w-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)");
        public static Regex ZipCode = new Regex(@"(\d{5})|(\d{5}\-\d{4})");
        public static Regex SSN = new Regex(@"\d{3}\-\d{2}\-\d{4}");

        //Regex URLPattern = new Regex(@"^(?<link>((?<prot>http:\/\/)*(?<subdomain>(www|[^\-\n]*)*)(\.)*(?<domain>[^\-\n]+)\.(?<after>[a-zA-Z]{2,3}[^>\n]*)))");
        //Regex URLPattern = new Regex(@"((ftp|http|https):\/\/|)([\w-]+\.)+[\w-]+(/[\w-./?%&=]*)?");
        public static Regex Url = new Regex(@"([\w-]+\.)+[\w-]+(/[\w-./?%&=]*)?");
        //Regex URLPattern = new Regex(@"(([a-zA-Z][0-9a-zA-Z+\\-\\.]*:)?/{0,2}[0-9a-zA-Z;/?:@&=+$\\.\\-_!~*'()%]+)?(#[0-9a-zA-Z;/?:@&=+$\\.\\-_!~*'()%]+)?");

        public static Regex IP = new Regex(@"((25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)\.){3}(25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)");
        public static Regex Proxy = new Regex(@"((25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)\.){3}(25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)|(([\w-]+\.)+[\w-]+(/[\w-./?%&=]*)?)");

        public static Regex UTF8ChineseInUrl = new Regex("(%([a-zA-Z0-9]){2}){3}");
        public static Regex UTF8CharInUrl = new Regex("(%([a-zA-Z0-9]){2})");

        public static Regex WhiteSpaceString = new Regex(@"[\r|\n|\t]");
    }

    public abstract class FicstarProgram
    {
        public static bool Validating(string programName)
        {
            string html = Web.GetHtml("http://www.ficstar.com/betaprogramexpirychecking.lst");
            if ((html != null) && (html != ""))
            {
                string verifyCode = Text.GetSubstring(html, programName + "=", "\r");
                if (verifyCode == "1")
                    return false;
            }
            return true;
        }
    }

    public abstract class Text
    {
        public static void CreateZipFromString(string fieldName, string input)
        {
            byte[] byteArray = ASCIIEncoding.ASCII.GetBytes(input);
            string encodedText = Convert.ToBase64String(byteArray);

            FileStream destFile = File.Create(fieldName);

            byte[] buffer = Encoding.UTF8.GetBytes(encodedText);
            MemoryStream memoryStream = new MemoryStream();

            using (System.IO.Compression.GZipStream gZipStream = new System.IO.Compression.GZipStream(destFile, System.IO.Compression.CompressionMode.Compress, true))
            {
                gZipStream.Write(buffer, 0, buffer.Length);
            }
        }

        // zero indexed tds
        public static string GetNthTDText(string input, int tdIndex)
        {
            string clipTagEnd = "</TD";
            string clipTagStart = "<TD";
            if (!input.Contains(clipTagStart))
            {
                clipTagEnd = "</td";
                clipTagStart = "<td";
            }

            for (int i = 0; i < tdIndex; i++)
            {
                input = Text.TextAfterTag(input, clipTagEnd);
            }

            string td = Text.GetTextStringBetweenTags(input, clipTagStart, clipTagEnd);
            td = RestoreHTMLSymbols(td).Trim();
            return td;
        }

        public static bool TextStartsWith(string input, string startsWith)
        {
            return input.StartsWith(startsWith);
        }

        public static string RemoveCRLF(string input)
        {
            return input.Replace("\r\n", "");
        }

        public static string GetValidPhoneNumber(string input)
        {
            string result = "";

            string resultPhone = Text.RemoveNonNumericCharacters(input);

            // No valid phone number found
            if ((Text.Length(resultPhone) <= 6) || (Length(resultPhone) >= 17))
            {
                result = "";
                return result;
            }

            // Assumes north american phone numbers
            if ((GetFirstChar(resultPhone) == "1") && (Length(resultPhone) >= 11))
            {
                resultPhone = Text.TextAfterTag(resultPhone, "1");
            }

            string areaCode = "";
            string part1 = "";
            string part2 = "";
            if (Length(resultPhone) >= 10)
            {
                areaCode = MidStr(resultPhone, 1, 3);
                part1 = MidStr(resultPhone, 4, 3);
                part2 = MidStr(resultPhone, 7, 4);
            }
            else if (Length(resultPhone) == 7)
            {
                part1 = MidStr(resultPhone, 1, 3);
                part2 = MidStr(resultPhone, 4, 4);

                result = part1 + "-" + part2;
                return result;
            }

            result = areaCode + "-" + part1 + "-" + part2;

            return result;
        }


        public static string GetFirstEmailAddress(string input)
        {
            string emailRegex = @"([\w-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([\w-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)";
            return GetFirstRegexMatch(input, emailRegex, true);
        }

        public static bool ContainsNumber(string input)
        {
            foreach (char character in input)
            {
                if (character >= 48 && character <= 57)
                {
                    return true;
                }
            }

            return false;
        }

        public static string SplitAddress(GenericDatabase database, string fullAddress, ref string address, ref string city, ref string state, ref string zip, ref string county)
        {
            const string country = "USA";
            if ((Text.LastChar(fullAddress) == ','))
            {
                fullAddress = Text.Trim(Text.TextBeforeLastTag(fullAddress, ","));
            }
            if ((Text.LastChar(fullAddress) == '.'))
            {
                fullAddress = Text.Trim(Text.TextBeforeLastTag(fullAddress, "."));
            }

            if (Text.IsZipCode(Text.GetLastWord(fullAddress)) || (Text.GetPostalCode(fullAddress) != ""))
            {
                if (Text.Length(Text.GetLastWord(fullAddress)) == 9)
                {
                    string tmpZip = Text.GetLastWord(fullAddress);
                    address = Text.TextBeforeLastTag(fullAddress, " ") + " " + Text.Copy(tmpZip, 1, 5) + "-" + Text.Copy(tmpZip, 6, 4);
                }

                SeparateAddress(database, fullAddress, country, ref address, ref city, ref state, ref zip);
            }
            else
            {
                if (Text.Trim(fullAddress) != "")
                {
                    if ((Text.Length(Text.GetLastWord(fullAddress)) != 2) && (state != ""))
                    {
                        fullAddress = Text.ReplaceStateWithAbbr(fullAddress);
                    }

                    if ((Text.Length(Text.GetLastWord(fullAddress)) != 2) && (state != ""))
                    {
                        fullAddress = fullAddress + ", " + state;
                    }

                    SplitAddressWithoutZip(database, fullAddress, ref address, ref city, ref state, ref county);
                }
            }

            return "";
        }

        public static string MidStr(string input, int startIndex, int length)
        {
            return input.Substring(startIndex - 1, length);
        }

        protected static void SplitAddressWithoutZip(GenericDatabase database, string ss, ref string Street, ref string City, ref string State, ref string County)
        {
            string tmp = "";
            if (Text.Length(GetLastWord(ss)) == 2)
            {
                Text.RightSplitString(ss, " ", ref ss, ref State);

                if ((GetLastChar(State) == ",") || (GetLastChar(State) == "."))
                {
                    State = Text.Trim(RemoveLastChar(State));
                }

                if ((GetLastChar(ss) == ",") || (GetLastChar(ss) == "."))
                {
                    ss = Text.Trim(RemoveLastChar(ss));
                }

                if (Pos(",", ss) != 0)
                {
                    Text.RightSplitString(ss, ",", ref Street, ref City);
                }
                else if ((County != "") && (State != ""))
                {
                    tmp = GetCity(database, ss, County, State);
                    if (tmp != "")
                    {
                        City = tmp;
                        Street = Text.TextBeforeTag(ss, tmp);
                    }
                }
            }
        }

        protected static string GetCity(GenericDatabase database, string address, string zip, string state)
        {
            string query = "select * from directory.dbo.CityZipCountyCountry where State = '{0}' and Zip = '{1}'";
            query = string.Format(query, state, zip);

            DataTable cities = database.GetDataTable(query);

            foreach (DataRow city in cities.Rows)
            {
                string cityFromDB = city["City"].ToString();

                if (address.EndsWith(cityFromDB))
                {
                    return cityFromDB;
                }
            }

            return "";
        }

        public static bool IsUpperCase(string input)
        {
            return (input.ToUpper() == input);
        }

        public static Dictionary<string, string> GetNames(string name)
        {
            Dictionary<String, String> lstNames;
            string pattern = @"\s";
            string firstName = "";
            string lastName = "";
            string middleName = "";
            lstNames = new Dictionary<string, string>();
            string[] names = Regex.Split(name, pattern);
            if (names != null)
            {
                if (names.Length == 3)
                {
                    firstName = names[0];
                    middleName = names[1];
                    lastName = names[2];

                }
                else
                {
                    firstName = names[0];
                    lastName = names[names.Length - 1];
                }
            }
            lstNames.Add("FirstName", firstName);
            lstNames.Add("MiddleName", middleName);
            lstNames.Add("LastName", lastName);
            return lstNames;
        }

        public static Dictionary<string, string> GetStateZipCode(string address)
        {
            Dictionary<String, String> lstStateZipCode;
            string stateCode = "";
            string zipCode = "";
            string stateZipCode = "";
            lstStateZipCode = new Dictionary<string, string>();
            
            if (address.Contains(","))
                stateZipCode = address.Substring(address.LastIndexOf(',') + 1);
            if (stateZipCode == "") { stateZipCode = address; }

            string[] postal_zipcode = Regex.Split(stateZipCode.Trim(), @"\s+");
            if (postal_zipcode.Length == 2)
            {
                if (Regex.IsMatch(postal_zipcode[0], "^[a-zA-Z][a-zA-Z]$"))
                {
                    stateCode = postal_zipcode[0].Trim();
                }
                if (Regex.IsMatch(postal_zipcode[1], "^[0-9]"))
                {
                    zipCode = postal_zipcode[1].Trim();
                }
            }
            else
            {
                if (Regex.IsMatch(postal_zipcode[0], "^[a-zA-Z][a-zA-Z]$"))
                {
                    stateCode = postal_zipcode[0].Trim();
                }
                if (Regex.IsMatch(postal_zipcode[0], "^[0-9]"))
                {
                    zipCode = postal_zipcode[0].Trim();
                }
            }
            lstStateZipCode.Add("StateCode", stateCode);
            lstStateZipCode.Add("ZipCode", zipCode);
            return lstStateZipCode;
        }

        protected static void SeparateAddress(GenericDatabase database, string FullAddress, string Country, ref string address, ref string City, ref string State, ref string Zip)
        {
            string ss = "";
           // string st = "";
            string s1 = "";
            string lookUpZip = "";
            string lookUpState = "";
            string lookUpCity = "";

            FullAddress = Text.Trim(FullAddress);
            if (Text.Pos(", ", FullAddress) != 0)
            {
                Text.LeftSplitString(FullAddress, ", ", ref address, ref ss);
            }
            else
            {
                ss = FullAddress;
            }

            if (IsZipCode(GetLastWord(ss)) || (GetPostalCode(ss) != ""))
            {
                if (Country == "USA")
                {
                    s1 = Text.TextBeforeLastTag(ss, " ");
                    lookUpZip = Text.TextBeforeTag(GetLastWord(ss), "-");
                }
                else if (Country == "Canada")
                {
                    s1 = Text.TextBeforeLastTag(ss, GetPostalCode(ss));
                    lookUpZip = GetPostalCode(ss);
                }

                if ((Text.GetLastChar(s1) == ",") || ((GetLastChar(s1) == ".") && (Length(GetLastWord(Text.Trim(RemoveLastChar(s1)))) == 2)))
                {
                    s1 = Text.Trim(RemoveLastChar(s1));
                }

                if ((Pos(",", s1) != 0) && (GetStateAbbr(Text.TextAfterLastTag(s1, ",")) != ""))
                {
                    s1 = Text.TextBeforeLastTag(s1, ",") + ", " + GetStateAbbr(Text.TextAfterLastTag(s1, ","));
                }
                else if (((Pos(" ", s1) == 0) && (Length(s1) != 2)))
                {
                    s1 = GetStateAbbr(s1);
                }

                if ((Length(GetLastWord(s1)) == 2) && (IsUpperCase(GetLastWord(s1))))
                {
                    lookUpState = GetLastWord(s1);
                    s1 = Text.TextBeforeLastTag(s1, " ");
                    if (GetLastChar(s1) == ",")
                    {
                        s1 = Text.Trim(RemoveLastChar(s1));
                    }

                    if (Pos(",", s1) > 0)
                    {
                        address = address + ", " + Text.TextBeforeLastTag(s1, ",");
                        ss = Text.TextAfterLastTag(ss, Text.TextBeforeLastTag(s1, ","));
                        if (Text.GetFirstChar(ss) == ",")
                            ss = Text.Trim(Text.RemoveFirstChar(ss));
                    }
                    else
                    {
                        string query = "select * from directory.dbo.CityZipCountyCountry where State = '{0}' and Zip = '{1}'";
                        query = string.Format(query, lookUpState, lookUpZip);
                        DataTable cityZips = database.GetDataTable(query);
                        lookUpCity = "";

                        foreach (DataRow city in cityZips.Rows)
                        {
                            if (address == "")
                            {
                                address = s1;
                            }
                            lookUpCity = city["City"].ToString();
                            if ((address.ToLower()).EndsWith(Text.ToLower(lookUpCity)))
                            {
                                ss = lookUpCity + ", " + lookUpState + " " + lookUpZip;
                                address = Text.TextBeforeLastTag(address, lookUpCity);
                                break;
                            }
                        }
                    }
                }

                if (Country == "USA")
                {
                    GetCityStateZip(ss, ref City, ref State, ref Zip);
                }
                else if (Country == "Canada")
                {
                    GetCityStatePostal(ss, ref City, ref State, ref Zip);
                }
                else
                {
                    address = "";
                }
            }
        }

        protected static void GetCityStateZip(string InputStr, ref string City, ref string State, ref string Zip)
        {
            if (TagNumberInText(InputStr, ",") == 2)
            {
                City = GetDelimitedText(InputStr, ",", 0);
                State = GetDelimitedText(InputStr, ",", 1);
                Zip = GetDelimitedText(InputStr, ",", 2);
            }
            else if ((Pos(",", InputStr) == 0) && (TagNumberInText(InputStr, " ") >= 2))
            {
                Zip = Text.TextAfterLastTag(InputStr, " ");
                InputStr = Text.TextBeforeLastTag(InputStr, " ");
                State = Text.TextAfterLastTag(InputStr, " ");
                City = Text.TextBeforeLastTag(InputStr, " ");
            }
            else
            {
                if (Pos(" ", Text.TextAfterLastTag(InputStr, ",")) != 0)
                {
                    Zip = Text.TextAfterLastTag(InputStr, " ");
                    InputStr = Text.TextBeforeLastTag(InputStr, " ");
                    State = Text.TextAfterLastTag(InputStr, ",");
                    City = Text.TextBeforeLastTag(InputStr, ",");
                    if (Text.TextInString(State, " ") && IsNumber(Text.TextAfterTag(State, " ")))
                    {
                        Zip = Text.TextAfterTag(State, " ") + " " + Zip;
                        State = Text.TextBeforeTag(State, " ");
                    }
                }
                else
                {
                    Zip = Text.TextAfterLastTag(InputStr, ",");
                    InputStr = Text.TextBeforeLastTag(InputStr, ",");
                    State = Text.TextAfterLastTag(InputStr, " ");
                    City = Text.TextBeforeLastTag(InputStr, " ");
                }
            }
        }

        protected static void GetCityStatePostal(string InputStr, ref string City, ref string State, ref string Postal)
        {
            if (Pos(",", InputStr) == 0)
            {
                InputStr = Text.Trim(InputStr);
                if (GetPostalCode(InputStr) != "")
                {
                    Postal = GetPostalCode(InputStr);
                    InputStr = Text.TextBeforeTag(InputStr, Postal);
                    if (Length(GetLastWord(InputStr)) == 2)
                    {
                        RightSplitString(InputStr, " ", ref City, ref State);
                    }
                }
                else
                {
                    if ((Length(InputStr) == 2) && (LastChar(InputStr) < 'a'))
                    {
                        State = InputStr;
                    }
                    else
                    {
                        City = InputStr;
                    }
                }
            }
            else
            {
                if (GetPostalCode(InputStr) != "")
                {
                    Postal = GetPostalCode(InputStr);
                    InputStr = Text.TextBeforeTag(InputStr, Postal);
                }

                if (GetLastChar(InputStr) == ",")
                {
                    InputStr = RemoveLastChar(InputStr);
                }

                if (Pos(",", InputStr) != 0)
                {
                    RightSplitString(InputStr, ",", ref City, ref State);
                }
                else
                {
                    RightSplitString(InputStr, " ", ref City, ref State);
                }
            }
        }

        protected static void RightSplitString(string s, string SepTag, ref string LeftString, ref string RightString)
        {
            if (Text.TextInString(s, SepTag))
            {
                LeftString = Text.TextBeforeLastTag(s, SepTag);
            }
            else
            {
                LeftString = s;
            }

            RightString = Text.TextAfterLastTag(s, SepTag);
        }

        protected static void LeftSplitString(string s, string SepTag, ref string LeftString, ref string RightString)
        {
            if (Text.TextInString(s, SepTag))
            {
                LeftString = Text.TextBeforeTag(s, SepTag);
            }
            else
            {
                LeftString = s;
            }

            RightString = Text.TextAfterTag(s, SepTag);
        }

        protected static void SplitString(string s, string SepTag, ref string LeftString, ref string RightString)
        {
            if (Text.TextInString(s, SepTag))
            {
                LeftString = Text.TextBeforeLastTag(s, SepTag);
            }
            else
            {
                LeftString = s;
            }

            RightString = Text.TextAfterLastTag(s, SepTag);
        }

        public static string ToLower(string input)
        {
            return input.ToLower();
        }

        public static string GetFirstChar(string input)
        {
            if (input.Length > 0)
            {
                return input[0].ToString();
            }
            else
            {
                return "";
            }
        }

        public static string RemoveFirstChar(string input)
        {
            if (input.Length > 1)
            {
                return input.Substring(1, input.Length - 1);
            }
            else
            {
                return "";
            }
        }

        public static string GetLastChar(string input)
        {
            return LastChar(input).ToString();
        }

        public static int TagNumberInText(string input, string tag)
        {
            return CountStringOccurrences(input, tag);
        }

        public static string Copy(string input, int startIndex, int length)
        {
            startIndex = startIndex - 1;
            if (startIndex < 0)
            {
                startIndex = 0;
            }
            if (length > (input.Length - startIndex))
            {
                length = input.Length - startIndex;
            }
            string result = "";

            result = input.Substring(startIndex, length);
            return result;
        }

        public static string RemoveLastChar(string input)
        {
            if (input.Length > 0)
            {
                return input.Substring(0, input.Length - 1);
            }
            else
            {
                return "";
            }
        }

        public static string GetSecondLastWord(string input)
        {
            input = TextBeforeLastTag(input, " ");
            return GetLastWord(input);
        }

        public static string ReplaceStateWithAbbr(string address)
        {
            string left = "";
            string right = "";
            string state = "";

            if (Text.IsZipCode(GetLastWord(address)))
            {
                RightSplitString(address, " ", ref left, ref right);
                right = " " + right;
            }
            else if (Text.GetPostalCode(address) != "")
            {
                left = Text.TextBeforeLastTag(address, GetPostalCode(address));
                right = " " + GetPostalCode(address);
            }
            else
            {
                left = address;
            }

            if (Text.GetLastChar(left) == ",")
            {
                left = Text.Trim(RemoveLastChar(left));
                right = "," + right;
            }

            state = GetStateAbbr(GetLastWord(left));
            if (state != "")
            {
                return Text.TextBeforeLastTag(left, " ") + " " + state + right;
            }
            else
            {
                state = GetStateAbbr(GetSecondLastWord(left) + " " + GetLastWord(left));

                if (state != "")
                {
                    return Text.TextBeforeTag(left, " ", Text.TagNumberInText(left, " ") - 1) + " " + state + right;
                }
                else
                {
                    return address;
                }
            }
        }

        public static string GetStateAbbr(string state)
        {
            Dictionary<string, string> stateStateAbbrDict = new Dictionary<string, string>();
            stateStateAbbrDict.Add("alabama", "AL");
            stateStateAbbrDict.Add("alaska", "AK");
            stateStateAbbrDict.Add("arizona", "AZ");
            stateStateAbbrDict.Add("arkansas", "AR");
            stateStateAbbrDict.Add("california", "CA");
            stateStateAbbrDict.Add("colorado", "CO");
            stateStateAbbrDict.Add("connecticut", "CT");
            stateStateAbbrDict.Add("district of columbia", "DC");
            stateStateAbbrDict.Add("delaware", "DE");
            stateStateAbbrDict.Add("florida", "FL");
            stateStateAbbrDict.Add("georgia", "GA");
            stateStateAbbrDict.Add("hawaii", "HI");
            stateStateAbbrDict.Add("idaho", "ID");
            stateStateAbbrDict.Add("illinois", "IL");
            stateStateAbbrDict.Add("indiana", "IN");
            stateStateAbbrDict.Add("iowa", "IA");
            stateStateAbbrDict.Add("kansas", "KS");
            stateStateAbbrDict.Add("Kentucky", "KY");
            stateStateAbbrDict.Add("louisiana", "LA");
            stateStateAbbrDict.Add("maine", "ME");
            stateStateAbbrDict.Add("maryland", "MD");
            stateStateAbbrDict.Add("massachusetts", "MA");
            stateStateAbbrDict.Add("michigan", "MI");
            stateStateAbbrDict.Add("minnesota", "MN");
            stateStateAbbrDict.Add("mississippi", "MS");
            stateStateAbbrDict.Add("missouri", "MO");
            stateStateAbbrDict.Add("montana", "MT");
            stateStateAbbrDict.Add("nebraska", "NE");
            stateStateAbbrDict.Add("nevada", "NV");
            stateStateAbbrDict.Add("new hampshire", "'),");
            stateStateAbbrDict.Add("new jersey", "NJ");
            stateStateAbbrDict.Add("new mexico", "NM");
            stateStateAbbrDict.Add("new york", "NY");
            stateStateAbbrDict.Add("north carolina", "NC");
            stateStateAbbrDict.Add("north dakota", "ND");
            stateStateAbbrDict.Add("ohio", "OH");
            stateStateAbbrDict.Add("oklahoma", "OK");
            stateStateAbbrDict.Add("oregon", "OR");
            stateStateAbbrDict.Add("pennsylvania", "PA");
            stateStateAbbrDict.Add("rhode island", "RI");
            stateStateAbbrDict.Add("south carolina", "SC");
            stateStateAbbrDict.Add("south dakota", "SD");
            stateStateAbbrDict.Add("tennessee", "TN");
            stateStateAbbrDict.Add("texas", "TX");
            stateStateAbbrDict.Add("itah", "UT");
            stateStateAbbrDict.Add("vermont", "VT");
            stateStateAbbrDict.Add("virginia", "VA");
            stateStateAbbrDict.Add("washington", "WA");
            stateStateAbbrDict.Add("west virginia", "WV");
            stateStateAbbrDict.Add("wisconsin", "WI");
            stateStateAbbrDict.Add("wyoming", "WY");

            if (stateStateAbbrDict.ContainsKey(state.ToLower()))
            {
                string value = stateStateAbbrDict[state.ToLower()];
                return value;
            }

            if (stateStateAbbrDict.ContainsValue(state.ToUpper()))
            {
                string value = state.ToUpper();
                return value;
            }

            return "";
        }

        public static string GetPostalCode(string s)
        {
            return GetPostalCode(s, true);
        }

        public static string GetPostalCode(string s, bool endsWith)
        {
            const string letter = "[ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz]";
            if (endsWith)
            {
                return GetFirstRegexMatch(s, letter + "\\d" + letter + "\\s?\\d" + letter + "(\\d|" + letter + ")$", false);
            }
            else
            {
                return GetLastRegexMatch(s, letter + "\\d" + letter + "\\s?\\d" + letter + "(\\d|" + letter + ")", false);
            }
        }

        public static string GetStringFromTextWithRegEx(string input, string regEx, bool caseSensitive, bool getLastMatch)
        {
            if (getLastMatch)
            {
                return GetFirstRegexMatch(input, regEx, !caseSensitive);
            }
            else
            {
                return GetLastRegexMatch(input, regEx, !caseSensitive);
            }
        }

        public static string GetLastWord(string s)
        {
            if (!s.Contains(" ")) return s;

            string lastWord = TextAfterLastTag(s, " ");
            return lastWord;
        }

        /// <summary>
        /// Finds the index of the last instance of delimiter within the string input.
        /// </summary>
        public static int LastDelimiter(string delimiter, string input)
        {
            int index = input.LastIndexOf(delimiter) + 1;
            return index;
        }

        public static int Length(string input)
        {
            return input.Length;
        }

        public static int Pos(string subString, string input)
        {
            int index = input.IndexOf(subString) + 1;
            return index;
        }

        public static string UnicodeString(string s)
        {
            s = Text.ReplaceTag1WithTag2(s, " ", "%20");
            s = Text.ReplaceTag1WithTag2(s, "!", "%21");
            s = Text.ReplaceTag1WithTag2(s, "\"", "%22");
            s = Text.ReplaceTag1WithTag2(s, "#", "%23");
            s = Text.ReplaceTag1WithTag2(s, "$", "%24");
            s = Text.ReplaceTag1WithTag2(s, "%", "%25");
            s = Text.ReplaceTag1WithTag2(s, "&", "%26");
            s = Text.ReplaceTag1WithTag2(s, "'", "%27");
            s = Text.ReplaceTag1WithTag2(s, "(", "%28");
            s = Text.ReplaceTag1WithTag2(s, ")", "%29");
            s = Text.ReplaceTag1WithTag2(s, "*", "%2A");
            s = Text.ReplaceTag1WithTag2(s, "+", "%2B");
            s = Text.ReplaceTag1WithTag2(s, ",", "%2C");
            s = Text.ReplaceTag1WithTag2(s, "-", "%2D");
            s = Text.ReplaceTag1WithTag2(s, ".", "%2E");
            s = Text.ReplaceTag1WithTag2(s, "/", "%2F");
            s = Text.ReplaceTag1WithTag2(s, ":", "%3A");
            s = Text.ReplaceTag1WithTag2(s, ";", "%3B");
            s = Text.ReplaceTag1WithTag2(s, "<", "%3C");
            s = Text.ReplaceTag1WithTag2(s, "=", "%3D");
            s = Text.ReplaceTag1WithTag2(s, ">", "%3E");
            s = Text.ReplaceTag1WithTag2(s, "?", "%3F");
            s = Text.ReplaceTag1WithTag2(s, "@", "%40");
            s = Text.ReplaceTag1WithTag2(s, "_", "%5F");
            s = Text.ReplaceTag1WithTag2(s, "__", "%2f");
            s = Text.ReplaceTag1WithTag2(s, "®", "%AE");

            return s;
        }

        public static bool IsNaturalNumber(string input)
        {
            int number;
            if (int.TryParse(input, out number) && number > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static string GetFirstLink(string s)
        {
            if (!s.Contains("href=\"")) return "";
            string link = TextBetweenTags(s, "href=\"", "\"");
            return link;
        }

        public static bool HasLowerCaseCharacter(string input)
        {
            // 97 - 122
            for (int i = 0; i < input.Length; ++i)
            {
                if ((input[i] >= 97) && (input[i] <= 122))
                {
                    return true;
                }
            }

            return false;
        }

        public static string CleanAddress(string s)
        {
            s = Text.RestoreHTMLSymbols(s);
            s = Text.ReplaceTag1WithTag2(s, "<BR>", ", ");
            s = Text.ReplaceTag1WithTag2(s, "\r\n", ", ");
            s = Text.ReplaceTag1WithTag2(s, "<LI>", ", ");
            s = Text.ReplaceTag1WithTag2(s, "</TR>", ", ");
            s = Text.ReplaceTag1WithTag2(s, "</P>", ", ");
            s = Text.Trim(Text.RemoveHTMLTags(s));
            s = Text.TextAfterTag(s, ">");
            s = Text.ReplaceTag1WithTag2(s, "  ", " ");
            s = Text.ReplaceTag1WithTag2(s, " ,", ",");
            s = Text.ReplaceTag1WithTag2(s, ", ,", ",");
            s = Text.ReplaceTag1WithTag2(s, ",,", ",");
            s = Text.ReplaceTag1WithTag2(s, " ,", ",");
            s = Text.ReplaceTag1WithTag2(s, ",", ", ");
            s = Text.ReplaceTag1WithTag2(s, "  ", " ");
            s = Text.ReplaceTag1WithTag2(s, ",,", ",");

            s = Text.ReplaceTag1WithTag2(s, "  ", " ");
            s = Text.ReplaceTag1WithTag2(s, " ,", ",");
            s = Text.ReplaceTag1WithTag2(s, ", ,", ",");
            s = Text.ReplaceTag1WithTag2(s, ",,", ",");
            s = Text.ReplaceTag1WithTag2(s, " ,", ",");
            s = Text.ReplaceTag1WithTag2(s, ",", ", ");
            s = Text.ReplaceTag1WithTag2(s, "  ", " ");
            s = Text.ReplaceTag1WithTag2(s, ",,", ",");
            s = Text.ReplaceTag1WithTag2(s, ", , ", ", ");

            s = s.Trim();

            if (s.StartsWith(","))
            {
                s = TextAfterTag(s, ",");
            }
            if (s.EndsWith(","))
            {
                s = TextBeforeLastTag(s, ",");
            }
            if (s.StartsWith(","))
            {
                s = TextAfterTag(s, ",");
            }
            if (s.EndsWith(","))
            {
                s = TextBeforeLastTag(s, ",");
            }
            if (s.StartsWith(","))
            {
                s = TextAfterTag(s, ",");
            }
            if (s.EndsWith(","))
            {
                s = TextBeforeLastTag(s, ",");
            }
            if (s.StartsWith(","))
            {
                s = TextAfterTag(s, ",");
            }
            if (s.EndsWith(","))
            {
                s = TextBeforeLastTag(s, ",");
            }

            return Text.Trim(s);
        }

        public static string NextChar(string s)
        {
            if (s == "") return "";

            if (Text.LastChar(s) != 'z')
            {
                char lastChar = Convert.ToChar(Text.LastChar(s));
                char lastPlusOne = (char)(((int)lastChar) + 1);
                s = Text.TextBeforeLastTag(s, lastChar.ToString()) + lastPlusOne;
            }
            else
            {
                s = TextBeforeLastChar(s);
                while (s.Length > 0)
                {
                    if (LastChar(s) == 'z')
                    {
                        s = Text.TextBeforeLastChar(s);
                    }
                    else
                    {
                        char lastChar = Convert.ToChar(Text.LastChar(s));
                        char lastPlusOne = (char)(((int)lastChar) + 1);
                        s = Text.TextBeforeLastTag(s, lastChar.ToString()) + lastPlusOne;
                        break;
                    }
                }
            }

            return s;
        }

        public static string TextBeforeLastChar(string input)
        {
            if (input == "") return "";
            string output = "";
            char lastChar = Text.LastChar(input);

            output = Text.TextBeforeLastTag(input, lastChar.ToString());

            return output;
        }

        public static char LastChar(string input)
        {
            if (input.Length == 0) return '\0';

            return input[input.Length - 1];
        }

        public static string EraseScripts(string input)
        {
            string firstHalf = "";
            string secondHalf = "";

            while (input.Contains("<SCRIPT") && input.Contains("</SCRIPT>"))
            {
                firstHalf = TextBeforeTag(input, "<SCRIPT");
                secondHalf = TextAfterTag(input, "</SCRIPT>");
                input = firstHalf + "\r\n" + secondHalf;
            }

            while (input.Contains("<script") && input.Contains("</script>"))
            {
                firstHalf = TextBeforeTag(input, "<script");
                secondHalf = TextAfterTag(input, "</script>");
                input = firstHalf + "\r\n" + secondHalf;
            }

            return input;
        }

        public static string RemoveNonNumericCharacters(string input)
        {
            string numericString = Regex.Replace(input, "[^.0-9]", "");
            return numericString;
        }

        public static int CountStringOccurrences(string text, string pattern)
        {
            // Loop through all instances of the string 'text'.
            int count = 0;
            int i = 0;
            while ((i = text.IndexOf(pattern, i)) != -1)
            {
                i += pattern.Length;
                count++;
            }
            return count;
        }

        public static string UpperCase(string input)
        {
            return input.ToUpper();
        }

        public static string LowerCase(string input)
        {
            return input.ToLower();
        }

        public static string EraseTag(string input, string tag)
        {
            return input.Replace(tag, "");
        }

        public static string RestoreHTMLSymbols(string s)
        {
            if (s == "") return s;
            s = s.Replace("Â", "");
            s = Text.ReplaceTag1WithTag2(s, "â‚¬", "€");
            s = Text.ReplaceTag1WithTag2(s, "â€“", "–");
            s = Text.ReplaceTag1WithTag2(s, "â„¢", "™");
            s = Text.ReplaceTag1WithTag2(s, "&#16484;", " ");
            s = Text.ReplaceTag1WithTag2(s, "&#16500;", " ");
            s = Text.ReplaceTag1WithTag2(s, "&#9642;", "•");
            s = Text.ReplaceTag1WithTag2(s, "&#8211;", "–");
            s = Text.ReplaceTag1WithTag2(s, "&#8212;", "—");
            s = Text.ReplaceTag1WithTag2(s, "&#8216;", "'");
            s = Text.ReplaceTag1WithTag2(s, "&#8217;", "'");
            s = Text.ReplaceTag1WithTag2(s, "&#8220;", "\"");
            s = Text.ReplaceTag1WithTag2(s, "&#8221;", "\"");
            s = Text.ReplaceTag1WithTag2(s, "&#8224;", "†");
            s = Text.ReplaceTag1WithTag2(s, "&#8226;", "•");
            s = Text.ReplaceTag1WithTag2(s, "&#8230;", "…");
            s = Text.ReplaceTag1WithTag2(s, "&#8260;", "/");
            s = Text.ReplaceTag1WithTag2(s, "&#8482;", "™");
            s = Text.ReplaceTag1WithTag2(s, "&#338;", "Œ");

            for (int i = 1; i <= 254; ++i)
            {
                s = Text.ReplaceTag1WithTag2(s, "&#" + i.ToString() + ";", ((char)i).ToString());
                s = Text.ReplaceTag1WithTag2(s, "&#0" + i.ToString() + ";", ((char)i).ToString());
            }

            s = Text.ReplaceTag1WithTag2(s, "&trade;", "™");
            s = Text.ReplaceTag1WithTag2(s, "&copy;", "©");
            s = Text.ReplaceTag1WithTag2(s, "&reg;", "®");
            s = Text.ReplaceTag1WithTag2(s, "&reg", "®");
            s = Text.ReplaceTag1WithTag2(s, "&deg;", "°");
            s = Text.ReplaceTag1WithTag2(s, "&amp;", "&");
            s = Text.ReplaceTag1WithTag2(s, "&nbsp;", " ");
            s = Text.ReplaceTag1WithTag2(s, "&quot;", "'");
            s = Text.ReplaceTag1WithTag2(s, "&agrave;", "à");
            s = Text.ReplaceTag1WithTag2(s, "&egrave;", "è");
            s = Text.ReplaceTag1WithTag2(s, "&ograve;", "ò");
            s = Text.ReplaceTag1WithTag2(s, "&eacute;", "é");
            s = Text.ReplaceTag1WithTag2(s, "&ouml;", "ö");
            s = Text.ReplaceTag1WithTag2(s, "&ocirc;", "ô");
            s = Text.ReplaceTag1WithTag2(s, "&euml;", "ë");
            s = Text.ReplaceTag1WithTag2(s, "&auml;", "ä");
            s = Text.ReplaceTag1WithTag2(s, "&iuml;", "ï");
            s = Text.ReplaceTag1WithTag2(s, "&uuml;", "ü");
            s = Text.ReplaceTag1WithTag2(s, "&szlig;", "ß");
            s = Text.ReplaceTag1WithTag2(s, "&oslash;", "ø");
            s = Text.ReplaceTag1WithTag2(s, "&aelig;", "æ");
            s = Text.ReplaceTag1WithTag2(s, "&aring;", "å");
            s = Text.ReplaceTag1WithTag2(s, "&ecirc;", "ê");
            s = Text.ReplaceTag1WithTag2(s, "&ntilde;", "ñ");
            s = Text.ReplaceTag1WithTag2(s, "&frac12;", "½");
            s = Text.ReplaceTag1WithTag2(s, "&frac34;", "¾");
            s = Text.ReplaceTag1WithTag2(s, "&gt;", ">");
            s = Text.ReplaceTag1WithTag2(s, "&lt;", "<");
            s = Text.ReplaceTag1WithTag2(s, "&hellip;", "…");
            s = Text.ReplaceTag1WithTag2(s, "&raquo;", "»");
            s = Text.ReplaceTag1WithTag2(s, "&laquo;", "«");
            s = Text.ReplaceTag1WithTag2(s, "&mdash;", "—");
            s = Text.ReplaceTag1WithTag2(s, "&ndash;", "–");
            s = Text.ReplaceTag1WithTag2(s, "&ldquo;", "“");
            s = Text.ReplaceTag1WithTag2(s, "&rdquo;", "”");
            s = Text.ReplaceTag1WithTag2(s, "&lsquo;", "‘");
            s = Text.ReplaceTag1WithTag2(s, "&rsquo;", "’");
            s = Text.ReplaceTag1WithTag2(s, "&acute;", "´");
            s = Text.ReplaceTag1WithTag2(s, "&middot;", "·");
            s = Text.ReplaceTag1WithTag2(s, "&bull;", "•");
            s = Text.ReplaceTag1WithTag2(s, "&sect;", "§");
            s = Text.ReplaceTag1WithTag2(s, "&euro;", "€");
            s = Text.ReplaceTag1WithTag2(s, "&euro", "€");
            s = Text.ReplaceTag1WithTag2(s, "&pound;", "£");
            s = Text.ReplaceTag1WithTag2(s, "&pound", "£");
            return s;
        }

        public static string GetURLParamValue(string url, string parameterName)
        {
            if (!url.Contains(parameterName)) return "";

            string value = Text.TextAfterTag(url, "?") + "&";
            value = Text.TextBetweenTags(value, parameterName + "=", "&");

            return value;
        }

        public static string TextBeforeTag1UntilTag2(string input, string tag1, string tag2)
        {
            if (!(input.Contains(tag1) && input.Contains(tag2))) return "";

            string before = Text.TextBeforeTag(input, tag1);
            if (!before.Contains(tag2)) return "";
            before = Text.TextAfterLastTag(before, tag2);

            return before;
        }

        public static string DelphiToCSharp(string input)
        {
            string output = input;

            /*  string vars = Text.TextBetweenTags(input, "var", "begin").Trim();
              string newVars = "";
              foreach (string varline in vars.Split("\r\n".ToCharArray()))
              {
                  if (varline == "") continue;

                  string type = Text.TextAfterLastTag(varline, ":").Trim();
                  type = type.Replace(";", "");
                  if (type == ("integer"))
                  {
                      type = "int";
                  }
                  string varNames = Text.TextBeforeLastTag(varline, ":").Trim();

                  foreach (string varName in varNames.Split(",".ToCharArray()))
                  {
                      if (varName == "") continue;
                      string thisVarName = varName.Trim();
                      newVars += "\r\n  " + type + " " + thisVarName;
                      if (type.Contains("string"))
                      {
                          newVars += " = '';";
                      }
                      else if (type.Contains("int"))
                      {
                          newVars += " = 0;";
                      }
                      else
                      {
                          newVars += " = new " + type + "();";
                      }
                      newVars = newVars.Trim();
                  }
              }
            
              if (newVars != "")
              {
                  output = Text.TextAfterTag(output, "var");
                  output = Text.TextAfterTag(output, "begin");
                  output = "{\r\n" + newVars + "\r\n\r\n  " + output.Trim();
              } */

            // database objects
            output = output.Replace("_bidRecord", "_bidDR");
            output = output.Replace("_documentRecord", "_bidDocumentDR");

            // language constructs
            output = output.Replace("begin", "{");
            output = output.Replace("end;\r\n", "}\r\n");
            output = output.Replace("end\r\n", "}\r\n");
            output = output.Replace(":=", "=");
            output = output.Replace("<>", "!=");

            // quotes
            output = output.Replace("\"", "\\\"");
            output = output.Replace("'", "\"");

            // loops and ifs
            output = output.Replace(" do\r\n", "");
            output = output.Replace(" then", "");
            output = output.Replace("exit;", "return;");
            //output = output.Replace("for ", "for (");           

            // line feeds
            output = output.Replace("#13#10#13#10#13#10", "\"\\r\\n\\r\\n\\r\\n\"");
            output = output.Replace("#13#10#13#10", "\"\\r\\n\\r\\n\"");
            output = output.Replace("#13#10", "\"\\r\\n\"");

            // specific functions
            output = output.Replace("EraseTextBetweenTags2", "|~ETBT2~|");
            output = output.Replace("GetTextStringBetweenTags", "Text.GetTextStringBetweenTags");
            output = output.Replace("ReplaceTag1WithTag2", "Text.ReplaceTag1WithTag2");
            output = output.Replace("TextBetweenTags", "Text.TextBetweenTags");
            output = output.Replace("Trim", "Text.Trim");
            output = output.Replace("TextAfterTag", "Text.TextAfterTag");
            output = output.Replace("TextBeforeTag", "Text.TextBeforeTag");
            output = output.Replace("TextAfterLastTag", "Text.TextAfterLastTag");
            output = output.Replace("GetURLParamValue(", "Text.GetURLParamValue(");
            output = output.Replace("TextBeforeLastTag", "Text.TextBeforeLastTag");
            output = output.Replace("TextBeforeTag1UnitlTag2", "Text.TextBeforeTag1UnitlTag2");
            output = output.Replace("ChangeAmps", "Text.ChangeAmps");
            output = output.Replace("RemoveHTMLTags", "Text.RemoveHTMLTags");
            output = output.Replace("GetFirstLink(", "Text.GetFirstLink(");
            output = output.Replace("GetDelimitedTextString", "Text.GetDelimitedTextString");
            output = output.Replace("resultHTML", "html");
            output = output.Replace("TextInString", "Text.TextInString");
            output = output.Replace(" , SiteName);", " , _siteName);");
            output = output.Replace("_bidDR.SetValue(\"SourceURL\", HomeURL);\r\n", "");
            output = output.Replace("_scanStatus.GetScanNumber);", "_scanStatus.ScanID);");
            output = output.Replace("RestoreHTMLSymbols", "Text.RestoreHTMLSymbols");
            output = output.Replace("EraseTag", "Text.EraseTag");
            output = output.Replace("|~ETBT2~|", "Text.EraseTextBetweenTags2");
            output = output.Replace("_browser1.LocationURL", "_primaryBrowser.LocationUrl");
            output = output.Replace("_browser2.LocationURL", "_secondaryBrowser.LocationUrl");
            output = output.Replace("_browser1", "_primaryBrowser");
            output = output.Replace("_browser2", "_secondaryBrowser");
            output = output.Replace("UpperCase", "Text.ToUpper");
            output = output.Replace("LowerCase", "Text.ToLower");
            output = output.Replace("SetValue(\"SiteName\", SiteName);", "SetValue(\"SiteName\", _siteName);");
            output = output.Replace("Application.ProcessMessages();", "Application.DoEvents();");
            output = Text.EraseTextBetweenTags2(output, "FreeAndNil(", ");\r\n", true, true);
            return output;
        }

        public static string EraseTextBetweenTags2(string s, string Tag1, string Tag2, bool eraseFirstTag, bool eraseSecondTag)
        {
            int firstTagPos = 0;
            int secondTagPos = 0;
            string firstPart = "";
            string secondPart = "";
            string result = "";

            firstTagPos = s.ToLower().IndexOf(Tag1.ToLower());
            secondTagPos = s.ToLower().IndexOf(Tag2.ToLower());

            if ((firstTagPos < 0) || (secondTagPos < 0))
            {
                return s;
            }

            firstPart = Text.TextBeforeTag(s, Tag1);
            secondPart = Text.TextAfterTag(s, Tag1);
            secondPart = Text.TextAfterTag(secondPart, Tag2);

            if (eraseFirstTag && eraseSecondTag)
            {
                result = firstPart + secondPart;
            }
            else if (eraseFirstTag && !eraseSecondTag)
            {
                result = firstPart + Tag2 + secondPart;
            }
            else if (!eraseFirstTag && eraseSecondTag)
            {
                result = firstPart + Tag1 + secondPart;
            }
            else if (!eraseFirstTag && !eraseSecondTag)
            {
                result = firstPart + Tag1 + Tag2 + secondPart;
            }

            return result;
        }

        public static string ReplaceTag1WithTag2(string input, string tag1, string tag2)
        {
            return input.Replace(tag1, tag2);
        }

        public static string Trim(string input)
        {
            return input.Trim();
        }

        public static string ChangeAmps(string input)
        {
            return input.Replace("&amp;", "&");
        }

        public static string GetFirstRegexMatch(string input, string regex, bool ignoreCase)
        {
            // Here we call Regex.Match.
            Match match = null;
            if (ignoreCase)
            {
                match = Regex.Match(input, regex, RegexOptions.IgnoreCase);
            }
            else
            {
                match = Regex.Match(input, regex, RegexOptions.None);
            }

            // Here we check the Match instance.
            if (match.Success)
            {
                // Finally, we get the Group value and display it.
                string key = match.Groups[0].Value;
                return key;
            }
            else
            {
                return "";
            }
        }

        public static string GetLastRegexMatch(string input, string regex, bool ignoreCase)
        {
            // Here we call Regex.Match.
            Match match = null;
            if (ignoreCase)
            {
                match = Regex.Match(input, regex, RegexOptions.IgnoreCase);
            }
            else
            {
                match = Regex.Match(input, regex, RegexOptions.None);
            }

            // Here we check the Match instance.
            if (match.Success)
            {
                // Finally, we get the Group value and display it.

                string key = match.Groups[match.Groups.Count - 1].Value;
                return key;
            }
            else
            {
                return "";
            }
        }

        public static string Compress(string text)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(text);
            MemoryStream ms = new MemoryStream();
            using (GZipStream zip = new GZipStream(ms, CompressionMode.Compress, true))
            {
                zip.Write(buffer, 0, buffer.Length);
            }

            ms.Position = 0;
            MemoryStream outStream = new MemoryStream();

            byte[] compressed = new byte[ms.Length];
            ms.Read(compressed, 0, compressed.Length);

            byte[] gzBuffer = new byte[compressed.Length + 4];
            System.Buffer.BlockCopy(compressed, 0, gzBuffer, 4, compressed.Length);
            System.Buffer.BlockCopy(BitConverter.GetBytes(buffer.Length), 0, gzBuffer, 0, 4);
            return Convert.ToBase64String(gzBuffer);
        }

        public static string Decompress(string compressedText)
        {
            byte[] gzBuffer = Convert.FromBase64String(compressedText);
            using (MemoryStream ms = new MemoryStream())
            {
                int msgLength = BitConverter.ToInt32(gzBuffer, 0);
                ms.Write(gzBuffer, 4, gzBuffer.Length - 4);

                byte[] buffer = new byte[msgLength];

                ms.Position = 0;
                using (GZipStream zip = new GZipStream(ms, CompressionMode.Decompress))
                {
                    zip.Read(buffer, 0, buffer.Length);
                }

                return Encoding.UTF8.GetString(buffer);
            }
        }

        public static int CountWords(string input)
        {
            int c = 0;
            for (int i = 1; i < input.Length; i++)
            {
                if (char.IsWhiteSpace(input[i - 1]) == true)
                {
                    if (char.IsLetterOrDigit(input[i]) == true ||
                        char.IsPunctuation(input[i]))
                    {
                        c++;
                    }
                }
            }
            if (input.Length > 2)
            {
                c++;
            }
            return c;
        }

        public static ArrayList GetImageAltList(string inputHtml)
        {
            ArrayList tempList = new ArrayList();
            while (inputHtml.ToUpper().Contains("<IMG "))
            {
                inputHtml = GetSubstring(inputHtml, "<IMG ", null, true);
                string temp = GetSubstring(inputHtml, "alt=", ">");
                if (temp.ToLower().Contains(" src="))
                    temp = GetSubstring(temp, null, " src=", true);
                temp = RemoveStartEndQuotes(temp);
                if (temp != "")
                    tempList.Add(temp);
            }
            return tempList;
        }

        /// <summary>
        /// Legacy Delphi function name, makes use of GetSubstring.
        /// </summary>
        public static string TextAfterLastTag(string value, string tag)
        {
            return GetSubstring(value, tag, true, "", false, true).Trim();
        }

        /// <summary>
        /// Legacy Delphi function name, makes use of GetSubstring.
        /// </summary>
        public static string TextBeforeLastTag(string value, string tag)
        {
            return GetSubstring(value, "", false, tag, true, true).Trim();
        }

        /// <summary>
        /// Legacy Delphi function name, makes use of GetSubstring.
        /// </summary>
        public static string TextAfterTag(string value, string tag)
        {
            return GetSubstring(value, tag, "").Trim();
        }

        /// <summary>
        /// Legacy Delphi function name, makes use of GetSubstring.
        /// </summary>
        public static string TextBeforeTag(string value, string tag)
        {
            if (!value.Contains(tag)) return value;
            return GetSubstring(value, "", tag).Trim();
        }

        public static string TextBeforeTag(string value, string tag, int instance)
        {
            return GetSubstring(value, "", tag).Trim();
        }

        /// <summary>
        /// Legacy Delphi function name, makes use of GetSubstring.
        /// </summary>
        public static string TextBetweenTags(string value, string startTag, string endTag)
        {
            if (!(value.Contains(startTag) && value.Contains(endTag))) return "";
            return GetSubstring(value, startTag, endTag).Trim();
        }

        /// <summary>
        /// Legacy Delphi function name, makes use of GetSubstring.
        /// </summary>
        public static string GetTextStringBetweenTags(string value, string startTag, string endTag)
        {
            if (startTag == "" && endTag == "") return "";

            if (!(value.Contains(startTag) && value.Contains(endTag))) return "";

            string result = RemoveHTMLTags(GetSubstring(value, startTag, endTag));
            if (result.Contains(">"))
            {
                result = Text.TextAfterTag(result, ">");
            }
            if (result.Contains("<"))
            {
                result = Text.TextBeforeTag(result, "<");
            }

            result = result.Trim();

            return result;
        }

        /// <summary>
        /// Renamed string.Contains function.  Checks to see if the tag is in the input string.
        /// </summary>
        /// <returns></returns>
        public static bool TextInString(string input, string tag)
        {
            return input.Contains(tag);
        }

        /// <summary>
        /// Renamed string.Contains function.  Checks to see if the tag is in the input string.
        /// </summary>
        /// <returns></returns>
        public static bool TextInString(string input, string tag, bool ignoreCase)
        {
            if (ignoreCase)
            {
                return input.ToLower().Contains(tag.ToLower());
            }

            return input.Contains(tag);
        }

        /// <summary>
        /// Returns the text between the start tag and the end tag of the value string.
        /// All HTML tag content is removed after the substring is parsed.
        /// </summary>
        public static string SubstringNoTag(string value, string startTag, string endTag)
        {
            return RemoveHTMLTags(GetSubstring(value, startTag, endTag));
        }

        /// <summary>
        /// Removes all html tags and inner tag text from the input parameter
        /// value and returns the resulting string.
        /// </summary>
        public static string RemoveHTMLTags(string value)
        {
            int i = 0;
            string newValue = "";

            while (i < value.Length)
            {
                if (value[i] == '<')
                {
                    while ((i < value.Length) && value[i] != '>')
                    {
                        ++i;
                    }
                    ++i;
                }
                else
                {
                    newValue = newValue + value[i];
                    ++i;
                }

            }
            return newValue;
        }

        public static string GetImageAltList(string inputHtml, string delimitor)
        {
            return ArrayListToString(GetImageAltList(inputHtml), delimitor);
        }

        public static int TextMatchCount(string inputString, string matchText)
        {
            return Regex.Matches(inputString, matchText).Count;
        }

        #region Text Format Verification

        public static bool IsYear(string inputString)
        {
            return RegexPattern.Year.IsMatch(inputString);
        }

        public static bool IsChineseDate(string inputString)
        {
            return RegexPattern.ChineseDate.IsMatch(inputString);
        }

        public static bool IsWholeNumber(string inputString)
        {
            return !RegexPattern.NotWholeNumber.IsMatch(inputString);
        }

        public static bool IsInteger(string inputString)
        {
            return !RegexPattern.NotInteger.IsMatch(inputString) &&
                RegexPattern.Integer.IsMatch(inputString);
        }

        public static bool IsPositiveNumber(string inputString)
        {
            return !RegexPattern.NotPositiveNumber.IsMatch(inputString) &&
                   RegexPattern.PositiveNumber.IsMatch(inputString) &&
                   !RegexPattern.NumberWithTwoDots.IsMatch(inputString);
        }

        public static bool IsNumber(string inputString)
        {
            return !RegexPattern.NotNumber.IsMatch(inputString) &&
                   !RegexPattern.NumberWithTwoDots.IsMatch(inputString) &&
                   !RegexPattern.NumberWithTwoMinus.IsMatch(inputString) &&
                   RegexPattern.Number.IsMatch(inputString);
        }

        public static bool IsAlpha(string inputString)
        {
            return !RegexPattern.Alpha.IsMatch(inputString);
        }

        public static bool IsAlphaNumeric(string inputString)
        {
            return !RegexPattern.AlphaNumeric.IsMatch(inputString);
        }

        public bool IsEmail(string inputString)
        {
            return RegexPattern.Email.IsMatch(inputString);
        }

        public static bool IsPhoneNumber(string inputString)
        {
            return RegexPattern.PhoneNumber.IsMatch(inputString);
        }

        public static bool IsZipCode(string inputString)
        {
            return RegexPattern.ZipCode.IsMatch(inputString);
        }

        public bool IsSSN(string inputString)
        {
            return RegexPattern.SSN.IsMatch(inputString);
        }

        public bool IsStringContainsOnlyWhiteSpace(string inputString)
        {
            return RegexPattern.WhiteSpaceString.IsMatch(inputString);
        }

        #endregion

        #region Extract Result From Web Pages

        public static string ConvertUTF8ToASCII(string inputString)
        {
            string charString;
            while (RegexPattern.UTF8CharInUrl.IsMatch(inputString))
            {
                charString = RegexPattern.UTF8CharInUrl.Match(inputString).ToString();
                char c = (char)Int32.Parse(charString.Replace("%", "").Trim(), NumberStyles.AllowHexSpecifier);
                inputString = inputString.Replace(charString, c.ToString());
            }
            return inputString;
        }

        public static string ConvertUTF8ToChinese(string inputString, OleDbConnection conn)
        {
            string UTF8Chars;
            string ChineseChar;
            while (RegexPattern.UTF8ChineseInUrl.IsMatch(inputString))
            {
                UTF8Chars = RegexPattern.UTF8ChineseInUrl.Match(inputString).ToString();
                ChineseChar = Database.GetFieldValue(conn, UnicodeChinese.TableName, UnicodeChinese.ChineseCharacter,
                    UnicodeChinese.UTF8, "=", UTF8Chars.Replace("%", " ").Trim());
                if (ChineseChar == "")
                    ChineseChar = "(" + UTF8Chars.Replace("%", " ").Trim() + ")";
                inputString = inputString.Replace(UTF8Chars, ChineseChar);
            }
            inputString = ConvertUTF8ToASCII(inputString);
            return inputString;
        }

        public string GetFirstPhoneNumber(string inputString, bool returnStandardPhoneNumberFormat)
        {
            string result = RegexPattern.PhoneNumber.Match(inputString).ToString();
            if ((result != "") && returnStandardPhoneNumberFormat)
                result = Regex.Replace(ExtractNumbers(result),
                    "(?<NPA>\\d{1,3})(?<NXX>\\d{1,3})(?<number>\\d{1,4})",
                    "(${NPA}) ${NXX}-${number}");
            return result;
        }

        public string GetFirstEmail(string inputString, bool toLowerCase)
        {
            string result = RegexPattern.Email.Match(inputString).ToString();
            if ((result != "") && toLowerCase)
                result = result.ToLower();
            return result;
        }

        public static void GetUrl(string inputString, ListBox list)
        {
            foreach (Match m in RegexPattern.Url.Matches(inputString))
            {
                if (!list.Items.Contains(m.Value))
                    list.Items.Add(m.Value);
            }
        }

        public static void GetProxy(string inputString, ListBox list)
        {
            string s = inputString;
            //input is web browser's innertext
            string proxy;
            string port;
            int i;
            foreach (Match m in RegexPattern.Proxy.Matches(s))
            {
                proxy = m.Value;
                i = s.IndexOf(proxy) + proxy.Length;
                if ((s[i].ToString() == ":") || (Regex.IsMatch(proxy, RegexPattern.IP.ToString())))
                {
                    if ((s[i].ToString() == ":") ||
                        (s[i].ToString() == " ") ||
                        (s[i].ToString() == "-") ||
                        (s[i].ToString() == "+"))
                        i++;
                    if ((i <= s.Length) && (IsInteger(s[i].ToString())))
                    {
                        port = "";
                        while ((i <= s.Length) && (IsInteger(s[i].ToString())))
                        {
                            port += s[i];
                            i++;
                        }
                        if (Convert.ToInt32(port) < 65536)
                        {
                            proxy += ":" + port;
                            if (!list.Items.Contains(proxy))
                                list.Items.Add(proxy);
                        }
                    }
                }
            }
        }

        #endregion

        public static string[,] CountryCode = {
            { "China", "86" }
        };

        public static string GetPhoneNumberCountryCode(string countryName)
        {
            return GetStringArrayValue(CountryCode, 0, 1, countryName, false);
        }

        public static string StandardizePhoneNumber(string phoneNumber, string countryName)
        {
            return StandardizePhoneNumber(phoneNumber, countryName, "-");
        }

        public static string StandardizePhoneNumber(string phoneNumber, string countryName, string delimitor)
        {
            if ((phoneNumber == null) || (phoneNumber.Trim().Length == 0))
                return "";
            string result = phoneNumber.Trim();
            while ((result.Length > 0) && (result.StartsWith("+")))
                result = result.Substring(1);
            while ((result.Length > 0) && (result.StartsWith("0")))
                result = result.Substring(1);
            if ((delimitor != null) && (delimitor.Length > 0))
            {
                if (result.Contains(" " + delimitor + " "))
                    result = result.Replace(" " + delimitor + " ", delimitor);
                result = result.Replace(" ", delimitor);
            }
            if ((countryName != null) && (countryName != ""))
            {
                string countryCode = GetPhoneNumberCountryCode(countryName);
                if ((countryCode.Length > 0) && (!result.StartsWith(countryCode)))
                    result = countryCode + delimitor + result;
                string realNumber = GetSubstring(result, countryCode + delimitor, null).Trim();
                while ((realNumber.Length > 0) && (realNumber.StartsWith("0")))
                    realNumber = realNumber.Substring(1);
                result = countryCode + delimitor + realNumber;
            }
            return result;
        }

        /*public static string LastCharIncrement(string inputString)
        {
            return inputString.Substring(0, inputString.Length - 1) + Convert.ToString(CharIncrement(LastChar(inputString)));
        }*/

        /*public static char LastChar(string inputString)
        {
            return Convert.ToChar(inputString.Substring(inputString.Length - 1, 1));
        }*/

        public static char CharIncrement(char c)
        {
            return ++c;
        }

        public static void IncreaseLabelCounter(Label label)
        {
            if (Text.IsInteger(label.Text))
            {
                label.Text = Convert.ToString(Convert.ToInt32(label.Text) + 1);
            }
        }

        public static bool StringContainsArrayItem(string inputString, string[] stringArray, bool ignoreCase)
        {
            if (stringArray != null)
            {
                foreach (string s in stringArray)
                {
                    if ((s != "") && (inputString.Contains(s) || (ignoreCase && inputString.ToLower().Contains(s.ToLower()))))
                        return true;
                }
            }
            return false;
        }

        public static int StringInListBox(ListBox listBox, string inputString, bool containString, bool ignoreCase)
        {
            for (int i = 0; i < listBox.Items.Count; i++)
            {
                if ((listBox.Items[i].ToString() == inputString) ||
                    ((ignoreCase) && (listBox.Items[i].ToString().ToLower() == inputString.ToLower())) ||
                    ((containString) && listBox.Items[i].ToString().Contains(inputString)) ||
                    ((containString) && (ignoreCase) && (listBox.Items[i].ToString().ToLower().Contains(inputString.ToLower()))))
                    return i;
            }
            return -1;
        }

        public void SetItemsStringBeforeTag(ListBox listBox, string tag)
        {
            for (int i = 0; i < listBox.Items.Count; i++)
            {
                listBox.Items[i] = StringBeforeTag(listBox.Items[i].ToString(), tag);
            }
        }

        public void AddItem(ListBox listBox, string inputString, int MaxItemCount)
        {
            listBox.Items.Add(inputString);
            //there is no limit on number of items if MaxItemCount equals or less than 0
            if (MaxItemCount > 0)
            {
                while (listBox.Items.Count > MaxItemCount)
                {
                    listBox.Items.RemoveAt(0);
                }
            }
            //listBox.SelectedIndex = listBox.Items.Count - 1;
        }

        public void ScrollToBottom(TextBox textBox)
        {
            textBox.SelectionStart = textBox.Text.Length;
            textBox.ScrollToCaret();
        }

        public static void ItemsAddText(ListBox listBox, string prefix, string suffix)
        {
            for (int i = 0; i < listBox.Items.Count; i++)
            {
                listBox.Items[i] = prefix + listBox.Items[i].ToString() + suffix;
            }
        }

        public static void SetOutputFieldValue(string[] fieldNames, string[] fieldValues, string outputFeildName,
            string outputFieldValue)
        {
            int index = -1;
            for (int i = 0; i < fieldNames.Length; i++)
            {
                if (fieldNames[i] == outputFeildName)
                {
                    index = i;
                    break;
                }
            }
            if ((index >= 0) && (fieldValues.Length > index))
                fieldValues[index] = outputFieldValue;
        }

        public void SetOutputFieldName(string[,] outputFields, string oldFieldName, string newFieldName)
        {
            SetStringArrayValue(outputFields, 0, 0, oldFieldName, newFieldName, true);
        }

        public static void SetOutputFieldValue(string[,] outputFields, string indexFieldValue, string outputFieldValue)
        {
            SetStringArrayValue(outputFields, 0, 2, indexFieldValue, outputFieldValue, true);
        }

        public static void ClearOutputFieldValues(string[,] outputFields)
        {
            ClearStringArrayValues(outputFields, 2);
        }

        public static string GetOutputFieldsValue(string[,] outputFields, string indexFieldValue)
        {
            return GetStringArrayValue(outputFields, 0, 2, indexFieldValue, true);
        }

        public int GetOutputFieldsIndex(string[,] outputFields, string indexFieldName)
        {
            return StringInArray(outputFields, indexFieldName, 0, false, false);
        }

        public static void ClearStringArrayValues(string[,] stringArray, int fieldIndex)
        {
            if (stringArray.Rank >= fieldIndex)
            {
                int fieldLength = stringArray.GetLength(0);
                for (int i = 0; i < fieldLength; i++)
                {
                    stringArray[i, fieldIndex] = "";
                }
            }
        }

        public static void SetStringArrayValue(string[,] stringArray, int indexFieldIndex, int outputFieldIndex,
            string indexFieldValue, string outputFieldValue, bool indexFieldValueCaseSensitive)
        {
            if (stringArray.Rank > indexFieldIndex)
            {
                for (int i = 0; i < stringArray.GetLength(indexFieldIndex); i++)
                {
                    if ((stringArray[i, indexFieldIndex] == indexFieldValue)
                        || (!indexFieldValueCaseSensitive && (stringArray[i, indexFieldIndex].ToLower() == indexFieldValue.ToLower())))
                    {
                        stringArray[i, outputFieldIndex] = outputFieldValue;
                        break;
                    }
                }
            }
        }

        public static string GetStringArrayValue(string[,] stringArray, int indexFieldIndex, int outputFieldIndex,
            string indexFieldValue, bool indexFieldValueCaseSensitive)
        {
            if (stringArray.Rank > indexFieldIndex)
            {
                for (int i = 0; i < stringArray.GetLength(indexFieldIndex); i++)
                {
                    if ((stringArray[i, indexFieldIndex] == indexFieldValue)
                        || (!indexFieldValueCaseSensitive && (stringArray[i, indexFieldIndex].ToLower() == indexFieldValue.ToLower())))
                    {
                        return stringArray[i, outputFieldIndex];
                    }
                }
            }
            return "";
        }

        public static string[] GetListBoxItems(ListBox listBox)
        {
            string[] result = new string[listBox.Items.Count];
            for (int i = 0; i < listBox.Items.Count; i++)
            {
                result[i] = listBox.Items[i].ToString();
            }
            return result;
        }

        public static void CopyListBox(ListBox fromListBox, ListBox toListBox)
        {
            if ((fromListBox != null) && (toListBox != null))
            {
                toListBox.Sorted = false;
                toListBox.Items.Clear();
                foreach (string s in fromListBox.Items)
                {
                    toListBox.Items.Add(s);
                }
            }
        }

        public static void CopyStringArray(string[] fromStringArray, string[] toStringArray)
        {
            fromStringArray.CopyTo(toStringArray, 0);
        }

        public static void AppendStringArray(ref string[] stringArray, string value)
        {
            if (stringArray != null)
            {
                string[] newStringArray = new string[stringArray.Length + 1];
                stringArray.CopyTo(newStringArray, 0);
                newStringArray[newStringArray.Length - 1] = value;
                stringArray = newStringArray;
            }
        }

        public static void ArrayListToListBox(ArrayList list, ListBox listBox, bool NoDuplicate)
        {
            foreach (string s in list)
            {
                if ((!NoDuplicate) || (!listBox.Items.Contains(s)))
                    listBox.Items.Add(s);
            }
        }

        public static void ArrayListToComboBox(ArrayList list, ComboBox comboBox)
        {
            ArrayListToComboBox(list, comboBox, true);
        }

        public static void ArrayListToComboBox(ArrayList list, ComboBox comboBox, bool NoDuplicate)
        {
            foreach (string s in list)
            {
                if ((!NoDuplicate) || (!comboBox.Items.Contains(s)))
                    comboBox.Items.Add(s);
            }
        }

        public static ArrayList ArrayListRemoveItem(ArrayList arrayList, string itemValue)
        {
            ArrayList tempList = new ArrayList();
            for (int i = 0; i < arrayList.Count; i++)
                if (arrayList[i].ToString() != itemValue)
                    tempList.Add(arrayList[i].ToString());
            return tempList;
        }

        public static string[] ArrayListToStringList(ArrayList arrayList)
        {
            string[] result = new string[arrayList.Count];
            for (int i = 0; i < arrayList.Count; i++)
                result[i] = arrayList[i].ToString();
            return result;
        }

        public static string ArrayListToString(ArrayList arrayList, string delimitor)
        {
            string result = "";
            for (int i = 0; i < arrayList.Count; i++)
                result += arrayList[i].ToString() + delimitor;
            if (result.EndsWith(delimitor))
                result = GetSubstring(result, null, false, delimitor, true, false);
            return result;
        }

        public static ArrayList StringListToArrayList(string[] stringList)
        {
            ArrayList arrayList = new ArrayList();
            foreach (string s in stringList)
                arrayList.Add(s);
            return arrayList;
        }

        public static void StringListToListBox(string[] stringList, ListBox listbox)
        {
            foreach (string s in stringList)
                listbox.Items.Add(s);
        }

        public static string[,] StringListsToStringArray(string[] stringList1, int listIndex1, string[] stringList2, int listIndex2)
        {
            if ((listIndex2 < listIndex1) ||
                (stringList1.Length != stringList2.Length))
                return null;
            string[,] stringArray = new string[stringList1.Length, listIndex2 + 1];
            for (int i = 0; i < stringList1.Length; i++)
            {
                stringArray[i, listIndex1] = stringList1[i];
                stringArray[i, listIndex2] = stringList2[i];
            }
            return stringArray;
        }

        public void GetAddressCityStateZip(string inputString,
            ref string address, ref string city, ref string state, ref string zip)
        {
            string st = inputString;
            if (st.Contains("\n"))
            {
                SplitString(st, "\n", false, false, out address, out st);
            }
            SplitString(st, " ", false, false, out st, out zip);
            if (st.Contains(","))
                SplitString(st, ",", true, false, out city, out state);
            else
                SplitString(st, " ", false, false, out city, out state);
        }

        /// <summary>
        /// Returns true if the inputstring exists within an element of the array. 
        /// Note: Ignores case.
        /// </summary>
        public static bool StringInArray(string[] stringArray, string inputString)
        {
            return StringInArray(stringArray, inputString, true, true) >= 0;
        }

        public static int StringInArray(string[] stringArray, string inputString, bool containString, bool ignoreCase)
        {
            for (int i = 0; i < stringArray.Length; i++)
            {
                if ((stringArray[i] == inputString) ||
                    ((containString) && (stringArray[i].Contains(inputString))) ||
                    ((ignoreCase) && (stringArray[i].ToLower() == inputString.ToLower())) ||
                    ((ignoreCase) && (containString) && (stringArray[i].ToLower().Contains(inputString.ToLower()))))
                    return i;
            }
            return -1;
        }

        public static int StringInArray(string[,] stringArray, string inputString, int indexColumn, bool containString, bool ignoreCase)
        {
            for (int i = 0; i < stringArray.Length; i++)
            {
                if ((stringArray[i, indexColumn] == inputString) ||
                    ((containString) && (stringArray[i, indexColumn].Contains(inputString))) ||
                    ((ignoreCase) && (stringArray[i, indexColumn].ToLower() == inputString.ToLower())) ||
                    ((ignoreCase) && (containString) && (stringArray[i, indexColumn].ToLower().Contains(inputString.ToLower()))))
                    return i;
            }
            return -1;
        }

        public static string CreateString(ListBox[] list, int depth, string delimitor)
        {
            string result = "";
            if ((list != null) && (depth >= 0) && (depth < list.Length))
            {
                for (int i = 0; i <= depth; i++)
                    result += list[i].SelectedItem.ToString() + delimitor;
            }
            if (result.EndsWith(delimitor))
                result = StringBeforeLastTag(result, delimitor);
            return result;
        }

        public string CreateStringFromArray(string[] stringArray, string delimitor)
        {
            //not tested
            string result = "";
            if (stringArray.Length > 0)
            {
                result += stringArray[0];
                for (int i = 1; i < stringArray.Length; i++)
                    result += delimitor + stringArray[i];
            }
            return result;
        }

        public string CreateStringFromArray(string[,] stringArray, int index, string delimitor)
        {
            //not tested
            string result = "";
            if (stringArray.GetLength(index) > 0)
            {
                result += stringArray[index, 0];
                for (int i = 1; i < stringArray.Length; i++)
                    result += delimitor + stringArray[index, i];
            }
            return result;
        }

        public static string GetDelimitedText(string inputString, string delimitor, int index)
        {
            string[] delimitors = new string[] { delimitor };
            string[] result = inputString.Split(delimitors, StringSplitOptions.RemoveEmptyEntries);
            if (result.Length > index)
                return result[index];
            else
                return "";
        }

        public static string[] GetDelimitedText(string inputString, string delimitor)
        {
            string[] delimitors = new string[] { delimitor };
            return inputString.Split(delimitors, StringSplitOptions.RemoveEmptyEntries);
        }

        public static string[] GetDelimitedText(string inputString, string delimitor, int elementCount, string defaultValue)
        {
            string[] result;
            string[] delimitors = new string[] { delimitor };
            result = inputString.Split(delimitors, StringSplitOptions.RemoveEmptyEntries);
            return SetStringList(result, elementCount, defaultValue);
        }

        public static string[] SetStringList(string[] stringList, int elementCount, string defaultValue)
        {
            string[] temp = new string[elementCount];
            if (stringList.Length > elementCount)
            {
                for (int i = 0; i < elementCount; i++)
                    temp[i] = stringList[i];
                return temp;
            }
            else if (stringList.Length < elementCount)
            {
                for (int i = 0; i < stringList.Length; i++)
                    temp[i] = stringList[i];
                for (int i = stringList.Length; i < elementCount; i++)
                    temp[i] = defaultValue;
                return temp;
            }
            else
                return stringList;
        }

        public static bool StringContainsTags(string inputString, string[] Tags)
        {
            foreach (string tag in Tags)
            {
                if ((tag != "") && (!inputString.Contains(tag)))
                    return false;
            }
            return true;
        }

        public ArrayList GetEmails(string s)
        {
            ArrayList list = new ArrayList();
            int length = 50;
            string st;
            string name;
            string domain;
            while (s.Contains("@"))
            {
                st = StringBeforeTag(s, "@");
                if (st.Length > length)
                    st = st.Substring(st.Length - length, length);
                st = ReverseString(st);
                name = "";
                foreach (char ch in st)
                {
                    if (IsValidEmailChar(ch))
                        name += ch;
                    else
                        break;
                }
                if (IsValidEmailName(name))
                {
                    st = StringAfterTag(s, "@");
                    if (st.Length > length)
                        st = st.Substring(0, length);
                    domain = "";
                    foreach (char ch in st)
                    {
                        if (IsValidEmailChar(ch))
                            domain += ch;
                        else
                            break;
                    }
                    if (IsValidEmailDomain(domain))
                    {
                        st = ReverseString(name) + "@" + domain;
                        if (!list.Contains(st))
                            list.Add(st);
                    }
                }
                s = StringAfterTag(s, "@");
            }
            return list;
        }

        public void GetEmails(string s, ListBox listBox, bool NoDuplicate)
        {
            int length = 50;
            string st;
            string name;
            string domain;
            while (s.Contains("@"))
            {
                st = StringBeforeTag(s, "@");
                if (st.Length > length)
                    st = st.Substring(st.Length - length, length);
                st = ReverseString(st);
                name = "";
                foreach (char ch in st)
                {
                    if (IsValidEmailChar(ch))
                        name += ch;
                    else
                        break;
                }
                if (IsValidEmailName(name))
                {
                    st = StringAfterTag(s, "@");
                    if (st.Length > length)
                        st = st.Substring(0, length);
                    domain = "";
                    foreach (char ch in st)
                    {
                        if (IsValidEmailChar(ch))
                            domain += ch;
                        else
                            break;
                    }
                    if (IsValidEmailDomain(domain))
                    {
                        st = ReverseString(name) + "@" + domain;
                        if (!NoDuplicate || (!listBox.Items.Contains(st)))
                            listBox.Items.Add(st);
                    }
                }
                s = StringAfterTag(s, "@");
                Application.DoEvents();
            }
        }

        //Todo: below needs to be used as Regex
        private bool IsValidEmailChar(char c)
        {
            if (Char.IsLetterOrDigit(c) || (c.ToString() == ".")
                || (c.ToString() == "-") || (c.ToString() == "_"))
                return true;
            else
                return false;
        }

        private bool IsValidEmailName(string s)
        {
            if ((s != "")
                && (s.Substring(0, 1) != ".")
                && (s.Substring(s.Length - 1, 1) != ".")
                && (!s.Contains("..")))
                return true;
            else
                return false;
        }

        private bool IsValidEmailDomain(string s)
        {
            if ((s != "")
                && (s.Contains("."))
                && (s.Substring(0, 1) != ".")
                && (s.Substring(s.Length - 1, 1) != ".")
                && (!s.Contains("..")))
                return true;
            else
                return false;
        }

        public string ExtractNumbers(string inputString)
        {
            return Regex.Replace(inputString, @"[^0-9]", "");
        }

        /*public static void SplitString(string inputString, string sepTag,
            out string leftOutputStr, out string rightOutputStr)
        {
            leftOutputStr = GetSubstring(inputString, null, sepTag);
            rightOutputStr = GetSubstring(inputString, sepTag, null);
        }*/

        public static void SplitString(string inputString, string sepTag, bool ignoreCase,
            out string leftOutputStr, out string rightOutputStr)
        {
            leftOutputStr = GetSubstring(inputString, null, sepTag, ignoreCase);
            rightOutputStr = GetSubstring(inputString, sepTag, null, ignoreCase);
        }

        public static void SplitString(string inputString, string sepTag, bool lastTag, bool ignoreCase,
            out string leftOutputStr, out string rightOutputStr)
        {
            leftOutputStr = GetSubstring(inputString, null, false, sepTag, lastTag, ignoreCase);
            rightOutputStr = GetSubstring(inputString, sepTag, lastTag, null, false, ignoreCase);
        }

        public static string GetTextInsideQuotes(string inputString)
        {
            if (inputString.Contains("'"))
                return GetSubstring(inputString, "'", false, "'", false, true);
            else if (inputString.Contains("\""))
                return GetSubstring(inputString, "\"", false, "\"", false, true);
            else
                return "";
        }

        public static string RemoveStartEndQuotes(string inputString)
        {
            if (inputString.StartsWith("'"))
                return RemoveStartEndTag(inputString, "'", false);
            else if (inputString.StartsWith("\""))
                return RemoveStartEndTag(inputString, "\"", false);
            else
                return inputString;
        }

        public static string RemoveStartEndTag(string inputString, string tag, bool ignoreCase)
        {
            string s = inputString;
            if (s.StartsWith(tag) ||
                (ignoreCase && (s.ToLower().StartsWith(tag.ToLower()))))
                s = s.Substring(tag.Length);
            if (s.EndsWith(tag) ||
                (ignoreCase && (s.ToLower().EndsWith(tag.ToLower()))))
                s = s.Substring(0, s.Length - tag.Length);
            return s;
        }

        public static string RemoveStringLineBreaks(string inputString)
        {
            string s = inputString;
            if (s.Contains("\r"))
                s = s.Replace("\r", "");
            if (s.Contains("\n"))
                s = s.Replace("\n", "");
            return s;
        }

        public static string RemoveStringMultiSpaces(string inputString)
        {
            string s = inputString;
            while (s.Contains("  "))
                s = s.Replace("  ", " ");
            return s;
        }

        public string ConvertHtmlValueToASCII(string inputString)
        {
            string s = inputString;
            string str = "";
            for (int i = 0; i < 255; i++)
            {
                if (i.ToString().Length < 3)
                {
                    str = SetStringLength(i.ToString(), 3, "0", "");
                    if (s.Contains("&#" + str + ";"))
                        s = s.Replace("&#" + str + ";", Convert.ToString((char)i));
                    if (s.Contains("&#" + str))
                        s = s.Replace("&#" + str, Convert.ToString((char)i));
                }
                if (s.Contains("&#" + i.ToString() + ";"))
                    s = s.Replace("&#" + i.ToString() + ";", Convert.ToString((char)i));
                if (s.Contains("&#" + i.ToString()))
                    s = s.Replace("&#" + i.ToString(), Convert.ToString((char)i));
            }
            return s;
        }

        public string SetStringLength(string inputString, int length, string prefix, string suffix)
        {
            string result = inputString;
            if (result.Length < length)
            {
                if (prefix != "")
                {
                    while (result.Length < length)
                        result = prefix + result;
                }
                else if (suffix != "")
                {
                    while (result.Length < length)
                        result += suffix;
                }
            }
            return result;
        }

        public static string HtmlCharacterToText(string inputString)
        {
            string s = inputString;
            s = s.Replace("&nbsp;", " ");
            s = s.Replace("&amp;", "&");
            s = s.Replace("&quot;", "\"");
            return s;
        }

        public static string GetInnerTextStrings(string inputString)
        {
            return GetInnerTextStrings(inputString, "");
        }

        public static string GetInnerTextStrings(string inputString, string delimitor)
        {
            string s = inputString;
            string result = "";
            string str = "";
            while (s.Contains(">"))
            {
                s = GetSubstring(s, ">", null);
                if (s.Contains("<"))
                {
                    SplitString(s, "<", ref str, ref s);
                    if (str != "")
                        result += delimitor + str;
                }
                else
                {
                    if (s != "")
                    {
                        result += delimitor + s;
                        s = "";
                    }
                }
            }
            return result;
        }

        public string GetHtmlListText(string inputString, int listInstance, string delimitor)
        {
            /* listInstance tested: 0, 1, 2, -1
             * Input text string: <tr><td valign="top"><ul><li>Driver Air Bag</li><li>Anti-Lock Brakes</li><li>Air Conditioning</li><li>Alloy Wheels</li><li>Cruise Control</li><li>Passenger Air Bag</li></ul></td><td valign="top"><ul><li>Rear Window Defroster</li><li>Power Seats</li><li>Leather Seats</li><li>Power Door Locks</li><li>Power Mirrors</li><li>Power Windows</li></ul></td><td valign="top"><ul><li>Power Steering</li><li>Side Air Bag</li><li>Sunroof/Moonroof</li><li>Tinted Glass</li><li>Tilt Wheel</li></ul></td></tr>
             * */
            string s = inputString;
            for (int i = 0; i < listInstance; i++)
            {
                s = StringAfterTag(s, "</UL>", false);
            }
            //when listInstance = -1, get itmes in all lists
            if ((listInstance >= 0) && (s.ToUpper().Contains("</UL>")))
                s = StringBeforeTag(s, "</UL>", false);
            string result = "";
            while (s.ToUpper().Contains("<LI>"))
            {
                s = StringAfterTag(s, "<LI>", false);
                if (s.ToUpper().Contains("</LI>"))
                    result += StringBeforeTag(s, "</LI>", false);
                else if (s.ToUpper().Contains("<LI>"))
                    result += StringBeforeTag(s, "<LI>", false);
                else
                    result += s;
                result += delimitor;
            }
            if (result.EndsWith(delimitor))
                result = StringBeforeLastTag(result, delimitor);
            return result;
        }

        public static string GetSubstring(string inputString, string startTag, bool reverse, string endTag)
        {
            string s = "";
            if (!reverse)
                s = GetSubstring(inputString, startTag, endTag);
            else
            {
                s = GetSubstring(inputString, null, startTag);
                s = GetSubstring(s, endTag, true, null, false, false);
            }
            return s;
        }

        public static string GetSubstring(string inputString, string startTag, string endTag)
        {
            return GetSubstring(inputString, startTag, false, endTag, false, false);
        }

        public static string GetSubstring(string inputString, string startTag, string endTag, bool ignoreCase)
        {
            return GetSubstring(inputString, startTag, false, endTag, false, ignoreCase);
        }

        public static string GetSubstring(string inputString, string startTag, string endTag, bool ignoreCase, bool returnInputString)
        {
            return GetSubstring(inputString, startTag, false, endTag, false, ignoreCase, returnInputString);
        }

        public static string GetSubstring(string inputString, string startTag, bool lastStartTag,
            string endTag, bool lastEndTag, bool ignoreCase)
        {
            return GetSubstring(inputString, startTag, lastStartTag, endTag, lastEndTag, ignoreCase, false);
        }

        public static string GetSubstring(string inputString, string startTag, bool lastStartTag,
            string endTag, bool lastEndTag, bool ignoreCase, bool returnInputString)
        {
            string s = inputString;
            if ((s != null) && (s != ""))
            {
                if (((startTag != null) && RegexPattern.WhiteSpaceString.IsMatch(startTag)) ||
                    ((endTag != null) && RegexPattern.WhiteSpaceString.IsMatch(endTag)))
                    s = s.Trim();
                if ((startTag != null) && (startTag != ""))
                {
                    if (s.Contains(startTag))
                    {
                        if (lastStartTag)
                            s = s.Substring(s.LastIndexOf(startTag) + startTag.Length);
                        else
                            s = s.Substring(s.IndexOf(startTag) + startTag.Length);
                    }
                    else if (ignoreCase && (s.ToLower().Contains(startTag.ToLower())))
                    {
                        if (lastStartTag)
                            s = s.Substring(s.ToLower().LastIndexOf(startTag.ToLower()) + startTag.Length);
                        else
                            s = s.Substring(s.ToLower().IndexOf(startTag.ToLower()) + startTag.Length);
                    }
                    else if (returnInputString)
                        return inputString;
                    else
                        return inputString;
                }
                if ((endTag != null) && (endTag != ""))
                {
                    if (s.Contains(endTag))
                    {
                        if (lastEndTag)
                            s = s.Substring(0, s.LastIndexOf(endTag));
                        else
                            s = s.Substring(0, s.IndexOf(endTag));
                    }
                    else if (ignoreCase && (s.ToLower().Contains(endTag.ToLower())))
                    {
                        if (lastEndTag)
                            s = s.Substring(0, s.ToLower().LastIndexOf(endTag.ToLower()));
                        else
                            s = s.Substring(0, s.ToLower().IndexOf(endTag.ToLower()));
                    }
                    else if (returnInputString)
                        return inputString;
                    else
                        return "";
                }
            }
            return s;
        }

        public static bool Tag1NextToTag2(string inputString, string tag1, string tag2)
        {
            return Tag1NextToTag2(inputString, tag1, tag2, true);
        }

        public static bool Tag1NextToTag2(string inputString, string tag1, string tag2, bool nextToTheLeft)
        {
            bool result = false;
            if (inputString.Contains(tag2))
            {
                if (nextToTheLeft)
                {
                    inputString = inputString.Substring(0, inputString.IndexOf(tag2));
                    if (inputString.EndsWith(tag1))
                        return true;
                }
                else
                {
                    inputString = inputString.Substring(inputString.IndexOf(tag2) + tag2.Length);
                    if (inputString.StartsWith(tag1))
                        return true;
                }

            }
            return result;
        }

        public static string AddTag1NextToTag2(string inputString, string tag1, string tag2)
        {
            return AddTag1NextToTag2(inputString, tag1, tag2, true);
        }

        public static string AddTag1NextToTag2(string inputString, string tag1, string tag2, bool nextToTheLeft)
        {
            string result = inputString;
            if (inputString.Contains(tag2))
            {
                string leftString = inputString.Substring(0, inputString.IndexOf(tag2));
                string rightString = inputString.Substring(inputString.IndexOf(tag2) + tag2.Length);
                if (nextToTheLeft)
                {
                    return leftString + tag1 + tag2 + rightString;
                }
                else
                {
                    return leftString + tag2 + tag1 + rightString;
                }
            }
            return result;
        }

        public string StringAfterTag(string s, string tag)
        {
            if (s.Contains(tag))
                return s.Substring(s.IndexOf(tag) + tag.Length);
            else
                return "";
        }

        public string StringAfterTag(string inputString, string tag, bool ignoreCase)
        {
            if ((inputString != null) && (inputString != ""))
            {
                string s = inputString;
                if (s.Contains(tag))
                    return s.Substring(s.IndexOf(tag) + tag.Length);
                else if (ignoreCase && (s.ToLower().Contains(tag.ToLower())))
                    return s.Substring(s.ToLower().IndexOf(tag.ToLower()) + tag.Length);
                else
                    return "";
            }
            else
                return "";
        }

        public string StringBeforeTag(string s, string tag)
        {
            if (s.Contains(tag))
                return s.Substring(0, s.IndexOf(tag));
            else
                return "";
        }

        public string StringBeforeTag(string s, string tag, bool ignoreCase)
        {
            string result = "";
            if (s.Contains(tag))
                result = s.Substring(0, s.IndexOf(tag));
            if (ignoreCase && (s.ToLower().Contains(tag.ToLower())))
                result = s.Substring(0, s.ToLower().IndexOf(tag.ToLower()));
            return result;
        }

        public string StringBeforeTag(string s, string tag, bool ignoreCase, bool returninputStringing)
        {
            string result = StringBeforeTag(s, tag, ignoreCase);
            if ((result.Length == 0) && (returninputStringing))
                result = s;
            return result;
        }

        public string StringBetweenTags(string s, string startTag, string endTag)
        {
            string st = StringAfterTag(s, startTag);
            if (st.Contains(endTag))
                return StringBeforeTag(st, endTag);
            else
                return "";
        }

        public string StringBetweenTags(string inputStringing, string startTag, string endTag, bool ignoreCase)
        {
            string st = StringAfterTag(inputStringing, startTag, ignoreCase);
            if (st.Contains(endTag) ||
                (ignoreCase && (st.ToLower().Contains(endTag.ToLower()))))
                return StringBeforeTag(st, endTag, ignoreCase);
            else
                return "";
        }

        public string StringAfterLastTag(string inputString, string tag)
        {
            string s = inputString;
            while (s.Contains(tag))
                s = s.Substring(s.IndexOf(tag) + tag.Length);
            return s;
        }

        public static string StringBeforeLastTag(string inputString, string tag)
        {
            string s = inputString;
            string str = "";
            while (s.Contains(tag))
            {
                str += s.Substring(0, s.IndexOf(tag) + tag.Length);
                s = s.Substring(s.IndexOf(tag) + tag.Length);
            }
            if (str != "")
                return str.Substring(0, str.Length - tag.Length);
            else
                return "";
        }

        public string ReverseString(string s)
        {
            StringBuilder sb = new StringBuilder(s.Length);
            foreach (char ch in s)
            {
                sb.Insert(0, ch);
            }
            return sb.ToString();
        }

        public void ListBoxRemoveItemsBeforeTag(ListBox listBox, string tag)
        {
            if ((tag != "") && (listBox.Items.Contains(tag)))
            {
                int pos = listBox.Items.IndexOf(tag);
                for (int i = 0; i < pos; i++)
                    listBox.Items.RemoveAt(0);
            }
        }

        public void ListBoxRemoveItemsUnitlTag(ListBox listBox, string tag)
        {
            if ((tag != "") && (listBox.Items.Contains(tag)))
            {
                int pos = listBox.Items.IndexOf(tag);
                for (int i = 0; i <= pos; i++)
                    listBox.Items.RemoveAt(0);
            }
        }

        public static void ListBoxReplaceTag1WithTag2(ListBox listBox, string tag1, string tag2)
        {
            if ((tag1 != null) && (tag1 != "") && (tag2 != null))
            {
                for (int i = 0; i < listBox.Items.Count; i++)
                {
                    if (listBox.Items[i].ToString().Contains(tag1))
                        listBox.Items[i] = listBox.Items[i].ToString().Replace(tag1, tag2);
                }
            }
        }

        public string GetTableRowString(string tableString, int rowIndex)
        {
            int count = 0;
            string result = "";
            string s = tableString;
            if (s.ToLower().Contains("<tr"))
            {
                s = StringAfterTag(s, "<tr", false);
                for (int i = 0; i < rowIndex; i++)
                {
                    if (s.ToLower().Contains("<tr"))
                    {
                        s = StringAfterTag(s, "<tr", false);
                        count++;
                    }
                }
                if ((count == rowIndex) && s.ToLower().Contains("</tr>"))
                {
                    result = "<TR " + StringBeforeTag(s, "</tr>", false) + "</TR>";
                }
            }
            return result;
        }

        public int GetTableRowCount(string tableString)
        {
            int count = 0;
            string s = tableString;
            if (s.ToLower().Contains("<tr"))
            {
                s = StringAfterTag(s, "<tr", false);
                while (s.ToLower().Contains("</tr>"))
                {
                    s = StringAfterTag(s, "</tr>", false);
                    s = StringAfterTag(s, "<tr", false);
                    count++;
                }
            }
            return count;
        }

        public string CapitalizeString(string s)
        {
            string result = "";
            while (s.Contains(" "))
            {
                result += CapitalizeWord(StringBeforeTag(s, " ")) + " ";
                s = StringAfterTag(s, " ");
            }
            result += CapitalizeWord(s);
            return result;
        }

        public string CapitalizeWord(string s)
        {
            if ((s != "") && !s.Contains(" "))
                s = s.Substring(0, 1).ToUpper() + s.Substring(1).ToLower();
            else
                s = "";
            return s;
        }

        public string TextBetweenTag1AndTag2(string s, string tag1, string tag2, bool ignoreCase, int textIndex)
        {
            int count = 0;
            string result = "";
            if (s.Contains(tag1) || (ignoreCase && (s.ToLower().Contains(tag1))))
            {
                s = StringAfterTag(s, tag1, ignoreCase);
                for (int i = 0; i < textIndex; i++)
                {
                    if (s.Contains(tag1) || (ignoreCase && (s.ToLower().Contains(tag1))))
                    {
                        s = StringAfterTag(s, tag1, ignoreCase);
                        count++;
                    }
                }
                if ((count == textIndex) && (s.Contains(tag2) || (ignoreCase && s.ToLower().Contains(tag2))))
                {
                    result = StringBeforeTag(s, tag2, ignoreCase);
                }
            }
            return result;
        }

        public string RemoveTextBetweenTag1AndTag2(string s, string tag1, string tag2)
        {
            while (s.Contains(tag1))
            {
                s = s.Remove(s.IndexOf(tag1), s.Substring(s.IndexOf(tag1)).IndexOf(tag2) + tag2.Length);
            }
            return s;
        }

    }

    public abstract class Database
    {
        

        public static void Connect(DbConnection conn, string connStr)
        {
            conn.Close();
            conn.ConnectionString = connStr;
            conn.Open();
        }

        public static void Connect(OdbcConnection conn, string connStr)
        {
            conn.Close();
            conn.ConnectionString = connStr;
            conn.Open();
        }

        public static void Connect(OleDbConnection conn, string connStr)
        {
            conn.Close();
            conn.ConnectionString = connStr;
            conn.Open();
        }

        public static void Connect(OleDbConnection conn, string Provider, string PersistSecurityInfo, string UserID,
            string password, string DataSource, string InitialCatalog, string ConnectTimeout)
        {
            string connStr = CreateConnectionString(Provider, PersistSecurityInfo, UserID, password,
                DataSource, InitialCatalog, ConnectTimeout);
            Connect(conn, connStr);
        }

        public static void AccessToExcel(OleDbConnection accessConn, OleDbConnection excelConn,
            string accessTableName, string excelTableName)
        {
            Database.CopyTable(accessConn, excelConn, accessTableName, excelTableName, null, true);

            /*int arrayLength = 0;
            int startField = 0;
            string[,] outputFields = new string[arrayLength, 3];
            string query = string.Format("SELECT * FROM {0}", accessTableName);

            OleDbCommand sqlCommand = new OleDbCommand(query, accessConn);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                if (arrayLength == 0)
                {
                    arrayLength = reader.FieldCount;
                    if (removeIDField && (reader.GetName(0).ToUpper() == "ID"))
                    {
                        arrayLength--;
                        startField = 1;
                    }
                    outputFields = new string[arrayLength, 3];
                    for (int i = 0; i < arrayLength; i++)
                    {
                        outputFields[i, 0] = reader.GetName(i + startField);
                        outputFields[i, 1] = reader.GetFieldType(i + startField).ToString();
                    }
                }
                for (int i = 0; i < arrayLength; i++)
                {
                    outputFields[i, 2] = reader.GetValue(i + startField).ToString();
                }
                AddRecord(excel, outputTableName, outputFields);
                Application.DoEvents();
            }


            /*
            procedure AccessToExcel(AccessTableName, ExcelTableName: string; RemoveIdField: Boolean = False);
var
  i: integer;
  OutputFieldName, OutputFieldText: array of string;
begin
  with dm.ADOQuery1 do
  begin
    Application.ProcessMessages;
    Close;
    SQL.Text := 'SELECT * FROM [' + AccessTableName + ']';
    try
      Open;
      if RemoveIdField and (UpperCase(Fields[0].FieldName) = 'ID') then
      begin
        SetLength(OutputFieldName, FieldCount - 1);
        SetLength(OutputFieldText, FieldCount - 1);
        for i := 1 to FieldCount - 1 do
          OutputFieldName[i - 1] := Fields[i].FieldName;
      end else
      begin
        SetLength(OutputFieldName, FieldCount);
        SetLength(OutputFieldText, FieldCount);
        for i := 0 to FieldCount - 1 do
          OutputFieldName[i] := Fields[i].FieldName;
      end;
      while not Eof do
      begin
        if RemoveIdField and (UpperCase(Fields[0].FieldName) = 'ID') then
        begin
          for i := 1 to FieldCount - 1 do
            OutputFieldText[i - 1] := Fields[i].AsString;
          AddRecord2(ExcelTableName, OutputFieldName, OutputFieldText, FieldCount - 1);
        end else
        begin
          for i := 0 to FieldCount - 1 do
            OutputFieldText[i] := Fields[i].AsString;
          AddRecord2(ExcelTableName, OutputFieldName, OutputFieldText, FieldCount)
        end;
        Application.ProcessMessages;
        Next;
      end;
    except
    end;
  end;
end;*/


        }

        public static void ConnectToExcel(OleDbConnection conn, string databaseName)
        {
            conn.ConnectionString = "Provider=Microsoft.Jet.OLEDB.4.0;" +
             "Data Source=" + databaseName + ';' +
             "Extended Properties=Excel 8.0;";
            conn.Open();
        }

        public static string CreateConnectionString(string Provider, string PersistSecurityInfo, string UserID,
            string Password, string DataSource, string InitialCatalog, string ConnectTimeout)
        {
            string s = "";
            if (Provider != "")
                s += "Provider=" + Provider + ";";
            if (PersistSecurityInfo != "")
                s += "Persist Security Info=" + PersistSecurityInfo + ";";
            if (UserID != "")
                s += "User ID=" + UserID + ";";
            if (Password != "")
                s += "Password=" + Password + ";";
            if (InitialCatalog != "")
                s += "Initial Catalog=" + InitialCatalog + ";";
            if (DataSource != "")
                s += "Data Source=" + DataSource + ";";
            if (ConnectTimeout != "")
                s += "Connect Timeout=" + ConnectTimeout + ";";
            s = Text.StringBeforeLastTag(s, ";");
            return s;
        }

        public static void ExecuteSqlNonQuery(OleDbConnection conn, string sqlText)
        {
            //TODO: need to replace all repeated codes above to use this method
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            sqlCommand.ExecuteNonQuery();
        }

        public static void ExecuteSqlNonQuery(OdbcConnection conn, string sqlText)
        {
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            sqlCommand.ExecuteNonQuery();
        }

        public static void DropTable(OleDbConnection conn, string tableName)
        {
            string sqlText = "DROP TABLE [" + tableName + "]";
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static void CreateTable(OdbcConnection conn, string tableName, string[,] tableFields)
        {
            CreateTable(conn, tableName, tableFields, "ID");
        }

        public static void CreateTable(SqlConnection conn, string tableName, string[,] tableFields)
        {
            string primaryKey = "ID";

            string sqlText = "CREATE TABLE " + tableName + " (";
            if (primaryKey == "ID")
                sqlText += "ID INT IDENTITY(1,1) NOT NULL, ";
            for (int i = 0; i < tableFields.GetLength(0); i++)
            {
                //field name, field type, field value, not null
                if (tableFields.Rank > 3)
                    sqlText += tableFields[i, 0] + " " + tableFields[i, 1] + " " + tableFields[i, 3] + ", ";
                else
                    sqlText += tableFields[i, 0] + " " + tableFields[i, 1] + ", ";
            }
            if (primaryKey != "")
                sqlText += " PRIMARY KEY (" + primaryKey + ")";
            else
                sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ")";

            SqlCommand command = new SqlCommand(sqlText, conn);

            try
            {
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("There is already an object named"))
                {
                    throw ex;
                }
            }
        }


        public static void CreateTable(OdbcConnection conn, string tableName, string[,] tableFields, string primaryKey)
        {
            string sqlText = "CREATE TABLE IF NOT EXISTS " + tableName + " (";
            if (primaryKey == "ID")
                sqlText += "ID INT NOT NULL AUTO_INCREMENT, ";
            for (int i = 0; i < tableFields.GetLength(0); i++)
            {
                //field name, field type, field value, not null
                if (tableFields.Rank > 3)
                    sqlText += tableFields[i, 0] + " " + tableFields[i, 1] + " " + tableFields[i, 3] + ", ";
                else
                    sqlText += tableFields[i, 0] + " " + tableFields[i, 1] + ", ";
            }
            if (primaryKey != "")
                sqlText += " PRIMARY KEY (" + primaryKey + ")";
            else
                sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ")";
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static void CreateTable(OleDbConnection conn, string tableName, string[] outputFieldName, string[] outputFieldType,
            bool withID)
        {
            string sqlText = "CREATE TABLE [" + tableName + "] (";
            if (withID)
                sqlText += "ID Int Identity, ";
            for (int i = 0; i != outputFieldName.Length; i++)
            {
                sqlText += "[" + outputFieldName[i] + "] " + outputFieldType[i] + ", ";
            }
            if (withID)
                sqlText += " PRIMARY KEY (ID)";
            else
                sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ")";
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static void CreateTable(OleDbConnection conn, string tableName, string[,] tableFields, string primaryKey)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "CREATE TABLE [" + tableName + "] (";
            if (primaryKey == "ID")
                sqlText += "ID Int Identity, ";
            for (int i = 0; i != fieldLength; i++)
            {
                sqlText += "[" + tableFields[i, 0] + "] " + tableFields[i, 1] + ", ";
            }
            if (primaryKey != "")
                sqlText += " PRIMARY KEY ([" + primaryKey + "])";
            else
                sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ")";
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static void CreateIndex(OleDbConnection conn, bool isUnique, string tableName, string indexName, string[] indexFieldName)
        {
            string sqlText = "CREATE";
            if (isUnique)
                sqlText += " UNIQUE";
            sqlText += " INDEX " + indexName + " ON [" + tableName + "] (";
            foreach (string fieldName in indexFieldName)
            {
                sqlText += "[" + fieldName + "], ";
            }
            sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ")";
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static List<string> GetIndexFieldValues(string[] indexFields, DataRow outputRow)
        {
            List<string> indexFieldValues = new List<string>();

            foreach (string indexField in indexFields)
            {
                indexFieldValues.Add(outputRow[indexField].ToString());
            }

            return indexFieldValues;
        }

        public static void CreateIndex(OleDbConnection conn, bool isUnique, string tableName, string indexName, string indexFieldName)
        {
            string[] indexFields = { indexFieldName };
            CreateIndex(conn, isUnique, tableName, indexName, indexFields);
        }

        public static void CreateIndex(OleDbConnection conn, bool isUnique, string tableName, string indexName, ArrayList indexFields)
        {
            string[] indexFieldNames = Text.ArrayListToStringList(indexFields);
            CreateIndex(conn, isUnique, tableName, indexName, indexFieldNames);
        }

        public static bool RecordChanged(OleDbConnection conn, string tableName, ArrayList compareFieldName, ArrayList compareFieldValue,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            return RecordChanged(conn, tableName, compareFieldName, compareFieldValue, conditionFields);
        }

        public static bool RecordChanged(OleDbConnection conn, string tableName, ArrayList compareFieldName, ArrayList compareFieldValue,
            string[,] conditionFields)
        {
            string[] fieldNames = Text.ArrayListToStringList(compareFieldName);
            string[] fieldValues = Text.ArrayListToStringList(compareFieldValue);
            return RecordChanged(conn, tableName, fieldNames, fieldValues, conditionFields);
        }

        public static bool RecordChanged(OleDbConnection conn, string tableName, string[] compareFieldNames, string[] compareFieldValues,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            return RecordChanged(conn, tableName, compareFieldNames, compareFieldValues, conditionFields);
        }

        public static bool RecordChanged(OleDbConnection conn, string tableName, string[] compareFieldNames, string[] compareFieldValues,
            string[,] conditionFields)
        {
            string[] fieldValues = GetFieldValues(conn, tableName, compareFieldNames, conditionFields);
            if (fieldValues.Length != compareFieldNames.Length)
                return true;
            for (int i = 0; i < fieldValues.Length; i++)
            {
                if (fieldValues[i] != compareFieldValues[i])
                    return true;
            }
            return false;
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string fieldName,
            string fieldValue, string[,] conditionFields)
        {
            string[] fieldNames = { fieldName };
            string[] fieldValues = { fieldValue };
            UpdateTable(conn, tableName, fieldNames, fieldValues, conditionFields);
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string fieldName,
            string fieldValue, string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            UpdateTable(conn, tableName, fieldName, fieldValue, conditionFields);
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string[,] tableFields,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            UpdateTable(conn, tableName, tableFields, conditionFields);
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string[] fieldNames,
            string[] fieldValues, string[,] conditionFields)
        {
            string[,] tableFields = Text.StringListsToStringArray(fieldNames, 0, fieldValues, 2);
            UpdateTable(conn, tableName, tableFields, conditionFields);
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string[] fieldNames,
            string[] fieldValues, string[,] conditionFields, bool ignoreNullValueFields)
        {
            string[,] tableFields = Text.StringListsToStringArray(fieldNames, 0, fieldValues, 2);
            UpdateTable(conn, tableName, tableFields, conditionFields, ignoreNullValueFields);
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string[,] tableFields,
            string[,] conditionFields)
        {
            UpdateTable(conn, tableName, tableFields, conditionFields, true);
        }

        public static void UpdateTable(OleDbConnection conn, string tableName, string[,] tableFields,
            string[,] conditionFields, bool ignoreNullValueFields)
        {
            string sqlText = "UPDATE [" + tableName + "] SET ";
            for (int i = 0; i != tableFields.GetLength(0); i++)
            {
                if ((!ignoreNullValueFields) || (tableFields[i, 2] != ""))
                    sqlText += "[" + tableFields[i, 0] + "] = ?, ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != tableFields.GetLength(0); i++)
            {
                if ((!ignoreNullValueFields) || (tableFields[i, 2] != ""))
                {
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(),
                        ValidateNumericString(tableFields[i, 1], tableFields[i, 2]));
                }
            }
            SqlCommandAddParameters(sqlCommand, conditionFields);
            sqlCommand.ExecuteNonQuery();
        }

        private static string SqlTextAddConditions(string sqlText, string[,] conditionFields)
        {
            string result = sqlText;
            if ((conditionFields != null) && (conditionFields.GetLength(0) > 0))
            {
                if (!sqlText.ToUpper().Contains(" WHERE "))
                    result += " WHERE ";
                else
                    result += " AND ";
                for (int i = 0; i != conditionFields.GetLength(0); i++)
                {
                    if ((conditionFields[i, 0] != null) && (conditionFields[i, 0] != "") &&
                        (conditionFields[i, 1] != null) && (conditionFields[i, 1] != "") &&
                        (conditionFields[i, 2] != null) && (conditionFields[i, 2] != ""))
                    {
                        if ((!conditionFields[i, 0].Contains("(")) &&
                            (conditionFields[i, 0].Contains(" ") ||
                            conditionFields[i, 0].Contains("-")))
                            result += "[" + conditionFields[i, 0] + "] " + conditionFields[i, 1];
                        else
                            result += conditionFields[i, 0] + conditionFields[i, 1];
                        result += " ?";
                        result += " AND ";
                    }
                }
                if (result.EndsWith("AND "))
                    result = Text.StringBeforeLastTag(result, "AND ");
                if (result.EndsWith(" WHERE "))
                    result = Text.StringBeforeLastTag(result, " WHERE ");
            }
            return result;
        }

        private static void SqlCommandAddParameters(OdbcCommand sqlCommand, string[,] conditionFields)
        {
            if (conditionFields != null)
            {
                for (int i = 0; i != conditionFields.GetLength(0); i++)
                {
                    if ((conditionFields[i, 2] != null) && (conditionFields[i, 2] != ""))
                        sqlCommand.Parameters.AddWithValue("@cp" + i.ToString(),
                            ValidateNumericString(conditionFields[i, 1], conditionFields[i, 2]));
                }
            }
        }

        private static void SqlCommandAddParameters(OleDbCommand sqlCommand, string[,] conditionFields)
        {
            if (conditionFields != null)
            {
                for (int i = 0; i != conditionFields.GetLength(0); i++)
                {
                    if ((conditionFields[i, 2] != null) && (conditionFields[i, 2] != ""))
                        sqlCommand.Parameters.AddWithValue("@cp" + i.ToString(),
                            ValidateNumericString(conditionFields[i, 1], conditionFields[i, 2]));
                }
            }
        }

        private static string ValidateNumericString(string fieldType, string fieldValue)
        {
            string result = fieldValue;
            if ((fieldType != null) && ((fieldType.ToLower() == "int") || (fieldType.ToLower() == "smallint")
                || (fieldType.ToLower() == "money") || (fieldType.ToLower() == "smallmoney")))
            {
                if (result[0].ToString() == "$")
                    result = result.Substring(1);
                if (result.Contains(","))
                    result = result.Replace(",", "");
                if (!Text.IsNumber(result))
                    result = "0";
            }
            return result;
        }

        public static void DeleteRecord(OleDbConnection conn, string tableName, string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            DeleteRecord(conn, tableName, conditionFields);
        }

        public static void DeleteRecord(OleDbConnection conn, string tableName, string[,] conditionFields)
        {
            string sqlText = "DELETE FROM [" + tableName + "]";
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            sqlCommand.ExecuteNonQuery();
        }

        public static void DeleteRecord(OdbcConnection conn, string tableName, string[,] conditionFields)
        {
            string sqlText = "";
            if (tableName.Contains(" "))
                sqlText += "DELETE FROM [" + tableName + "]";
            else
                sqlText += "DELETE FROM " + tableName;
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            sqlCommand.ExecuteNonQuery();
        }

        public static string[] TableRowToStringList(DataRow row)
        {
            string[] result = new string[row.ItemArray.Length];
            for (int i = 0; i < row.ItemArray.Length; i++)
                result[i] = row[i].ToString();
            return result;
        }

        public static string[,] CreateTableFields(DataTable table)
        {
            string[,] result = new string[table.Columns.Count, 3];
            for (int i = 0; i < table.Columns.Count; i++)
            {
                result[i, 0] = table.Columns[i].ColumnName;
                result[i, 1] = "varchar(200)";
                if (table.Columns[i].DataType.Name == "Int32")
                    result[i, 1] = "int";
                if (table.Columns[i].DataType.Name == "String")
                    result[i, 1] = "varchar(200)";
                result[i, 2] = "";
            }
            return result;
        }

        public static void ImportTable(OdbcConnection conn, DataTable table)
        {
            ImportTable(conn, table, null);
        }

        public static void ImportTable(OdbcConnection conn, DataTable table, ToolStripProgressBar progressBar)
        {
            string[,] tableFields = CreateTableFields(table);
            for (int i = 0; i < tableFields.GetLength(0); i++)
            {
                if (tableFields[i, 0].Contains(" "))
                    tableFields[i, 0] = tableFields[i, 0].Replace(" ", "_");
            }
            CreateTable(conn, table.TableName, tableFields);
            if (progressBar != null)
            {
                progressBar.Maximum = table.Rows.Count;
                progressBar.Visible = true;
            }
            for (int i = 0; i < table.Rows.Count; i++)
            {
                for (int j = 0; j < table.Columns.Count; j++)
                    tableFields[j, 2] = table.Rows[i].ItemArray.GetValue(j).ToString();
                AddRecord(conn, table.TableName, tableFields);
                progressBar.Value++;
                Application.DoEvents();
            }
            progressBar.Visible = false;
        }

        public static void CopyTable(OleDbConnection conn1, OleDbConnection conn2, string inputTableName, string outputTableName,
            string[,] conditionFields, bool removeIDField)
        {
            int arrayLength = 0;
            int startField = 0;
            string[,] outputFields = new string[arrayLength, 3];
            string sqlText = "SELECT * FROM [" + inputTableName + "]";
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn1);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                if (arrayLength == 0)
                {
                    arrayLength = reader.FieldCount;
                    if (removeIDField && (reader.GetName(0).ToUpper() == "ID"))
                    {
                        arrayLength--;
                        startField = 1;
                    }
                    outputFields = new string[arrayLength, 3];
                    for (int i = 0; i < arrayLength; i++)
                    {
                        outputFields[i, 0] = reader.GetName(i + startField);
                        outputFields[i, 1] = reader.GetFieldType(i + startField).ToString();
                    }
                }
                for (int i = 0; i < arrayLength; i++)
                {
                    outputFields[i, 2] = reader.GetValue(i + startField).ToString();
                }
                AddRecord(conn2, outputTableName, outputFields);
                Application.DoEvents();
            }
        }

        public static DataTable ExportTable(OleDbConnection conn, string tableName)
        {
            return ExportTable(conn, tableName, true);
        }

        public static DataTable ExportTable(OleDbConnection conn, string tableName, bool ignoreIdField)
        {
            DataTable table = new DataTable();
            OleDbDataAdapter dataAdapter = new OleDbDataAdapter("SELECT * FROM [" + tableName + "]", conn);
            dataAdapter.Fill(table);
            if (ignoreIdField)
            {
                int idField = table.Columns.IndexOf("Id");
                if (idField >= 0)
                    table.Columns.RemoveAt(idField);
            }
            return table;
        }

        public static DataTable ExportTable(OdbcConnection conn, string tableName, bool ignoreIdField)
        {
            DataTable table = new DataTable();
            OdbcDataAdapter dataAdapter = new OdbcDataAdapter("SELECT * FROM " + tableName, conn);
            dataAdapter.Fill(table);
            if (ignoreIdField)
            {
                int idField = table.Columns.IndexOf("Id");
                if (idField >= 0)
                    table.Columns.RemoveAt(idField);
            }
            return table;
        }
        /*
        public static DataTable ExportTable(OleDbConnection conn, string tableName, string[,] conditionFields)
        {
            return ExportTable(conn, tableName, "", false, conditionFields, null);
        }

        public static DataTable ExportTable(OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            if ((outputFieldName == null) || (outputFieldName == ""))
                outputFieldName = "*";
            string[] fieldNames = { outputFieldName };
            return ExportTable(conn, tableName, fieldNames, distinct, conditionFields, orderBy);
        }

        public static DataTable ExportTable(OleDbConnection conn, string tableName, ArrayList outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] fieldNames = Text.ArrayListToStringList(outputFieldNames);
            return ExportTable(conn, tableName, fieldNames, distinct, conditionFields, orderBy);
        }

        public static DataTable ExportTable(OleDbConnection conn, string tableName, string[] outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            DataTable table = new DataTable();
            OleDbDataAdapter dataAdapter = new OleDbDataAdapter("SELECT * FROM [" + tableName + "]", conn);
            dataAdapter.Fill(table);
            return table;
        }*/
        /*
        public static DataTable ExportTable(OleDbConnection conn, string tableName, string[] outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            OleDbDataAdapter dataAdapter = new OleDbDataAdapter("SELECT * FROM [" + tableName + "]", conn);
            DataSet dataSet = new DataSet();

            DataTable table = new DataTable();
            dataAdapter.Fill(table);

            DataColumn column;
            DataRow row;
            OleDbCommand sqlCommand = new OleDbCommand();
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldNames, distinct, conditionFields, orderBy);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                if (table.Columns.Count < reader.FieldCount)
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        column = new DataColumn();
                        column.DataType = reader.GetFieldType(i);
                        column.ColumnName = reader.GetName(i);
                        table.Columns.Add(column);
                    }
                }
                row = table.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.GetValue(i).ToString();
                }
                table.Rows.Add(row);
            }
            return table;
        }*/

        public static void AddRecord(OdbcConnection conn, string tableName, string[,] tableFields)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "";
            if (tableName.Contains(" "))
                sqlText += "INSERT INTO [" + tableName + "] (";
            else
                sqlText += "INSERT INTO " + tableName + " (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                {
                    if (tableFields[i, 0].Contains(" "))
                        sqlText += "[" + tableFields[i, 0] + "], ";
                    else
                        sqlText += tableFields[i, 0] + ", ";
                }
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ") VALUES (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "?, ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ")";
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                {
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(),
                        ValidateNumericString(tableFields[i, 1], tableFields[i, 2]));
                }
            }
            sqlCommand.ExecuteNonQuery();
        }

        public static void AddRecord(OleDbConnection conn, string tableName, string fieldName, string fieldValue)
        {
            string[] fieldNames = { fieldName };
            string[] fieldValues = { fieldValue };
            AddRecord(conn, tableName, fieldNames, fieldValues);
        }

        public static void AddRecord(OleDbConnection conn, string tableName, string[] fieldNames, string[] fieldValues)
        {
            string sqlText = "INSERT INTO [" + tableName + "] (";
            foreach (string fieldName in fieldNames)
            {
                sqlText += "[" + fieldName + "], ";
            }
            sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ") VALUES (";
            for (int i = 0; i != fieldValues.Length; i++)
            {
                sqlText += "?, ";
            }
            sqlText = sqlText.Remove(sqlText.Length - 2, 1);
            sqlText += ")";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldValues.Length; i++)
            {
                sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), fieldValues[i]);
            }
            sqlCommand.ExecuteNonQuery();
        }

        public static void AddRecord(OleDbConnection conn, string tableName, string[,] tableFields)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "";
            sqlText += "INSERT INTO [" + tableName + "] (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "[" + tableFields[i, 0] + "], ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ") VALUES (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "?, ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ")";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                {
                    string value = ValidateNumericString(tableFields[i, 1], tableFields[i, 2]);
                    if (value == null)
                    {
                        value = string.Empty;
                    }

                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), value);
                }
            }
            sqlCommand.ExecuteNonQuery();
        }

        public static void AddRecord(OleDbConnection conn, string tableName, string[,] tableFields,
            string indexFieldName, string indexFieldOperator, string indexFieldValue)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "";
            if ((indexFieldName != "") && (indexFieldValue != ""))
            {
                sqlText += "IF NOT EXISTS (SELECT NULL FROM "
                    + tableName + " WHERE " + indexFieldName + indexFieldOperator + "'" + indexFieldValue + "')";
            }
            sqlText += " INSERT INTO [" + tableName + "] (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "[" + tableFields[i, 0] + "], ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ") VALUES (";
            //try not to use parameters since it seems it slows down the SQL Server
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    //sqlText += "'" + ValidateNumericString(tableFields[i, 1], tableFields[i, 2]) + "', ";
                    sqlText += "?, ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ")";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, tableFields);
            sqlCommand.ExecuteNonQuery();
        }

        public static void AddRecord(OleDbConnection conn, string tableName, string[,] tableFields,
            string indexFieldName, string indexFieldOperator, string indexFieldValue, bool useParameters)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "";
            if ((indexFieldName != "") && (indexFieldValue != ""))
            {
                sqlText += "IF NOT EXISTS (SELECT NULL FROM "
                    + tableName + " WHERE " + indexFieldName + indexFieldOperator + "?"
                    + ") ";
            }
            sqlText += "INSERT INTO [" + tableName + "] (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "[" + tableFields[i, 0] + "], ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ") VALUES (";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "?, ";
            }
            if (sqlText.EndsWith(", "))
                sqlText = Text.StringBeforeLastTag(sqlText, ", ");
            sqlText += ")";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            sqlCommand.Parameters.AddWithValue("@existscondition", indexFieldValue);
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                {
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(),
                        ValidateNumericString(tableFields[i, 1], tableFields[i, 2]));
                }
            }
            sqlCommand.ExecuteNonQuery();
        }

        public static string ExecuteSqlQuery(OleDbConnection conn, string sqlText, string parameter)
        {
            //TODO need to replace all repeated codes above to use this method
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            if ((parameter != null) && (parameter != ""))
                sqlCommand.Parameters.AddWithValue("@p0", parameter);
            object sqlResult = sqlCommand.ExecuteScalar();
            if (sqlResult != null)
                return sqlResult.ToString();
            else
                return "";
        }

        public static string SqlCommandExecuteScalar(OleDbCommand sqlCommand)
        {
            //TODO need to replace all repeated codes above to use this method
            object sqlResult = sqlCommand.ExecuteScalar();
            if (sqlResult != null)
                return sqlResult.ToString();
            else
                return "";
        }

        public static string SqlCommandExecuteScalar(OdbcCommand sqlCommand)
        {
            object sqlResult = sqlCommand.ExecuteScalar();
            if (sqlResult != null)
                return sqlResult.ToString();
            else
                return "";
        }

        public static string GetFieldValue(OleDbConnection conn, string tableName, string outputFieldName, bool distinct,
            string conditionName, string conditionOperator, string conditionValue, string orderBy)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            return GetFieldValue(conn, tableName, outputFieldName, distinct, conditionFields, orderBy);
        }

        public static string GetFieldValue(OleDbConnection conn, string tableName, string outputFieldName,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            return GetFieldValue(conn, tableName, outputFieldName, false, conditionFields, "");
        }

        public static string GetFieldValue(OleDbConnection conn, string tableName, string outputFieldName, bool firstRecord)
        {
            int minId = GetTableMinID(conn, tableName, 0);
            string[,] conditionFields = { { "ID", "=", minId.ToString() } };
            return GetFieldValue(conn, tableName, outputFieldName, conditionFields);
        }

        public static string GetFieldValue(OleDbConnection conn, string tableName, string outputFieldName,
            string[,] conditionFields)
        {
            return GetFieldValue(conn, tableName, outputFieldName, false, conditionFields, null);
        }

        public static string GetFieldValue(OleDbConnection conn, string tableName, string outputFieldName,
            string[,] conditionFields, string orderBy)
        {
            return GetFieldValue(conn, tableName, outputFieldName, false, conditionFields, orderBy);
        }

        public static string GetFieldValue(OleDbConnection conn, string tableName, string outputFieldName, bool distinct,
            string[,] conditionFields, string orderBy)
        {
            OleDbCommand sqlCommand = new OleDbCommand();
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldName, distinct,
                conditionFields, orderBy);
            return SqlCommandExecuteScalar(sqlCommand);
        }

        public static string GetScalar(OleDbConnection conn, string query)
        {
            OleDbCommand sqlCommand = new OleDbCommand();
            sqlCommand.CommandText = query;
            return SqlCommandExecuteScalar(sqlCommand);
        }

        public static OleDbDataReader GetDataReader(OleDbConnection conn, string query)
        {
            OleDbCommand sqlCommand = new OleDbCommand();
            sqlCommand.Connection = conn;
            sqlCommand.CommandText = query;
            OleDbDataReader reader = sqlCommand.ExecuteReader();

            return reader;
        }

        public static string GetFieldValue(OdbcConnection conn, string tableName, string outputFieldName, bool distinct,
            string[,] conditionFields, string orderBy)
        {
            OdbcCommand sqlCommand = new OdbcCommand();
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldName, distinct,
                conditionFields, orderBy);
            return SqlCommandExecuteScalar(sqlCommand);
        }

        public static void GetFieldValues(ListBox listBox, OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct)
        {
            GetFieldValues(listBox, conn, tableName, outputFieldName, distinct, null, null);
        }

        public static void GetFieldValues(ListBox listBox, OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string conditionName, string conditionOperator, string conditionValue, string orderBy)
        {
            string[,] conditionFields = {
                { conditionName, conditionOperator, conditionValue }
            };
            GetFieldValues(listBox, conn, tableName, outputFieldName, distinct, conditionFields, orderBy);
        }

        public static void GetFieldValues(ListBox listBox, OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            ArrayList arrayList = GetFieldValues(conn, tableName, outputFieldName, distinct, conditionFields, orderBy);
            foreach (string s in arrayList)
            {
                listBox.Items.Add(s);
            }
        }

        public static string[] GetFieldValues(OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string conditionName, string conditionOperator, string conditionValue, string orderBy)
        {
            ListBox listBox = new ListBox();
            GetFieldValues(listBox, conn, tableName, outputFieldName,
                distinct, conditionName, conditionOperator, conditionValue, orderBy);
            return Text.GetListBoxItems(listBox);
        }

        public static string[] GetFieldValues(OleDbConnection conn, string tableName, string[] outputFieldNames,
            string[,] conditionFields)
        {
            ArrayList fieldNames = Text.StringListToArrayList(outputFieldNames);
            ArrayList fieldValues = GetFieldValues(conn, tableName, fieldNames, conditionFields);
            string[] outputFieldValues = Text.ArrayListToStringList(fieldValues);
            return outputFieldValues;
        }

        public static string[] GetFieldValues(OleDbConnection conn, string tableName, string outputFieldName,
            string[,] conditionFields)
        {
            string[] outputFieldNames = { outputFieldName };
            return GetFieldValues(conn, tableName, outputFieldNames, conditionFields);
        }

        public static ArrayList GetFieldValues(OleDbConnection conn, string tableName, ArrayList outputFieldNames,
            string[,] conditionFields)
        {
            return GetFieldValues(conn, tableName, outputFieldNames, false, conditionFields, null);
        }

        public static ArrayList GetFieldValues(OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] outputFieldNames = { outputFieldName };
            ArrayList fieldNames = Text.StringListToArrayList(outputFieldNames);
            return GetFieldValues(conn, tableName, fieldNames, distinct, conditionFields, orderBy);
        }

        public static ArrayList GetFieldValues(OleDbConnection conn, string tableName, ArrayList outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            ArrayList fieldValues = new ArrayList();
            OleDbCommand sqlCommand = new OleDbCommand();
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldNames, distinct, conditionFields, orderBy);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    fieldValues.Add(reader.GetValue(i).ToString());
            }
            return fieldValues;
        }

        public static ArrayList GetFieldValues(OdbcConnection conn, string tableName, string outputFieldName)
        {
            return GetFieldValues(conn, tableName, outputFieldName, false, null, null);
        }

        public static ArrayList GetFieldValues(OdbcConnection conn, string tableName, string outputFieldName,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] outputFieldNames = { outputFieldName };
            return GetFieldValues(conn, tableName, outputFieldNames, distinct, conditionFields, orderBy);
        }

        public static ArrayList GetFieldValues(OdbcConnection conn, string tableName, string[] outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            ArrayList fieldNames = Text.StringListToArrayList(outputFieldNames);
            return GetFieldValues(conn, tableName, fieldNames, distinct, conditionFields, orderBy);
        }

        public static ArrayList GetFieldValues(OdbcConnection conn, string tableName, ArrayList outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            ArrayList fieldValues = new ArrayList();
            OdbcCommand sqlCommand = new OdbcCommand();
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldNames, distinct, conditionFields, orderBy);
            OdbcDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    fieldValues.Add(reader.GetValue(i).ToString());
            }
            return fieldValues;
        }

        private void SqlCommandExecuteReader(ref OleDbDataReader reader, OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string conditionName, string conditionOperator, string conditionValue, string orderBy)
        {
            /*
             * This method cannot work as C# will generate an error when define an external OleDbDataReader to call this method
             * Statement: OleDbDataReader reader = new OleDbDataReader();
             * Error message: The type 'System.Data.OleDb.OleDbDataReader' has no constructors defined
             * Use below SetSqlCommand is ok.
             * */
            string sqlText = "SELECT ";
            if (distinct)
                sqlText += "DISTINCT ";
            sqlText += "[" + outputFieldName + "] FROM [" + tableName + "]";
            if ((conditionName != null) && (conditionName != "") && (conditionOperator != null) && (conditionOperator != ""))
            {
                sqlText += " WHERE [" + conditionName + "] " + conditionOperator;
                if ((conditionValue != null) && (conditionValue != ""))
                    sqlText += " ?";
            }
            if ((orderBy != null) && (orderBy != ""))
                sqlText += " ORDER BY [" + orderBy + "]";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            if ((conditionValue != null) && (conditionValue != ""))
                sqlCommand.Parameters.AddWithValue("@p0", conditionValue);
            reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
        }

        private static void SetSqlCommand(ref OleDbCommand sqlCommand, OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string conditionName, string conditionOperator, string conditionValue, string orderBy)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldName, distinct, conditionFields, orderBy);
        }

        private static void SetSqlCommand(ref OleDbCommand sqlCommand, OleDbConnection conn, string tableName, string[] outputFieldNames,
            string[,] conditionFields)
        {
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldNames, false, conditionFields, null);
        }

        private static void SetSqlCommand(ref OleDbCommand sqlCommand, OleDbConnection conn, string tableName, ArrayList outputFieldNames,
            string[,] conditionFields)
        {
            string[] fieldNameStrings = Text.ArrayListToStringList(outputFieldNames);
            SetSqlCommand(ref sqlCommand, conn, tableName, fieldNameStrings, conditionFields);
        }

        private static void SetSqlCommand(ref OleDbCommand sqlCommand, OleDbConnection conn, string tableName, string outputFieldName,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] outputFieldNames = { outputFieldName };
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldNames, distinct, conditionFields, orderBy);
        }

        private static void SetSqlCommand(ref OleDbCommand sqlCommand, OleDbConnection conn, string tableName, ArrayList outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] fieldNameStrings = Text.ArrayListToStringList(outputFieldNames);
            SetSqlCommand(ref sqlCommand, conn, tableName, fieldNameStrings, distinct, conditionFields, orderBy);
        }

        private static void SetSqlCommand(ref OleDbCommand sqlCommand, OleDbConnection conn, string tableName, string[] outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string sqlText = "SELECT ";
            if (distinct)
                sqlText += "DISTINCT ";
            for (int i = 0; i < outputFieldNames.Length; i++)
            {
                if (outputFieldNames[i].Contains("(") && outputFieldNames[i].Contains(")"))
                    sqlText += outputFieldNames[i] + ", ";
                else
                    sqlText += "[" + outputFieldNames[i] + "], ";
            }
            sqlText = DispatcherExporter.Text.GetSubstring(sqlText, null, false, ", ", true, true);
            sqlText += " FROM ";
            sqlText += "[" + tableName + "]";
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            if ((orderBy != null) && (orderBy != ""))
            {
                sqlText += " ORDER BY [" + orderBy + "]";
            }
            sqlCommand = new OleDbCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
        }

        private static void SetSqlCommand(ref OdbcCommand sqlCommand, OdbcConnection conn, string tableName, string outputFieldName,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] outputFieldNames = { outputFieldName };
            SetSqlCommand(ref sqlCommand, conn, tableName, outputFieldNames, distinct, conditionFields, orderBy);
        }

        private static void SetSqlCommand(ref OdbcCommand sqlCommand, OdbcConnection conn, string tableName, ArrayList outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string[] fieldNameStrings = Text.ArrayListToStringList(outputFieldNames);
            SetSqlCommand(ref sqlCommand, conn, tableName, fieldNameStrings, distinct, conditionFields, orderBy);
        }

        private static void SetSqlCommand(ref OdbcCommand sqlCommand, OdbcConnection conn, string tableName, string[] outputFieldNames,
            bool distinct, string[,] conditionFields, string orderBy)
        {
            string sqlText = "SELECT ";
            if (distinct)
                sqlText += "DISTINCT ";
            for (int i = 0; i < outputFieldNames.Length; i++)
            {
                if (outputFieldNames[i].Contains(" ") ||
                    outputFieldNames[i].Contains("-"))
                    sqlText += "[" + outputFieldNames[i] + "], ";
                else
                    sqlText += outputFieldNames[i] + ", ";
            }
            sqlText = DispatcherExporter.Text.GetSubstring(sqlText, null, false, ", ", true, true);
            sqlText += " FROM ";
            if (tableName.Contains(" ") ||
                tableName.Contains("-"))
                sqlText += "[" + tableName + "]";
            else
                sqlText += tableName;
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            if ((orderBy != null) && (orderBy != ""))
            {
                if (orderBy.Contains(" ") ||
                    orderBy.Contains("-"))
                    sqlText += " ORDER BY [" + orderBy + "]";
                else
                    sqlText += " ORDER BY " + orderBy;
            }
            sqlCommand = new OdbcCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
        }

        public static void SetFieldValue(OdbcConnection conn, string tableName,
            string outputFieldName, string outputFieldValue,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            SetFieldValue(conn, tableName, outputFieldName, outputFieldValue, conditionFields);
        }

        public static void SetFieldValue(OdbcConnection conn, string tableName,
            string outputFieldName, string outputFieldValue, string[,] conditionFields)
        {
            string sqlText = "UPDATE " + tableName + " SET " + outputFieldName + " = ?";
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            sqlCommand.Parameters.AddWithValue("@p0", outputFieldValue);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            sqlCommand.ExecuteNonQuery();
        }

        public static void SetFieldValue(OleDbConnection conn, string tableName,
            string outputFieldName, string outputFieldValue, string[,] conditionFields)
        {
            string sqlText = "UPDATE [" + tableName + "] SET [" + outputFieldName + "] = ?";
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            sqlCommand.Parameters.AddWithValue("@p0", outputFieldValue);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            sqlCommand.ExecuteNonQuery();
        }

        public static void SetFieldValue(OleDbConnection conn, string tableName,
            string outputFieldName, string outputFieldValue,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string sqlText = "UPDATE [" + tableName + "] SET [" + outputFieldName + "] = ?";
            if ((conditionName != null) && (conditionName != ""))
                sqlText += " WHERE [" + conditionName + "] " + conditionOperator;
            if ((conditionValue != null) && (conditionValue != ""))
                sqlText += " ?";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            sqlCommand.Parameters.AddWithValue("@p0", outputFieldValue);
            if ((conditionValue != null) && (conditionValue != ""))
                sqlCommand.Parameters.AddWithValue("@p1", conditionValue);
            sqlCommand.ExecuteNonQuery();
        }

        public static void SetFieldValue(OleDbConnection conn, string tableName,
            string outputFieldName, string outputFieldValue)
        {
            SetFieldValue(conn, tableName, outputFieldName, outputFieldValue, null, null, null);
        }

        public static string[] GetTableNames(OleDbConnection connection, bool ifExcel)
        {
            List<string> tableNames = new List<string>();
            DataTable dt = connection.GetSchema("Tables");
            foreach (DataRow row in dt.Rows)
            {
                string tableName = row["TABLE_NAME"].ToString();
                tableNames.Add(tableName);
            }
            return tableNames.ToArray();
        }

        public static bool TableExists(OleDbConnection conn, string tableName, bool ifExcel)
        {
            string[] tableNames = GetTableNames(conn, ifExcel);
            if (ifExcel)
            {
                // Table names ends with a $, or with $'
                if (Text.StringInArray(tableNames, tableName + "$", false, false) >= 0)
                    return true;
                else if (Text.StringInArray(tableNames, tableName + "$\'", false, false) >= 0)
                    return true;
            }
            else
            {
                if (Text.StringInArray(tableNames, tableName, false, false) >= 0)
                    return true;
            }
            return false;
        }

        public static bool TableExists(OleDbConnection conn, string tableName)
        {
            return TableExists(conn, tableName, false);
        }

        /*
        public static bool TableExists(OleDbConnection conn, string sqlTableName)
        {
            //SQL Server compatible, not for Access & Excel
            string sqlText = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES"
                + " WHERE table_type = 'BASE TABLE'"
                + " AND table_name = ?";
            string result = ExecuteSqlQuery(conn, sqlText, sqlTableName);
            if (result != "")
                return true;
            else
                return false;
        }

        public static ArrayList GetTableList(OdbcConnection conn)
        {
            //SQL Server compatible, not for Access & Excel
            ArrayList tableList = new ArrayList();
            string sqlText = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES"
                + " WHERE table_type = 'BASE TABLE'";
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            OdbcDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    tableList.Add(reader.GetValue(i).ToString());
            }
            return tableList;
        }
         * */

        public static void GetTableList(OleDbConnection conn, ComboBox.ObjectCollection items)
        {
            ArrayList tableList = new ArrayList();
            tableList = GetTableList(conn);

            foreach (string tableName in tableList)
            {
                items.Add(tableName);
            }
        }

        public static ArrayList GetTableList(OleDbConnection conn)
        {
            //SQL Server compatible, not for Access & Excel
            ArrayList tableList = new ArrayList();
            string sqlText = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES"
                + " WHERE table_type = 'BASE TABLE'";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    tableList.Add(reader.GetValue(i).ToString());
            }
            return tableList;
        }

        public static int GetRecordCount(OleDbConnection conn, string tableName)
        {
            string sqlText = "SELECT COUNT(*) FROM [" + tableName + "]";
            string result = ExecuteSqlQuery(conn, sqlText, tableName);
            if (result != "")
                return Convert.ToInt32(result);
            else
                return 0;
        }

        public static int GetRecordCount(OleDbConnection conn, string tableName,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            return GetRecordCount(conn, tableName, conditionFields);
        }

        public static int GetRecordCount(OleDbConnection conn, string tableName, string[,] conditionFields)
        {
            string sqlText = "SELECT COUNT(*) FROM [" + tableName + "]";
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            string result = SqlCommandExecuteScalar(sqlCommand);
            if (result.Length > 0)
                return Int32.Parse(result.ToString());
            else
                return 0;
        }

        public static int GetRecordCount(OdbcConnection conn, string tableName, string[,] conditionFields)
        {
            string sqlText = "SELECT COUNT(*) FROM " + tableName;
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            string result = SqlCommandExecuteScalar(sqlCommand);
            if (result.Length > 0)
                return Int32.Parse(result.ToString());
            else
                return 0;
        }

        public static void AddTableField(OleDbConnection conn, string tableName, string fieldName, string fieldType)
        {
            string sqlText = "ALTER TABLE [" + tableName + "] ADD [" + fieldName + "] " + fieldType;
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static void AddTableField(OdbcConnection conn, string tableName, string fieldName, string fieldType)
        {
            string sqlText = "ALTER TABLE " + tableName + " ADD " + fieldName + " " + fieldType;
            ExecuteSqlNonQuery(conn, sqlText);
        }

        public static bool TableFieldExists(OleDbConnection conn, string tableName, string fieldName)
        {
            string sqlText = "SELECT TOP 1 * FROM [" + tableName + "]";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i).ToLower() == fieldName.ToLower())
                        return true;
                }
            }
            return false;
        }

        public static void GetTableFieldList(OleDbConnection conn, string tableName, ComboBox.ObjectCollection items)
        {
            ArrayList tableFieldList = GetTableFieldList(conn, tableName);

            foreach (string tableFieldName in tableFieldList)
            {
                items.Add(tableFieldName);
            }
        }

        public static ArrayList GetTableFieldList(OleDbConnection conn, string tableName)
        {
            ArrayList fieldList = new ArrayList();
            string sqlText = "SELECT TOP 1 * FROM [" + tableName + "]";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            OleDbDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            if (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    fieldList.Add(reader.GetName(i));
                }
            }
            return fieldList;
        }

        public static bool TableFieldExists(OdbcConnection conn, string tableName, string fieldName)
        {
            //to-do: potential problem, assumed dataset with at least one record
            string sqlText = "SELECT * FROM " + tableName + " LIMIT 1";
            OdbcCommand sqlCommand = new OdbcCommand(sqlText, conn);
            OdbcDataReader reader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i).ToLower() == fieldName.ToLower())
                        return true;
                }
            }
            return false;
        }

        public static int GetRecordID(OleDbConnection conn, string tableName,
            string[] conditionName, string[] conditionOperator, string[] conditionValue)
        {
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE ";
            for (int i = 0; i != conditionName.Length; i++)
            {
                sqlText += "[" + conditionName[i] + "] " + conditionOperator[i] + " ? AND ";
            }
            sqlText = sqlText.Remove(sqlText.Length - 5, 5);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != conditionName.Length; i++)
            {
                sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), conditionValue[i]);
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetRecordID(OleDbConnection conn, string tableName, string[] conditionName, string[] conditionValue)
        {
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE ";
            foreach (string fieldName in conditionName)
            {
                sqlText += "[" + fieldName + "] = ? AND ";
            }
            sqlText = sqlText.Remove(sqlText.Length - 5, 5);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != conditionName.Length; i++)
            {
                sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), conditionValue[i]);
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetRecordID(OdbcConnection conn, string tableName, string[,] conditionFields)
        {
            string result = GetFieldValue(conn, tableName, "ID", false, conditionFields, null);
            if (Text.IsInteger(result))
                return Convert.ToInt32(result);
            else
                return 0;
        }

        public static int GetRecordID(OleDbConnection conn, string tableName, string[,] tableFields)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE ";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                {
                    sqlText += "[" + tableFields[i, 0] + "] = ? AND ";
                }
            }
            sqlText = Text.StringBeforeLastTag(sqlText.Trim(), " ");
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), tableFields[i, 2]);
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetRecordID(OleDbConnection conn, string tableName, string[] fieldNames, string[] fieldValues, bool excludeEmpty)
        {
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE ";

            OleDbCommand sqlCommand = new OleDbCommand();
            sqlCommand.Connection = conn;
            for (int i = 0; i < fieldNames.Length; ++i)
            {
                if (!excludeEmpty || fieldValues[i] != "")
                {
                    sqlText += "[" + fieldNames[i] + "] = ? AND ";
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), fieldValues[i]);
                }
            }
            sqlText = Text.StringBeforeLastTag(sqlText.Trim(), " ");
            sqlCommand.CommandText = sqlText;

            object sqlResult = sqlCommand.ExecuteScalar();

            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetRecordIDNoNText(OleDbConnection conn, string tableName, string[,] tableFields)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE ";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 1].ToLower().Contains("ntext"))
                {
                    continue;
                }

                if (tableFields[i, 2] != "")
                {
                    sqlText += "[" + tableFields[i, 0] + "] = ? AND ";
                }
            }
            sqlText = Text.StringBeforeLastTag(sqlText.Trim(), " ");
            int paramNumber = 0;
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 1].ToLower().Contains("ntext"))
                {
                    continue;
                }

                paramNumber++;
                if (tableFields[i, 2] != "")
                    sqlCommand.Parameters.AddWithValue("@p" + paramNumber.ToString(), tableFields[i, 2]);
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetRecordID(OleDbConnection conn, string tableName,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE "
                + " [" + conditionName + "] " + conditionOperator + " ?";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            sqlCommand.Parameters.AddWithValue("@p0", conditionValue);
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetMaxID(OleDbConnection conn, string tableName)
        {
            string sqlText = "SELECT MAX(ID) FROM [" + tableName + "]";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return Int32.Parse(sqlResult.ToString());
            else
                return 0;
        }

        public static int GetTableMinID(OleDbConnection conn, string tableName, int minId)
        {
            return GetTableMinID(conn, tableName, minId, null);
        }

        public static int GetTableMinID(OleDbConnection conn, string tableName, int minId,
            string conditionName, string conditionOperator, string conditionValue)
        {
            string[,] conditionFields = { { conditionName, conditionOperator, conditionValue } };
            return GetTableMinID(conn, tableName, minId, conditionFields);
        }

        public static int GetTableMinID(OleDbConnection conn, string tableName, int minId, string[,] conditionFields)
        {
            string sqlText = "SELECT MIN(ID) FROM [" + tableName + "] WHERE ID >= " + minId.ToString();
            sqlText = SqlTextAddConditions(sqlText, conditionFields);
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            SqlCommandAddParameters(sqlCommand, conditionFields);
            string result = SqlCommandExecuteScalar(sqlCommand);
            if (result.Length > 0)
                return Int32.Parse(result.ToString());
            else
                return 0;
        }

        public static bool RecordExists(OleDbConnection conn, string tableName, string[,] tableFields)
        {
            int fieldLength = tableFields.GetLength(0);
            string sqlText = "SELECT TOP 1 * FROM [" + tableName + "] WHERE ";
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlText += "[" + tableFields[i, 0] + "] = ? AND ";
            }
            sqlText = Text.StringBeforeLastTag(sqlText.Trim(), " ");
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (tableFields[i, 2] != "")
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), tableFields[i, 2]);
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return true;
            else
                return false;
        }

        public static bool RecordExists(OleDbConnection conn, string tableName, string[] indexFieldNames, string[] indexFieldValues)
        {
            int fieldLength = indexFieldNames.Length;
            string sqlText = "SELECT TOP 1 * FROM [" + tableName + "] WHERE ";
            for (int i = 0; i != fieldLength; i++)
            {
                if (indexFieldNames[i] != "")
                {
                    sqlText += "[" + indexFieldNames[i] + "] = ? AND ";
                }
            }

            sqlText = Text.StringBeforeLastTag(sqlText.Trim(), " ");
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (indexFieldValues[i] != "")
                {
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), indexFieldValues[i]);
                }
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return true;
            else
                return false;
        }

        public static void GetFieldNamesAndValues(DataRow row, List<string> fieldNames, List<string> fieldValues)
        {
            DataTable table = row.Table;

            foreach (DataColumn column in table.Columns)
            {
                try
                {   // This will throw exceptions for null values 
                    if (column.ColumnName != "ID")
                    {
                        fieldValues.Add(row[column.ColumnName].ToString());
                        fieldNames.Add(column.ColumnName);
                    }
                }
                catch (Exception)
                {
                }
            }
        }

        public static string RecordExistsID(OleDbConnection conn, string tableName, string[] indexFieldNames, string[] indexFieldValues)
        {
            int fieldLength = indexFieldNames.Length;
            string sqlText = "SELECT ID FROM [" + tableName + "] WHERE ";
            for (int i = 0; i != fieldLength; i++)
            {
                if (indexFieldNames[i] != "")
                {
                    sqlText += "[" + indexFieldNames[i] + "] = ? AND ";
                }
            }

            sqlText = Text.StringBeforeLastTag(sqlText.Trim(), " ");
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            for (int i = 0; i != fieldLength; i++)
            {
                if (indexFieldValues[i] != "")
                {
                    sqlCommand.Parameters.AddWithValue("@p" + i.ToString(), indexFieldValues[i]);
                }
            }
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return sqlResult.ToString();
            else
                return "0";
        }

        public static bool RecordExists(OleDbConnection conn, string tableName,
            string conditionFieldName, string conditionOperator, string conditionFieldValue)
        {
            string sqlText = "SELECT TOP 1 [" + conditionFieldName + "] FROM [" + tableName + "] WHERE "
                + " [" + conditionFieldName + "] " + conditionOperator
                + "'" + conditionFieldValue + "'";
            //+ " ?";
            OleDbCommand sqlCommand = new OleDbCommand(sqlText, conn);
            //Special note: not to use parameters since it's causing a long time for SQL Server to parse the query
            //sqlCommand.Parameters.AddWithValue("@p0", conditionFieldValue);
            object sqlResult = sqlCommand.ExecuteScalar();
            if ((sqlResult != null) && (sqlResult.ToString() != ""))
                return true;
            else
                return false;
        }

        public void RemoveListBoxItemsInTable(ListBox listBox, OleDbConnection conn, string tableName,
            string conditionFieldName)
        {
            int i = 0;
            while (i < listBox.Items.Count)
            {
                if (RecordExists(conn, tableName, conditionFieldName, "=", listBox.Items[i].ToString()))
                    listBox.Items.RemoveAt(i);
                else
                    i++;
            }
        }

        public void RemoveListBoxItemsInTable(ListBox indexList, ListBox appendixList, OleDbConnection conn, string tableName,
            string conditionFieldName)
        {
            int i = 0;
            while (i < indexList.Items.Count)
            {
                if (RecordExists(conn, tableName, conditionFieldName, "=", indexList.Items[i].ToString()))
                {
                    indexList.Items.RemoveAt(i);
                    appendixList.Items.RemoveAt(i);
                }
                else
                    i++;
            }
        }

    }

    public class NntpException : System.ApplicationException
    {
        public NntpException(string str)
            : base(str)
        {
        }
    }

    public class Nntp : System.Net.Sockets.TcpClient
    {
        public void Connect(string server)
        {
            string response;

            Connect(server, 119);
            response = Response();
            if ((response.Substring(0, 3) != "200") && (response.Substring(0, 3) != "201"))
            {
                throw new NntpException(response);
            }
        }

        public void Disconnect()
        {
            string message;
            string response;

            message = "QUIT\r\n";
            Write(message);
            response = Response();
            if (response.Substring(0, 3) != "205")
            {
                throw new NntpException(response);
            }
        }

        public ArrayList GetNewsgroups(string keyword)
        {
            string message;
            string response;

            ArrayList retval = new ArrayList();

            message = "LIST\r\n";
            Write(message);
            response = Response();
            if (response.Substring(0, 3) != "215")
            {
                throw new NntpException(response);
            }

            while (true)
            {
                response = Response();
                if (response == ".\r\n" ||
                    response == ".\n")
                {
                    return retval;
                }
                else
                {
                    char[] seps = { ' ' };
                    string[] values =
                        response.Split(seps);
                    if ((keyword == "") || (values[0].IndexOf(keyword) >= 0))
                        retval.Add(values[0]);
                    continue;
                }
            }
        }

        public ArrayList GetArticles(string newsgroup)
        {
            string message;
            string response;

            ArrayList retval = new ArrayList();

            message = "GROUP " + newsgroup + "\r\n";
            Write(message);
            response = Response();
            if (response.Substring(0, 3) != "211")
            {
                throw new NntpException(response);
            }

            char[] seps = { ' ' };
            string[] values = response.Split(seps);

            long start = Int32.Parse(values[2]);
            long end = Int32.Parse(values[3]);

            for (long i = start; i < end; i++)
            {
                message = "ARTICLE " + i + "\r\n";
                Write(message);
                response = Response();
                if (response.Substring(0, 3) == "423")
                {
                    continue;
                }
                if (response.Substring(0, 3) != "220")
                {
                    throw new NntpException(response);
                }

                string article = "";
                while (true)
                {
                    response = Response();
                    if (response == ".\r\n")
                    {
                        break;
                    }

                    if (response == ".\n")
                    {
                        break;
                    }

                    if (article.Length < 1024)
                    {
                        article += response;
                    };
                }

                retval.Add(article);
            }

            return retval;
        }

        public void GetNewsgroupEmails(string newsgroup, ListBox listBox, ProgressBar progressBar)
        {
            string message;
            string response;

            message = "GROUP " + newsgroup + "\r\n";
            Write(message);
            response = Response();
            if (response.Substring(0, 3) != "211")
            {
                throw new NntpException(response);
            }

            char[] seps = { ' ' };
            string[] values = response.Split(seps);

            int start = Int32.Parse(values[2]);
            int end = Int32.Parse(values[3]);
            progressBar.Maximum = end - start;
            progressBar.Value = 0;

            for (int i = start; i < end; i++)
            {
                progressBar.Value = i - start + 1;
                Application.DoEvents();
                message = "ARTICLE " + i + "\r\n";
                Write(message);
                response = Response();
                if (response.Substring(0, 3) == "423")
                {
                    continue;
                }
                if (response.Substring(0, 3) != "220")
                {
                    throw new NntpException(response);
                }

                int count = 0;
                while (true)
                {
                    response = Response();
                    if (response == ".\r\n")
                    {
                        break;
                    }

                    if (response == ".\n")
                    {
                        break;
                    }

                    count++;
                    if (count < 1024)
                    {
                        /*if (isBody)
                        {
                            if (response.IndexOf("@") >= 0)
                                iText.GetEmails(response, listBox, true);
                        }
                        else
                        {
                            if ((response.IndexOf("From:") >= 0) && (response.IndexOf("@") >= 0))
                                iText.GetEmails(response, listBox, true);
                            if (response == "\r\n")
                                isBody = true;
                        }*/
                    }
                }
            }
        }

        //MyText iText = new MyText();

        public void Post(string newsgroup, string subject,
            string from, string content)
        {
            string message;
            string response;

            message = "POST " + newsgroup + "\r\n";
            Write(message);
            response = Response();
            if (response.Substring(0, 3) != "340")
            {
                throw new NntpException(response);
            }

            message = "From: " + from + "\r\n"
                + "Newsgroups: " + newsgroup + "\r\n"
                + "Subject: " + subject + "\r\n\r\n"
                + content + "\r\n.\r\n";
            Write(message);
            response = Response();
            if (response.Substring(0, 3) != "240")
            {
                throw new NntpException(response);
            }
        }

        private void Write(string message)
        {
            System.Text.ASCIIEncoding en =
                new System.Text.ASCIIEncoding();

            byte[] WriteBuffer = new byte[1024];
            WriteBuffer = en.GetBytes(message);

            System.Net.Sockets.NetworkStream stream = GetStream();
            stream.Write(WriteBuffer, 0, WriteBuffer.Length);
        }

        private string Response()
        {
            System.Text.ASCIIEncoding enc =
                new System.Text.ASCIIEncoding();
            byte[] serverbuff = new Byte[1024];
            System.Net.Sockets.NetworkStream stream = GetStream();
            int count = 0;
            while (true)
            {
                byte[] buff = new Byte[2];
                int bytes = stream.Read(buff, 0, 1);
                if (bytes == 1)
                {
                    serverbuff[count] = buff[0];
                    count++;

                    if (buff[0] == '\n')
                    {
                        break;
                    }
                }
                else
                {
                    break;
                };
            };

            string retval =
                enc.GetString(serverbuff, 0, count);
            return retval;
        }

    }

    public abstract class Web
    {
        const string WebProxyUrlSuffix = "index.php?hl=120&q=";
        public const string IPCheckingUrl = "http://www.ficstar.com/ip.php";

        public static string SetWebProxyUrl(string url)
        {
            string result = url;
            if (!result.ToLower().StartsWith("http"))
                result = "http://" + result;
            if (!result.EndsWith("/"))
                result += "/";
            result += WebProxyUrlSuffix;
            return result;
        }

        public static void SendEmail(string from, string body, List<string> to, string subject, string userName, string password, string smtpServer)
        {
            if (to.Count <= 0) { return; }

            MailMessage message = new MailMessage(from, to[0], subject, body);

            for (int i = 1; i < to.Count; ++i)
            {
                message.To.Add(to[i]);
            }

            SmtpClient emailClient = new SmtpClient(smtpServer);

            emailClient.UseDefaultCredentials = false;
            emailClient.Credentials = new System.Net.NetworkCredential(userName, password);
            emailClient.EnableSsl = true;
            emailClient.Send(message);
        }

        public static void SendHtmlEmail(string from, string htmlBody, List<string> to, string subject, string userName, string password, string smtpServer)
        {
            if (to.Count <= 0) { return; }

            MailMessage message = new MailMessage(from, to[0], subject, htmlBody);
            message.IsBodyHtml = true;

            for (int i = 1; i < to.Count; ++i)
            {
                message.To.Add(to[i]);
            }

            SmtpClient emailClient = new SmtpClient(smtpServer);

            emailClient.EnableSsl = true;
            emailClient.UseDefaultCredentials = false;
            emailClient.Port = 587;
            emailClient.Credentials = new System.Net.NetworkCredential(userName, password);
            
            emailClient.Send(message);
        }
        public static void SendHtmlEmailWithAttachment(string from, string htmlBody, List<string> to, string subject, string userName, string password, string smtpServer, string fileLoc)
        {
            if (to.Count <= 0) { return; }

            MailMessage message = new MailMessage(from, to[0], subject, htmlBody);
            message.IsBodyHtml = true;

            Attachment atc = new Attachment(fileLoc);
            message.Attachments.Add(atc);

            for (int i = 1; i < to.Count; ++i)
            {
                message.To.Add(to[i]);
            }
            SmtpClient emailClient = new SmtpClient(smtpServer);
            emailClient.EnableSsl = true;
            emailClient.UseDefaultCredentials = false;
            emailClient.Port = 587;
            emailClient.Credentials = new System.Net.NetworkCredential(userName, password);
            emailClient.Send(message);
            atc.Dispose();
            message.Dispose();            
        }

        public static void SendHtmlEmailWithAttachment(string from, string htmlBody, List<string> to, string subject,
            string userName, string password, string smtpServer, List<string> fileLoc)
        {
            if (to.Count <= 0) { return; }

            MailMessage message = new MailMessage(from, to[0], subject, htmlBody);
            message.IsBodyHtml = true;

            Attachment atc = null;
            foreach (string file in fileLoc)
            {
                if (File.Exists(file)) { atc = new Attachment(file); message.Attachments.Add(atc); }
            }

            for (int i = 1; i < to.Count; ++i)
            {
                message.To.Add(to[i]);
            }
            SmtpClient emailClient = new SmtpClient(smtpServer);
            emailClient.EnableSsl = true;
            emailClient.UseDefaultCredentials = false;
            emailClient.Port = 587;
            emailClient.Credentials = new System.Net.NetworkCredential(userName, password);
            emailClient.Send(message);
            atc.Dispose();
            message.Dispose();
        }

        public static string RemoveWebProxyFromUrl(string url)
        {
            return Text.GetSubstring(url, WebProxyUrlSuffix, null);
        }

        public static void RemoveWebProxy(ListBox listBox)
        {
            if (listBox != null)
            {
                ListBox tempList = new ListBox();
                for (int i = 0; i < listBox.Items.Count; i++)
                {
                    tempList.Items.Add(Text.GetSubstring(listBox.Items[i].ToString(), "q=", null, true, true));
                }
                Text.CopyListBox(tempList, listBox);
            }
        }

        public static bool WebFileExists(string url)
        {
            try
            {
                WebClient client = new WebClient();
                Byte[] responseBytes = client.DownloadData(url);
                return true;
            }
            catch (Exception ex)
            {
                string st = ex.Message;
                return false;
            }
        }

        public static void DownloadFile(string url, string fileName)
        {
            DownloadFile(url, fileName, "", true);
        }

        public static void DownloadFile(string url, string fileName, string proxy)
        {
            DownloadFile(url, fileName, proxy, true);
        }

        public static void DownloadFile(string url, string fileName, bool replaceExisting)
        {
            DownloadFile(url, fileName, "", replaceExisting);
        }

        public static void DownloadFile(string url, string fileName, string proxy, bool replaceExisting)
        {
            if (replaceExisting || (!File.Exists(fileName)))
            {
                WebClient client = new WebClient();
                if ((proxy != null) && (proxy != ""))
                    client.Proxy = new WebProxy(proxy);
                client.DownloadFile(url, fileName);
            }
        }

        public static bool DownloadFTPFile(string url, string fileName)
        {
            try
            {
                if (url.ToLower().Contains("ftp://"))
                {
                    string uri = url;
                    Uri serverUri = new Uri(uri);
                    if (serverUri.Scheme != Uri.UriSchemeFtp)
                    {
                        return false;
                    }
                    FtpWebRequest reqFTP;
                    reqFTP = (FtpWebRequest)FtpWebRequest.Create(uri);
                    reqFTP.KeepAlive = false;
                    reqFTP.Method = WebRequestMethods.Ftp.DownloadFile;
                    reqFTP.UseBinary = true;
                    reqFTP.Proxy = null;
                    reqFTP.UsePassive = false;
                    FtpWebResponse response = (FtpWebResponse)reqFTP.GetResponse();
                    Stream responseStream = response.GetResponseStream();
                    FileStream writeStream = new FileStream(fileName, FileMode.Create);
                    int Length = 2048;
                    Byte[] buffer = new Byte[Length];
                    int bytesRead = responseStream.Read(buffer, 0, Length);
                    while (bytesRead > 0)
                    {
                        writeStream.Write(buffer, 0, bytesRead);
                        bytesRead = responseStream.Read(buffer, 0, Length);
                    }
                    writeStream.Close();
                    response.Close();
                    return true;
                }
                else
                {
                    DownloadFile(url, fileName);
                    return true;
                }
            }
            catch (WebException wEx)
            {
                MessageBox.Show(wEx.Message, "Download Error");
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Download Error");
            }

            return false;
        }

        public void GetUrl(HtmlDocument doc, ListBox list)
        {
            if ((doc != null) && (doc.Body != null) && (doc.Body.InnerText != null))
                Text.GetUrl(doc.Body.InnerText, list);
        }

        public void GetProxy(HtmlDocument doc, ListBox list)
        {
            if ((doc != null) && (doc.Body != null) && (doc.Body.InnerText != null))
                Text.GetProxy(doc.Body.InnerText, list);
        }

        public static string GetInnerText(string htmlString, int textIndex)
        {
            string s = htmlString;
            if ((s != null) && (s.Length > 0))
            {
                while (s.Contains(">") && s.Contains("<") && (s.IndexOf(">") < s.IndexOf("<")))
                    s = Text.GetSubstring(s, ">", null);
                if (s.Length > 0)
                {
                    for (int i = 0; i < textIndex; i++)
                    {
                        while ((s.Length > 0) && (s[0] == Convert.ToChar("<")))
                            s = Text.GetSubstring(s, ">", null);
                        if (!s.Contains("<") && s.Contains(">"))
                            s = Text.GetSubstring(s, ">", null);
                        s = Text.GetSubstring(s, ">", null); ;
                    }
                    while ((s.Length > 0) && (s[0] == Convert.ToChar("<")))
                        s = Text.GetSubstring(s, ">", null);
                    if (!s.Contains("<") && s.Contains(">"))
                        s = Text.GetSubstring(s, ">", null);
                    if (s.Contains("<"))
                        s = Text.GetSubstring(s, null, "<");
                }
            }
            return s;
        }

        public static string GetInnerText(string htmlString, string startText, bool ignoreCase, int textIndex)
        {
            string result = "";
            if (htmlString.Contains(startText) || (ignoreCase && (htmlString.ToLower().Contains(startText.ToLower()))))
            {
                //assume textIndex > 0
                result = Text.GetSubstring(htmlString, startText, null, ignoreCase);
                result = GetInnerText(result, textIndex);
            }
            return result;
        }

        public static string GetInnerText(string htmlString, string startText, int textIndex)
        {
            return GetInnerText(htmlString, startText, true, textIndex);
        }

        public string GetInnerText(HtmlDocument doc, string startText, int textIndex)
        {
            string str = "";
            HtmlElementCollection elementCollection = doc.All;
            for (int i = 0; i < elementCollection.Count; i++)
            {
                if (elementCollection[i].InnerText == startText)
                {
                    str = elementCollection[i + textIndex + 1].InnerText;
                    break;
                }
            }
            return str;
        }

        public string GetInnerHtml(HtmlDocument doc, string startText, int textIndex)
        {
            string str = "";
            HtmlElementCollection elementCollection = doc.All;
            for (int i = 0; i < elementCollection.Count; i++)
            {
                if (elementCollection[i].InnerText == startText)
                {
                    str = elementCollection[i + textIndex + 1].InnerHtml;
                    break;
                }
            }
            return str;
        }

        public void GetCategoryList(HtmlDocument doc, ListBox[,] list, int depth, string[] linkTags, string[] invalidTags,
            string linkPrefix, string linkSuffix, bool clearList)
        {
            if (clearList)
            {
                list[depth, 0].Items.Clear();
                list[depth, 1].Items.Clear();
            }
            GetLinks(doc, list[depth, 0], list[depth, 1], linkTags, invalidTags, true);
            if ((linkPrefix != "") || (linkSuffix != ""))
                Text.ItemsAddText(list[depth, 0], linkPrefix, linkSuffix);
        }

        public struct Struct_INTERNET_PROXY_INFO
        {
            public int dwAccessType;
            public IntPtr proxy;
            public IntPtr proxyBypass;
        };

        [DllImport("wininet.dll", SetLastError = true)]
        private static extern bool InternetSetOption(IntPtr hInternet, int dwOption, IntPtr lpBuffer, int lpdwBufferLength);

        public static void RefreshIESettings(string strProxy)
        {
            const int INTERNET_OPTION_PROXY = 38;
            const int INTERNET_OPEN_TYPE_PROXY = 3;

            Struct_INTERNET_PROXY_INFO struct_IPI;

            // Filling in structure 
            struct_IPI.dwAccessType = INTERNET_OPEN_TYPE_PROXY;
            struct_IPI.proxy = Marshal.StringToHGlobalAnsi(strProxy);
            struct_IPI.proxyBypass = Marshal.StringToHGlobalAnsi("local");

            // Allocating memory 
            IntPtr intptrStruct = Marshal.AllocCoTaskMem(Marshal.SizeOf(struct_IPI));

            // Converting structure to IntPtr 
            Marshal.StructureToPtr(struct_IPI, intptrStruct, true);

            bool iReturn = InternetSetOption(IntPtr.Zero, INTERNET_OPTION_PROXY, intptrStruct, Marshal.SizeOf(struct_IPI));
        }
       
        public static bool TextInChildElement(HtmlElement element, string text, bool ignoreCase)
        {
            HtmlElementCollection elementCollection = element.All;
            for (int i = 1; i < elementCollection.Count; i++)
            {
                HtmlElement child = elementCollection[i] as HtmlElement;
                if ((child.InnerText != null) &&
                    (child.InnerText != "") &&
                    (element.TagName == child.TagName) &&
                    ((child.InnerText.Contains(text)) ||
                    ((ignoreCase) && child.InnerText.ToLower().Contains(text.ToLower()))))
                {
                    return true;
                }
            }
            return false;
        }

       

        public static void GetLinks(HtmlDocument doc, ListBox linkList, ListBox nameList,
            string linkTag, string invalidTag, string linkStringStartTag, string linkStringEndTag, bool noDuplicate)
        {
            foreach (HtmlElement hrefElement in doc.Links)
            {
                string str = hrefElement.GetAttribute("HREF");
                str = Text.GetSubstring(str, linkStringStartTag, false, linkStringEndTag, false, false);
                if (((linkTag == null) || (linkTag == "") || (str.Contains(linkTag)))
                    && ((invalidTag == null) || (invalidTag == "") || (!str.Contains(invalidTag)))
                    && (!noDuplicate || (!linkList.Items.Contains(str))))
                {
                    linkList.Items.Add(str);
                    if (nameList != null)
                    {
                        str = hrefElement.InnerText;
                        if (str == null)
                            str = "";
                        nameList.Items.Add(str);
                    }
                }
                Application.DoEvents();
            }
        }

        public static void GetLinks(HtmlDocument doc, ListBox linkList, string linkTag, string[] invalidTags, bool noDuplicate)
        {
            foreach (HtmlElement hrefElement in doc.Links)
            {
                string str = "";
                try
                {
                    str = hrefElement.GetAttribute("HREF");
                }
                catch
                {
                }
                if ((str != "") && ((linkTag == "") || (str.Contains(linkTag))))
                {
                    if (str.Contains("#"))
                        str = str.Remove(str.IndexOf("#"));
                    if ((str != doc.Url.ToString()) &&
                        (!noDuplicate || (!linkList.Items.Contains(str))) &&
                        (!Text.StringContainsArrayItem(str, invalidTags, true)))
                    {
                        linkList.Items.Add(str);
                    }
                }
                Application.DoEvents();
            }
        }

        public static void GetLinks(HtmlDocument doc, ListBox linkList, ListBox nameList, string linkTag, string[] invalidTags, bool noDuplicate)
        {
            foreach (HtmlElement hrefElement in doc.Links)
            {
                string str = hrefElement.GetAttribute("HREF");
                if ((linkTag == "") || (str.Contains(linkTag)))
                {
                    if (str.Contains("#"))
                        str = str.Remove(str.IndexOf("#"));
                    if ((str != doc.Url.ToString()) &&
                        (!noDuplicate || (!linkList.Items.Contains(str))) &&
                        (!Text.StringContainsArrayItem(str, invalidTags, true)))
                    {
                        linkList.Items.Add(str);
                        str = hrefElement.InnerText;
                        if (str == null)
                            str = "";
                        nameList.Items.Add(str);
                    }
                }
                Application.DoEvents();
            }
        }

        public static void GetLinks(HtmlDocument doc, ListBox linkList, ListBox nameList, string[] linkTags,
            string[] invalidTags, bool noDuplicate)
        {
            foreach (HtmlElement hrefElement in doc.Links)
            {
                string str = hrefElement.GetAttribute("HREF");
                if (Text.StringContainsTags(str, linkTags))
                {
                    if (str.Contains("#"))
                        str = str.Remove(str.IndexOf("#"));
                    if ((str != doc.Url.ToString()) &&
                        (!noDuplicate || (!linkList.Items.Contains(str))) &&
                        (!Text.StringContainsArrayItem(str, invalidTags, true)))
                    {
                        linkList.Items.Add(str);
                        str = hrefElement.InnerText;
                        if (str == null)
                            str = "";
                        nameList.Items.Add(str);
                    }
                }
                Application.DoEvents();
            }
        }

        public static void GetLinks(HtmlDocument doc, ListBox linkList, string linkTag, string[] invalidTags, bool invalidCaseSensitive,
            bool noDuplicate, OleDbConnection conn, string visitedTableName, string visitedFieldName)
        {
            foreach (HtmlElement hrefElement in doc.Links)
            {
                string str = hrefElement.GetAttribute("HREF");
                if ((linkTag == "") || (str.Contains(linkTag)))
                {
                    if (str.Contains("#"))
                        str = str.Remove(str.IndexOf("#"));
                    if ((str != doc.Url.ToString()) &&
                        (!noDuplicate || (!linkList.Items.Contains(str))) &&
                        (!Text.StringContainsArrayItem(str, invalidTags, true)))
                    {
                        if (!Database.RecordExists(conn, visitedTableName, visitedFieldName, "=", str))
                            linkList.Items.Add(str);
                    }
                }
                Application.DoEvents();
            }
        }

        public string GetLinksFromHtmlElementAttributeValues(HtmlDocument doc, ListBox linkList, ListBox nameList,
            string tagName, string attributeName, string linkTag, bool noDuplicate, bool insideQuotes, bool innerTextNotNull)
        {
            string str;
            foreach (HtmlElement element in doc.GetElementsByTagName(tagName))
            {
                str = element.GetAttribute(attributeName);
                if ((linkTag == "") || (str.ToLower().Contains(linkTag.ToLower())))
                {
                    if (str.Contains("#"))
                        str = str.Remove(str.IndexOf("#"));
                    if (str.ToLower().Contains("href="))
                    {
                        str = Text.GetSubstring(str, "href=", false, null, false, false);
                        str = Text.RemoveStartEndQuotes(str);
                    }
                    if (insideQuotes && (str.Contains("'") || str.Contains("\"")))
                        str = Text.GetTextInsideQuotes(str);
                    str = ValidateURL(doc, str);
                    if ((str != doc.Url.ToString()) &&
                        (!noDuplicate || (!linkList.Items.Contains(str))))
                    {
                        linkList.Items.Add(str);
                        str = element.InnerText;
                        if (str == null)
                            str = "";
                        str = str.Trim();
                        if (innerTextNotNull && (str == ""))
                            linkList.Items.RemoveAt(linkList.Items.Count - 1);
                        else
                            nameList.Items.Add(str);
                    }
                }
                Application.DoEvents();
            }
            return "";
        }

        public static string ValidateURL(HtmlDocument doc, string url)
        {
            if (!url.StartsWith("javascript:", true, null))
            {
                if ((url != null) && (url != "") &&
                    (url.Substring(0, doc.Url.Scheme.Length + 3).ToLower() != (doc.Url.Scheme.ToLower() + "://")))
                {
                    if (url.StartsWith("//"))
                        return doc.Url.Scheme + ":" + url;
                    else if (url.StartsWith("/"))
                        return doc.Url.Scheme + "://" + doc.Url.Host + url;
                    else if (url.StartsWith("?"))
                        return Text.GetSubstring(doc.Url.ToString(), null, false, "?", false, true) + url;
                    else if (url.StartsWith("./"))
                        return GetPageUrlPath(doc) + url.Substring(2);
                    else if (url.StartsWith("../"))
                        return GetPageUrlPath(doc, 1) + url.Substring(3);
                    else
                        return GetPageUrlPath(doc) + url;
                }
            }
            return url;
        }

        static string GetPageUrlPath(HtmlDocument doc)
        {
            return GetPageUrlPath(doc, 0);
        }

        static string GetPageUrlPath(HtmlDocument doc, int levelUp)
        {
            string str = doc.Url.ToString();
            if (!str.EndsWith("/"))
            {
                string temp = Text.GetSubstring(str, "//", false, null, false, true);
                if (temp.Contains("/"))
                    str = Text.StringBeforeLastTag(str, "/");
                str += "/";
            }
            if (levelUp > 0)
            {
                str = str.Remove(str.Length - 1);   //remove last char "/"
                string temp = Text.GetSubstring(str, "//", false, null, false, true);
                if (temp.Contains("/"))
                    str = Text.StringBeforeLastTag(str, "/");
                str += "/";
            }
            return str;
        }

        public string GetMetaContent(HtmlDocument doc, string tagName)
        {
            string result = "";
            foreach (HtmlElement MetaElement in doc.GetElementsByTagName("META"))
            {
                if (MetaElement.GetAttribute("Name").ToLower() == tagName.ToLower())
                {
                    result = MetaElement.GetAttribute("Content");
                    break;
                }
            }
            return result;
        }

        public bool HtmlElementExists(HtmlDocument doc, string elementId)
        {
            foreach (HtmlElement element in doc.All)
            {
                if ((element.Id != null) && (element.Id.ToLower() == elementId.ToLower()))
                {
                    return true;
                }
            }
            return false;
        }

        public static void SetHtmlElementValue(HtmlDocument doc, string tagName, string elementName, string value)
        {
            foreach (HtmlElement element in doc.GetElementsByTagName(tagName))
            {
                if (element.Name.ToLower() == elementName.ToLower())
                {
                    element.SetAttribute("value", value);
                    break;
                }
            }
        }

        public static void SetHtmlElementValue(HtmlDocument doc, string id, string value)
        {
            HtmlElement element = doc.GetElementById(id);

            if (element != null)
            {
                element.SetAttribute("value", value);
            }
        }

        public static string GetHtmlElementInnerText(HtmlDocument doc, string tagName, int elementIndex)
        {
            return GetHtmlElementInnerText(doc, tagName, null, null, elementIndex);
        }

        public static string GetHtmlElementInnerText(HtmlDocument doc, string tagName, string attributeName, string attributeValue)
        {
            return GetHtmlElementInnerText(doc, tagName, attributeName, attributeValue, 0);
        }

        public static string GetHtmlElementInnerText(HtmlDocument doc, string tagName,
            string attributeName, string attributeValue, int elementIndex)
        {
            int count = 0;
            foreach (HtmlElement element in doc.GetElementsByTagName(tagName))
            {
                if ((attributeName != null) && (attributeName != "") && (attributeValue != null))
                {
                    string str = element.GetAttribute(attributeName);
                    if ((attributeValue != null) && (str.ToLower() == attributeValue.ToLower()))
                    {
                        if ((elementIndex >= 0) && (count == elementIndex))
                            return element.InnerText;
                        else
                            count++;
                    }
                }
                else
                {
                    if ((elementIndex >= 0) && (count == elementIndex))
                        return element.InnerText;
                    else
                        count++;
                }
            }
            return "";
        }

        public static int GetHtmlElementCount(HtmlDocument doc, string tagName)
        {
            return GetHtmlElementCount(doc, tagName, null, null);
        }

        public static int GetHtmlElementCount(HtmlDocument doc, string tagName, string attributeName, string attributeValue)
        {
            int count = 0;
            foreach (HtmlElement element in doc.GetElementsByTagName(tagName))
            {
                if ((attributeName != null) && (attributeName != "") && (attributeValue != null))
                {
                    string str = element.GetAttribute(attributeName);
                    if ((attributeValue != null) && (str.ToLower() == attributeValue.ToLower()))
                    {
                        count++;
                    }
                }
                else
                {
                    count++;
                }
            }
            return count;
        }

        public static bool ClickHtmlLIElement(HtmlDocument doc, int elementIndex)
        {
            /*
            HtmlElementCollection elements = doc.GetElementsByTagName("LI");
            for (int i = 0; i < elements.Count; i++)
            {
                mshtml.IHTMLLIElement element = (mshtml.IHTMLLIElement)elements[i].DomElement;
                if (elementIndex == i)
                {
                    element.click();
                    return true;
                }
            }*/
            return false;
        }

        

        


        

        /// <summary>
        /// Clicks an image found on the doc with image source where imageSource is a substring.
        /// </summary>
        /// <param name="imageSource">Partial image source string.</param>
       

        

        public static bool TestWebProxy(string url)
        {
            string html = GetHtml(url, null, 0, 30, null, null);
            if (html != "")
                return true;
            else
                return false;
        }

        public static void SubmitHtmlForm(HtmlDocument doc, string formName)
        {
            foreach (HtmlElement form in doc.Forms)
            {
                if (form.Name.ToLower() == formName.ToLower())
                {
                    form.InvokeMember("submit");
                    break;
                }
            }
        }

        public static void SubmitHtmlForm(HtmlDocument doc, int formIndex)
        {
            if (doc.Forms.Count > formIndex)
                doc.Forms[formIndex].InvokeMember("submit");
        }

        public string GetPageCharset(HtmlDocument doc)
        {
            string result = "";
            string str;
            foreach (HtmlElement MetaElement in doc.GetElementsByTagName("META"))
            {
                str = MetaElement.GetAttribute("name").ToLower();
                if (str == "")
                {
                    str = MetaElement.GetAttribute("content").ToLower();
                    if (str.ToLower().Contains("charset="))
                    {
                        result = Text.GetSubstring(str, "charset=", false, null, false, true);
                        break;
                    }
                }
            }
            return result;
        }

        public static string GetMyIP()
        {
            string result = "";
            string html = GetHtml(ConstantValues.UrlShowMyIP);
            if (html != "")
                result = Text.GetSubstring(html, "My IP Address is:", "<").Trim();
            return result;
        }

        public static string GetHtmlHeadersValue(string URL, string HtmlHeaderKey)
        {
            string HtmlHeaderValue = "";
            Dictionary<string, string> headerDetails = Web.GetHtmlHeaders(URL);
            if (headerDetails.ContainsKey(HtmlHeaderKey))
            {
                HtmlHeaderValue = headerDetails[HtmlHeaderKey];
            }
            return HtmlHeaderValue;
        }

        public static Dictionary<string, string> GetHtmlHeaders(string URL)
        {
            Dictionary<string, string> headerDetails = new Dictionary<string, string>();
            try
            {
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(URL);
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                for (int i = 0; i < response.Headers.Count; i++)
                {
                    headerDetails.Add(response.Headers.GetKey(i), response.Headers.Get(i));
                }
            }
            catch (Exception)
            {
            }
            return headerDetails;
        }

        public static string GetHtml(string URL)
        {
            return GetHtml(URL, null, 0, 0, null, null);
        }

        public static string GetHtml(string URL, string proxyHost, int proxyPort, int requestTimeoutInSecond)
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(URL);
                if ((proxyHost != null) && (proxyHost != "") && (proxyPort != 0))
                    request.Proxy = new System.Net.WebProxy(proxyHost, proxyPort);
                if (requestTimeoutInSecond != 0)
                    request.Timeout = requestTimeoutInSecond * 1000;

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                StreamReader stream = new StreamReader(response.GetResponseStream(), UTF8Encoding.UTF8);
                string result = stream.ReadToEnd();
                stream.Close();
                response.Close();
                return result;
            }
            catch (Exception) { }

            return "";
        }

        public static string GetHtml(string URL, string proxyHost, int proxyPort, int requestTimeoutInSecond,
            string proxyUserName, string proxyPassword)
        {
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(URL);
            if ((proxyHost != null) && (proxyHost != "") && (proxyPort != 0))
                request.Proxy = new System.Net.WebProxy(proxyHost, proxyPort);
            if (requestTimeoutInSecond != 0)
                request.Timeout = requestTimeoutInSecond * 1000;
            if ((proxyUserName != null) && (proxyUserName != "") && (proxyPassword != null) && (proxyPassword != ""))
            {
                System.Net.NetworkCredential credential = new NetworkCredential();
                credential.UserName = "trial-globalsources";
                credential.Password = "Ush1geeb";
                request.Proxy.Credentials = credential;
            }
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            StreamReader stream = new StreamReader(response.GetResponseStream(), UTF8Encoding.UTF8);
            string result = stream.ReadToEnd();
            stream.Close();
            response.Close();
            return result;
        }

        public string GetHtml1(HttpWebRequest request)
        {
            //this function has a problem getting a chuck of duplicate inner html source codes
            //above method using stream.ReadToEnd() solves this problem

            string result = "";
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            System.IO.Stream stream = response.GetResponseStream();
            System.Text.Encoding ec = System.Text.Encoding.GetEncoding("utf-8");
            System.IO.StreamReader reader = new System.IO.StreamReader(stream, ec);
            //System.IO.StreamReader reader = new System.IO.StreamReader(stream);
            const int blockSize = 1024;
            char[] chars = new Char[blockSize];
            int count = reader.Read(chars, 0, blockSize);
            while (count > 0)
            {
                string str = new String(chars, 0, blockSize);
                result = result + str;
                count = reader.Read(chars, 0, blockSize);
            }
            response.Close();
            stream.Close();
            reader.Close();
            return result;
        }

        public string GetHtml1(string URL, string proxyHost, int proxyPort, int requestTimeoutInSecond)
        {
            string result = "";
            WebRequest request = WebRequest.Create(URL);
            if ((proxyHost != "") && (proxyPort != 0))
            {
                WebProxy proxy = new System.Net.WebProxy(proxyHost, proxyPort);
                request.Proxy = proxy;
            }
            if (requestTimeoutInSecond != 0)
                request.Timeout = requestTimeoutInSecond * 1000;
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            System.IO.Stream stream = response.GetResponseStream();
            System.Text.Encoding ec = System.Text.Encoding.GetEncoding("utf-8");
            System.IO.StreamReader reader = new System.IO.StreamReader(stream, ec);
            //System.IO.StreamReader reader = new System.IO.StreamReader(stream);
            const int blockSize = 1024;
            char[] chars = new Char[blockSize];
            int count = reader.Read(chars, 0, blockSize);
            while (count > 0)
            {
                string str = new String(chars, 0, blockSize);
                result = result + str;
                count = reader.Read(chars, 0, blockSize);
            }
            response.Close();
            stream.Close();
            reader.Close();
            return result;
        }

    }

}

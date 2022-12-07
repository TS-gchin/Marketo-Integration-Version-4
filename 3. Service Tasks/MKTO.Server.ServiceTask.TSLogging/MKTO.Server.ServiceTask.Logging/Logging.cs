using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using CdcSoftware.Pivotal.Applications.Core.Common;
using CdcSoftware.Pivotal.Applications.Core.Data.Element;
using CdcSoftware.Pivotal.Applications.Core.Server;

using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.Types.ServerTasks;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.Web.Configuration;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Transactions;

namespace MKTO.Server.ServiceTask
{
    /// <summary>
    /// The service server task class to undertake some specfic business tasks for the Specified Business object group. All methods
    /// can be directly called by other Form Server Tasks or remotely called by Client Tasks through client proxy classes.
    /// </summary>
    /// <history>
    /// #Revision   Date    Author  Description
    /// </history>
    public class Logging : AbstractApplicationServerTask
    {
        enum LogType  {Undefined, WriteToFile,WriteToDatabase };
        bool LogEnable = false;
        LogType logType = LogType.Undefined;
        string connectionString = "";
        string logFilePath = "";
        string logTableName = "";

        #region Constructor
        /// <summary>
        /// Initialize the instance of AppServiceServerTask class and set the default resource bundle to 'xxxxxx' LD Group.
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public Logging()
        {
            //base.DefaultResourceBundleName = "xxxxxxx";
        }
        #endregion

        #region Public methods
        /// <summary>
        /// 
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual void WriteToLog(Id integrationId, string messageToLog, EventLogEntryType palEventType, string tableName, string recordId, string className, string methodName, string otherInformation)
        {
            if (logType == LogType.Undefined)
            {
                // Retrieve logging information
                DataRow drIntegration = this.DefaultDataAccess.GetDataRow("Marketo_Configuration", integrationId,
                    new string[] { "Log_DB_Conn_String", "Log_DB_Table_Name", "Log_File_Location", "Log_Type", "Log_Enable" });

                if (drIntegration != null)
                {
                    //Check to see if logging is enabled
                    if (TypeConvert.ToBoolean(drIntegration["Log_Enable"]))
                    {
                        //Determine which type of logging is enabled
                        switch (TypeConvert.ToString(drIntegration["Log_Type"]).ToLower())
                        {
                            case "file":
                                logType = LogType.WriteToFile;
                                logFilePath = TypeConvert.ToString(drIntegration["Log_File_Location"]);
                                string logLine = GetLogLine(messageToLog, palEventType, tableName, recordId, className, methodName, otherInformation);
                                WriteLogToFile(logFilePath,logLine);
                                break;
                            case "database":
                                logType = LogType.WriteToDatabase;
                                logTableName = TypeConvert.ToString(drIntegration["Log_DB_Table_Name"]);
                                connectionString = TypeConvert.ToString(drIntegration["Log_DB_Conn_String"]);

                                string sqlString = GetInsertStatement(logTableName,
                                    messageToLog, palEventType, tableName, recordId, className, methodName, otherInformation);
                                WriteLogToDB(connectionString,sqlString);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
            else if (logType==LogType.WriteToDatabase)
            {
                string sqlString = GetInsertStatement(logTableName,
                    messageToLog, palEventType, tableName, recordId, className, methodName, otherInformation);
                WriteLogToDB(connectionString,sqlString);
            }
            else if (logType==LogType.WriteToFile)
            {
                string logLine = GetLogLine(messageToLog, palEventType, tableName, recordId, className, methodName, otherInformation);
                WriteLogToFile(logFilePath, logLine);
            }
        }
        #endregion

        #region Private methods

        private string GetInsertStatement(string logTableName, string messageToLog, EventLogEntryType palEventType, string tableName="", string recordId="", string className="", string methodName="", string otherInfo="")
        {
            // Get Pivotal User Name
            string userName = TypeConvert.ToString(this.SystemServer.UserProfile.UserName).Trim();
            if (messageToLog != null && messageToLog != String.Empty)
            {
                messageToLog = messageToLog.Replace("'", "''");
            }
            if (otherInfo != null && otherInfo != String.Empty)
            {
                otherInfo = otherInfo.Replace("'", "''");
            }
            string sqlString = "INSERT INTO " + logTableName +
                " (Event_Type, Description, Event_Date, User_Name, Primary_Table, Record_Id, Class_Name, Method_Name, Other) VALUES('";

            sqlString += palEventType.ToString() + "','" + messageToLog + "','";
            sqlString += DateTime.Now.ToString() + "','" + userName + "','";
            sqlString += tableName + "','" + recordId + "','";
            sqlString += className + "','" + methodName + "','";


            if (TypeConvert.ToString(otherInfo).Length > 0)
                sqlString += otherInfo.Replace('\'', ' ') + "')";
            else
                sqlString += "')";
            
            return sqlString;
        }
        private void WriteLogToDB(string connString, string sqlString)
        {
            // Connect to database and execute insert operation.
            SqlConnection sqlConn = new SqlConnection();
            sqlConn.ConnectionString = connString;
            var cmd = new SqlCommand();
            cmd.CommandText = sqlString;
            cmd.CommandType = System.Data.CommandType.Text;

            using (System.Transactions.TransactionScope transactionScope = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeOption.Suppress))
            {
                if (sqlConn != null)
                    cmd.Connection = sqlConn;

                if (cmd.Connection.State != System.Data.ConnectionState.Open)
                    cmd.Connection.Open();

                object id = cmd.ExecuteScalar();
                int newEventId = Convert.ToInt32(id);

                cmd.Connection.Close();
                cmd.Dispose();
                sqlConn.Close();
                sqlConn.Dispose();

                // Complete the Suppressed Transaction.  This isn't actually needed, as there's nothing to actually 'complete', as we're not in a Transaction.
                // It's here just for consistency/accordance with other TransactionScope modes.
                transactionScope.Complete();
            }
        }

        private string GetLogLine(string messageToLog, EventLogEntryType palEventType, string tableName, string recordId, string className, string methodName, string otherInfo)
        {
            string logLine = palEventType.ToString() + "|" + className + "|" + methodName + "|";
            logLine += messageToLog + "|" + tableName + "|" + recordId + "|" + otherInfo;

            return logLine;
        }

        private void WriteLogToFile(string fileLocation, string newLogLine)
        {
            string logFilename = "MKTOIntegrationLog_" + DateTime.Now.ToString("yyyyMMdd") + ".log";
            if (fileLocation.Substring(fileLocation.Length - 1) != "\\")
                fileLocation += "\\";
               
            StreamWriter sWriter = File.AppendText(fileLocation + logFilename);
            sWriter.WriteLine(newLogLine);
            sWriter.Close();
            sWriter.Dispose();

        }

        #endregion
    }
}



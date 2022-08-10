using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Windows.Forms;

using CdcSoftware.Pivotal.Applications.Core.Client;
using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.Types.Security;
using CdcSoftware.Pivotal.Engine.UI.Forms;
using CdcSoftware.Pivotal.Applications.Core.Common;
using System.Data.SqlClient;
using System.IO;
using CdcSoftware.Pivotal.Engine.Client.Services.Interfaces;

namespace MKTO.Client.FormTasks
{
    public partial class MarketoConfiguration : FormClientTask
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnEnableLoggingChanged(PivotalControl sender, EventArgs args)
        {
            PivotalCheckBox enableLog = (PivotalCheckBox)this.FormControl.GetControlByDataName("Log_Enable");
            PivotalDropDown logType = (PivotalDropDown)this.FormControl.GetControlByDataName("Log_Type");

            if (TypeConvert.ToBoolean(enableLog.Checked))
            {
                logType.Enabled = true;
                logType.Required = true;
                this.EnableDisableFields(false, false);
            }
            else
            {
                logType.Required = false;
                logType.Enabled = false;
                this.PrimaryDataRow["Log_Type"] = "";
                this.EnableDisableFields(false, false);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnLogTypeChanged(PivotalControl sender, EventArgs args)
        {
            PivotalDropDown logType = (PivotalDropDown)this.FormControl.GetControlByDataName("Log_Type");

            if (TypeConvert.ToString(logType.Value) == "File")
                this.EnableDisableFields(false, true);
            else if (TypeConvert.ToString(logType.Value) == "Database")
                this.EnableDisableFields(true, false);
            else
                this.EnableDisableFields(false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnTestConnectionClicked(PivotalControl sender, EventArgs args)
        {
            try
            {
                SqlConnection sqlConn = new SqlConnection();
                SqlCommand sqlComm = new SqlCommand();
                sqlConn.ConnectionString = TypeConvert.ToString(this.PrimaryDataRow["Log_DB_Conn_String"]);
                sqlConn.Open();
                sqlComm.Connection = sqlConn;
                sqlComm.CommandText = "select top 5 * from " + TypeConvert.ToString(this.PrimaryDataRow["Log_DB_Table_Name"]);
                int records = sqlComm.ExecuteNonQuery();

                PivotalMessageBox.Show("Test connection successful", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            catch (Exception exc)
            {
                PivotalMessageBox.Show("Test connection failed. Please verify information you entered is valid.", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnBrowseClicked(PivotalControl sender, EventArgs args)
        {
            FolderBrowserDialog fbd = new FolderBrowserDialog();

            DialogResult result = fbd.ShowDialog();

            if (!string.IsNullOrWhiteSpace(fbd.SelectedPath))
                this.PrimaryDataRow["Log_File_Location"] = fbd.SelectedPath;
        }

        private void EnableDisableFields(bool isDBLog, bool isFileLog)
        {
            PivotalTextBox fileLocation = (PivotalTextBox)this.FormControl.GetControlByDataName("Log_File_Location");
            PivotalTextBox dbConnString = (PivotalTextBox)this.FormControl.GetControlByDataName("Log_DB_Conn_String");
            PivotalButton testButton = (PivotalButton)this.FormControl.GetControlByName("TestConnection");
            PivotalButton browseButton = (PivotalButton)this.FormControl.GetControlByName("BrowseButton");
            PivotalTextBox dbTableName = (PivotalTextBox)this.FormControl.GetControlByDataName("Log_DB_Table_Name");


            fileLocation.Enabled = isFileLog;
            fileLocation.Required = isFileLog;
            browseButton.Enabled = isFileLog;

            dbConnString.Enabled = isDBLog;
            dbConnString.Required = isDBLog;
            dbTableName.Enabled = isDBLog;
            dbTableName.Required = isDBLog;
            testButton.Enabled = isDBLog;

            if (!isFileLog)
                this.PrimaryDataRow["Log_File_Location"] = "";

            if (!isDBLog)
            {
                this.PrimaryDataRow["Log_DB_Conn_String"] = "";
                this.PrimaryDataRow["Log_DB_Table_Name"] = "";
            }
        }
    }
}

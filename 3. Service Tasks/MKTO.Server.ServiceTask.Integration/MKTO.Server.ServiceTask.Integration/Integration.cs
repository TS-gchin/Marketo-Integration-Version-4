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
using MKTO.Server.ServiceTask;
using System.Runtime.Serialization.Json;
using System.Xml;
using MKTO.Common;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;

namespace MKTO.Server.ServiceTask
{
    /// <summary>
    /// Handle the integration between Pivotal and Marketo
    /// </summary>
    /// <history>
    /// #Revision   Date    Author  Description
    /// </history>
    public partial class Integration : AbstractApplicationServerTask
    {
        #region Constructor
        /// <summary>
        /// Initialize the instance of AppServiceServerTask class and set the default resource bundle to 'xxxxxx' LD Group.
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public Integration()
        {
        }
        #endregion

        #region Global variables

        private IntegrationData curData = new IntegrationData();
        //Utility utility = new Utility();
        public Logging ApplicationLog;
        private Id ConfigurationId;
        private int LoggingLevel=2;
        private bool OKToUpdateLastRunDate = true;
        private Utility utility;
        #endregion

        #region Protected methods

        /// <summary>
        ///     Retrieve values from the Marketo_Integration_Detail record and update the curData object
        /// </summary>
        /// <param name="integrationDetailId">Marketo_Integration_Detail_Id</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual void GetIntegrationDtlData(Id integrationDetailId)
        {
            const string Method = "GetIntegrationDtlData";
            try
            {
                SetApplicationLog();

                if (LoggingLevel>=2)
                {
                    ApplicationLog.WriteToLog(integrationDetailId, "Entering GetIntegrationDtlData for id" + integrationDetailId.ToString() , System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method,null);
                }
                
                DataTable dtIntegrationDtl = this.DefaultDataAccess.GetDataTable("Marketo: Integration Detail with Id ?", 1,
                    new object[] { integrationDetailId });

                if (dtIntegrationDtl.Rows.Count > 0)
                {
                    DataRow dr = dtIntegrationDtl.Rows[0];

                    if (curData.configurationId == null)
                    {
                        curData.configurationId = Id.Create(dr["Marketo_Configuration_Id"]);
                        ConfigurationId = curData.configurationId;
                    }

                    curData.integrationDtlId = Id.Create(dr["Marketo_Integration_Detail_Id"]);
                    curData.dataDirection = TypeConvert.ToString(dr["Data_Direction"]);
                    curData.lastRunDateTime = TypeConvert.ToDateTime(dr["Last_Run_Date_Time"]);
                    curData.isMarketoActivity = TypeConvert.ToBoolean(dr["Is_Marketo_Activity"]);
                    curData.integrationName = TypeConvert.ToString(dr["Name"]);
                    curData.fieldMapping = new Dictionary<string, string>();
                    curData.currentDateTime = DateTime.Now;
                    curData.queryName = TypeConvert.ToString(dr["Mapping_Query"]);

                    if (curData.isMarketoActivity)
                    {
                        Id marketoActivityTypeId = Id.Create(dr["Marketo_Activity_Types_Id"]);
                        if (marketoActivityTypeId != null)
                        {
                            DataRow drActivityTypes = this.DefaultDataAccess.GetDataRow("Marketo_Activity_Types", marketoActivityTypeId);
                            if (drActivityTypes != null)
                            {
                                curData.marketoActivityId = TypeConvert.ToString(drActivityTypes["Activity_Type_Id"]);
                                curData.queryName = TypeConvert.ToString(drActivityTypes["Query_Name"]);
                            }
                        }
                    }

                    GetConfigurationData(curData.configurationId);

                    //if (LoggingLevel >= 2)
                    //{
                    //    ApplicationLog.WriteToLog(integrationDetailId, "Entering GetIntegrationDtlData for id" + integrationDetailId.ToString(), System.Diagnostics.EventLogEntryType.Information,
                    //            null, null, "MKTO.Server.ServiceTask.Integration", Method, null);
                    //}
                }
            }
            catch(Exception exc)
            {
                ApplicationLog.WriteToLog(integrationDetailId, "Error occurred for id:"+integrationDetailId.ToString() , System.Diagnostics.EventLogEntryType.Error,
                        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
            }
        }

        /// <summary>
        ///     Retrieve values from the Marketo_Configuration record and update the curData object
        ///     with the Identity URL, REST URL, client and secret used to connect to Marketo
        /// </summary>
        /// <param name="configurationId">Marketo_Configuration_Id</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual void GetConfigurationData(Id configurationId)
        {
            const string Method= "GetConfigurationData";
            try
            {
                DataTable dtIntegration = this.DefaultDataAccess.GetDataTable("Marketo: Configuration with Id ?", 1,
                            new object[] { configurationId });

                if (dtIntegration.Rows.Count > 0)
                {
                    DataRow dr = dtIntegration.Rows[0];
                    curData.mktoIdentityURL = TypeConvert.ToString(dr["REST_Identity"]);
                    curData.mktoRestURL = TypeConvert.ToString(dr["REST_Endpoint"]);
                    curData.mktoClientId = TypeConvert.ToString(dr["REST_Client_Id"]);
                    curData.mktoClientSecret = TypeConvert.ToString(dr["REST_Secret"]);

                    utility = new Utility(curData.mktoIdentityURL, curData.mktoClientId, curData.mktoClientSecret);
                    //curData.mktoOAuthToken = utility.GetMarketoToken();
                    LoggingLevel = TypeConvert.ToInt32(dr["Log_Level"]);
                }

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(configurationId, "GetConfigurationData for id: " + configurationId.ToString(), System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Configuration", configurationId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    ApplicationLog.WriteToLog(configurationId, "REST Identity URL: " + curData.mktoIdentityURL, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Configuration", configurationId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    ApplicationLog.WriteToLog(configurationId, "REST Rest URL: " + curData.mktoRestURL, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Configuration", configurationId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                        "Marketo_Configuration", configurationId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
            }

        }

        /// <summary>
        ///     Get the Marketo paging token given timestamp so that Marketo will
        ///     retrieve records updated after sinceDateTime
        /// </summary>
        /// <param name="sinceDateTime">Timestamp</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual string GetPagingToken(DateTime sinceDateTime)
        {
            const string Method = "GetPagingToken";

            try
            {
                SetApplicationLog();

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Entering GetPageToken sinceDateTime=" + TypeConvert.ToString(sinceDateTime), System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }

                //String url = curData.mktoRestURL + "/v1/activities/pagingtoken.json?access_token=" + curData.mktoOAuthToken
                //    + "&sinceDatetime=" + sinceDateTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ");
                String url = curData.mktoRestURL + "/v1/activities/pagingtoken.json";
                string urlParams = "&sinceDatetime=" + sinceDateTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ"); 

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "url=" + url, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }


                XmlDocument xDoc = utility.CallMarketoRestAPI(url, urlParams , String.Empty, String.Empty);

                return xDoc.SelectSingleNode("root/nextPageToken").InnerText;
            }
            catch(Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return null;
            }
        }

        /// <summary>
        ///     Get field mappings based on the master system
        ///     valid values are "Pivotal" and "Marketo"
        /// </summary>
        /// <param name="masterSystem">Either Pivotal or Marketo</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual void GetFieldMapping(string masterSystem)
        {
            const string Method = "GetFieldMapping";
            try
            {
                SetApplicationLog();

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Entering GetFieldMapping(" + masterSystem + ")", System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(),"MKTO.Server.ServiceTask.Integration", Method, null);
                }

                //Retrieve mapping records
                DataTable dtMapping = this.DefaultDataAccess.GetDataTable("Marketo: Field Mapping for Integration Detail ?", 1,
                    new object[] { curData.integrationDtlId });

                if (dtMapping.Rows.Count > 0)
                {
                    //Retrieve Pivotal table to update
                    curData.pivObject = TypeConvert.ToString(dtMapping.Rows[0]["Pivotal_Table"]);
                    curData.mktoObject = TypeConvert.ToString(dtMapping.Rows[0]["External_Table"]);
                    if (curData.isMarketoActivity)
                    {

                    }    
                    string formulaField = "";
                    string masterField = "";
                    string updateField = "";

                    if (masterSystem == "Pivotal")
                    {
                        formulaField = "Pivotal_Field_Formula";
                        masterField = "Pivotal_Field";
                        updateField = "External_Field";

                        //Populate pivotal query
                        curData.pivQuery = CreateQuery(dtMapping);
                    }
                    else
                    {
                        formulaField = "External_Field_Formula";
                        masterField = "External_Field";
                        updateField = "Pivotal_Field";
                    }

                    // Link up Pivotal and Marketo fields
                    foreach (DataRow drMap in dtMapping.Rows)
                    {
                        //Check to see if formula was entered or Table/Field names were
                        string fieldFormula = TypeConvert.ToString(drMap[formulaField]);
                        if (fieldFormula == "")
                        {
                            if (LoggingLevel >= 2)
                            {
                                ApplicationLog.WriteToLog(ConfigurationId, "Mapping " + TypeConvert.ToString(drMap[masterField]) + " to " + TypeConvert.ToString(drMap[updateField]), System.Diagnostics.EventLogEntryType.Information,
                                        "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                            }

                            curData.fieldMapping.Add(TypeConvert.ToString(drMap[masterField]),
                                TypeConvert.ToString(drMap[updateField]));
                        }
                        else
                        {
                            if (LoggingLevel >= 2)
                            {
                                ApplicationLog.WriteToLog(ConfigurationId, "Mapping " + fieldFormula.Substring(fieldFormula.IndexOf(" AS ") + 4) + " to " + TypeConvert.ToString(drMap[updateField]), System.Diagnostics.EventLogEntryType.Information,
                                        "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                            }
//                            curData.fieldMapping.Add(fieldFormula,
//                                TypeConvert.ToString(drMap[updateField]));
                            curData.fieldMapping.Add(fieldFormula.Substring(fieldFormula.ToUpper().IndexOf(" AS ") + 4),
                                TypeConvert.ToString(drMap[updateField]));
                        }

                        // Set primary key values
                        if (TypeConvert.ToBoolean(drMap["Unique_Identifier"]) == true)
                        {

                            if (LoggingLevel >= 2)
                            {
                                ApplicationLog.WriteToLog(ConfigurationId, "Setting Marketo field  " + TypeConvert.ToString(drMap["External_Field"]) + " and Pivotal field " + 
                                    TypeConvert.ToString(drMap["Pivotal_Field"]) + " as Unique Identifiers", System.Diagnostics.EventLogEntryType.Information,
                                        "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                            }

                            curData.mktoPKFieldName = TypeConvert.ToString(drMap["External_Field"]);
                            curData.pivPKFieldName = TypeConvert.ToString(drMap["Pivotal_Field"]);
                        }

                    }
                }
                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Leaving GetFieldMapping(" + masterSystem + ")", System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
            }
        }

        /// <summary>
        ///     Create query to retrieve data from Pivotal
        /// </summary>
        /// <param name="dtMapping">Mapping table</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual string CreateQuery(DataTable dtMapping)
        {
            const string Method = "CreateQuery";
            try
            {
                SetApplicationLog();

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Entering CreateQuery" , System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }

                string qryFinal = "";
                string qryFields = "";
                string qryTables = "";

                foreach (DataRow dr in dtMapping.Rows)
                {
                    qryFields += RetrieveFieldName("Pivotal", dr);

                    //Only include each table name once
                    string newTable = RetrieveTableName("Pivotal", dr);
                    if (qryTables.IndexOf(newTable) < 0)
                        qryTables += newTable;
                }

                //Include Pivotal Id in field listing
                if (qryFields.IndexOf(curData.pivObject + "_Id,") < 0)
                    qryFields += curData.pivObject + "_Id,";

                string qryWhereClause = RetrieveWhereClause();

                if (qryWhereClause.Length > 0)
                    qryWhereClause += " AND ";

                qryWhereClause += curData.pivObject + ".Rn_Edit_Date BETWEEN '" + curData.lastRunDateTime.ToString() + "' AND '" + curData.currentDateTime.ToString() + "'";

                qryFinal = "SELECT " + qryFields.Substring(0, qryFields.Length - 1) + " FROM " + qryTables.Substring(0, qryTables.Length - 1)
                    + " WHERE " + qryWhereClause;

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Leaving CreateQuery Query= "+ qryFinal, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }

                return qryFinal;
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return String.Empty;
            }
        }

        /// <summary>
        ///     Retrieve field names for a given mapping based on the field prefix
        /// </summary>
        /// <param name="fieldPrefix"></param>
        /// <param name="drMapping"></param>
        /// <returns>Field name</returns>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual string RetrieveFieldName(string fieldPrefix, DataRow drMapping)
        {
            string fldValue = "";
            const string Method = "RetrieveFieldName";
            try
            {
                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Entering "+Method, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }

                if (TypeConvert.ToString(drMapping[fieldPrefix + "_Field_Formula"]).Length > 0)
                {
                    fldValue = TypeConvert.ToString(drMapping[fieldPrefix + "_Field_Formula"]) + ",";
                }
                else
                {
                    string tableName = "";
                    if (TypeConvert.ToString(drMapping[fieldPrefix + "_Table_Alias"]).Length > 0)
                    {
                        tableName = TypeConvert.ToString(drMapping[fieldPrefix + "_Table_Alias"]) + ".";
                    }
                    else
                    {
                        tableName = TypeConvert.ToString(drMapping[fieldPrefix + "_Table"]) + ".";
                    }

                    fldValue = tableName + TypeConvert.ToString(drMapping[fieldPrefix + "_Field"]) + ",";
                }

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Leaving " + Method, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", "RetrieveFieldName", exc.Message);
            }
            return fldValue;

        }

        /// <summary>
        ///     Retrieve table names for a given mapping based on the field prefix
        /// </summary>
        /// <param name="fieldPrefix"></param>
        /// <param name="drMapping"></param>
        /// <returns>Table name</returns>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual string RetrieveTableName(string fieldPrefix, DataRow drMapping)
        {
            string tableName = "";
            string tableAlias = "";
            const string Method = "RetrieveFieldName";
            try
            {
                SetApplicationLog();

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Entering " + Method, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }

                if (TypeConvert.ToString(drMapping[fieldPrefix + "_Table_Alias"]).Length > 0)
                    tableAlias = " " + TypeConvert.ToString(drMapping[fieldPrefix + "_Table_Alias"]);

                tableName = TypeConvert.ToString(drMapping[fieldPrefix + "_Table"]) + tableAlias + ",";

                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Leaving " + Method, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", "RetrieveTableName", exc.Message);
            }
            return tableName;
        }

        /// <summary>
        ///     Generate the where clause based on the Marketo_Integration_Detail record
        /// </summary>
        /// <returns>The Where Clause</returns>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual string RetrieveWhereClause()
        {
            string qryWhereClause = "";
            string Method = "RetrieveWhereClause(";

            try
            {
                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Entering " + Method, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
                DataTable dtRelationship = this.DefaultDataAccess.GetDataTable("Marketo: Table Relationship for Integration Detail ?",
                    1, curData.integrationDtlId);

                if (dtRelationship.Rows.Count > 0)
                {
                    int i = 0;
                    foreach (DataRow dr in dtRelationship.Rows)
                    {
                        if (i > 0)
                            qryWhereClause += TypeConvert.ToString(dr["Operator"]) + " ";

                        if (TypeConvert.ToString(dr["Formula"]).Length > 0)
                            qryWhereClause += TypeConvert.ToString(dr["Formula"]) + " ";
                        else
                        {
                            qryWhereClause += TypeConvert.ToString(dr["Parent_Table"]) + "." + TypeConvert.ToString(dr["Parent_Field"])
                                + " = " + TypeConvert.ToString(dr["Child_Table"]) + "." +
                                TypeConvert.ToString(dr["Child_Field"]) + " ";
                        }

                        i++;
                    }

                }
                if (LoggingLevel >= 2)
                {
                    ApplicationLog.WriteToLog(ConfigurationId, "Leaving " + Method+ " Where Clause: " + qryWhereClause, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", curData.integrationDtlId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", "RetrieveWhereClause", exc.Message);
            }
            return qryWhereClause;
        }

        /// <summary>
        ///     Sets the value of the Pivotal external source id with the Marketo Id
        /// </summary>
        /// <param name="pivObject">Pivotal object</param>
        /// <param name="pivRecordId">Pivotal Record Id</param>
        /// <param name="externalSourceId"></param>
        /// <param name="marketoFieldName"></param>
        /// <param name="configurationId"></param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        protected virtual void SetPivotalExternalSourceId(string pivObject, Id pivRecordId, string externalSourceId, string marketoFieldName, Id configurationId)
        {
            const string Method = "SetPivotalExternalSourceId";
            try
            {

                DataRow drPivRecord = this.DefaultDataAccess.GetDataRow(pivObject, pivRecordId,
                new string[] { marketoFieldName });

                if (drPivRecord != null)
                {
                    drPivRecord[marketoFieldName] = externalSourceId;
                    this.DefaultDataAccess.SaveDataRow(drPivRecord);
                }
            }
            catch (Exception exc)
            {
                throw exc;
            }
        }

        /// <summary>
        ///     Update Marketo_Integration_Detail.Last_Run_Date with the current timestamp
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        protected virtual void UpdateLastRunDate()
        {
            const string Method = "UpdateLastRunDate";
            try
            {
                Id detailIntegrationId = null;
                SetApplicationLog();
                if (LoggingLevel >= 2)
                {
                    if (OKToUpdateLastRunDate == false)
                    {
                        ApplicationLog.WriteToLog(ConfigurationId, "LastRunDate not updated  ", System.Diagnostics.EventLogEntryType.Information,
                                null, null, "MKTO.Server.ServiceTask.Integration", Method, null);
                    }
                }
                if (OKToUpdateLastRunDate)
                {
                    DataRow drIntegration = this.DefaultDataAccess.GetDataRow("Marketo_Integration_Detail", curData.integrationDtlId,
                        new string[] { "Marketo_Integration_Detail_Id", "Last_Run_Date_Time" });
                    if (drIntegration != null)
                    {
                        drIntegration["Last_Run_Date_Time"] = curData.currentDateTime;
                        detailIntegrationId = Id.Create(drIntegration["Marketo_Integration_Detail_Id"]);
                        this.DefaultDataAccess.SaveDataRow(drIntegration);
                    }
                    if (LoggingLevel >= 2)
                    {
                        ApplicationLog.WriteToLog(ConfigurationId, "Set Last_Run_Date_Time on record  " + detailIntegrationId.ToString() + " to " + TypeConvert.ToString(curData.currentDateTime), System.Diagnostics.EventLogEntryType.Information,
                                null, null, "MKTO.Server.ServiceTask.Integration", Method, null);
                    }
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                        null,null,"MKTO.Server.ServiceTask.Integration", "UpdateLastRunDate", exc.Message);
            }
        }

        /// <summary>
        ///     Update Marketo_Integration_Detail.Last_Run_Date with the current timestamp
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual void UpdateLastRunDate(Id integrationDetailId)
        {
            const string Method = "UpdateLastRunDate";
            try
            {
                SetApplicationLog();
                //if (LoggingLevel >= 2)
                //{
                //    if (OKToUpdateLastRunDate == false)
                //    {
                //        ApplicationLog.WriteToLog(ConfigurationId, "LastRunDate not updated  ", System.Diagnostics.EventLogEntryType.Information,
                //                null, null, "MKTO.Server.ServiceTask.Integration", Method, null);
                //    }
                //}

                if (OKToUpdateLastRunDate)
                {
                    DataRow drIntegration = this.DefaultDataAccess.GetDataRow("Marketo_Integration_Detail", integrationDetailId,
                        new string[] { "Last_Run_Date_Time" });

                    if (drIntegration != null)
                    {
                        drIntegration["Last_Run_Date_Time"] = DateTime.Now;
                        this.DefaultDataAccess.SaveDataRow(drIntegration);
                    }
                    //if (LoggingLevel >= 2)
                    //{
                    //    ApplicationLog.WriteToLog(ConfigurationId, "Set Last_Run_Date_Time on record  " + integrationDetailId.ToString() + " to " + TypeConvert.ToString(DateTime.Now), System.Diagnostics.EventLogEntryType.Information,
                    //            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    //}
                }
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", "UpdateLastRunDate", exc.Message);
            }
        }


        #endregion

        #region Public Methods

        /// <summary>
        ///     This method is used to cycle through all of the integration detail records
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual void ProcessRecords()
        {
            string strQuery = "SELECT Marketo_Integration_Detail_Id, mc.Marketo_Configuration_Id, Data_Direction,Log_level, mid.Is_Marketo_Activity  " +
                              "FROM Marketo_Integration_Detail mid INNER JOIN Marketo_Configuration mc on mc.Marketo_Configuration_Id = mid.Marketo_Configuration_Id " +
                              "WHERE ISNULL(Active,0) = 1 " +
                              "ORDER BY Execution_Order ASC ";
            //            string strQuery = "SELECT Marketo_Integration_Detail_Id, Marketo_Configuration_Id, Data_Direction FROM Marketo_Integration_Detail WHERE ISNULL(Active,0) = 1";
            using (DataTable dtRecords = this.DefaultDataAccess.GetDataTable(strQuery))
            {
                if (dtRecords.Rows.Count > 0)
                {
                    LoggingLevel = TypeConvert.ToInt32(dtRecords.Rows[0]["Log_Level"]);
                }

                ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();

                curData.currentDateTime = DateTime.Now;

                foreach (DataRow dr in dtRecords.Rows)
                {
                    bool runResult1 = true;
                    bool runResult2 = true;
                    bool runResult3 = true;
                    bool finalRunResult = true;
                    
                    curData.integrationDtlId = Id.Create(dr["Marketo_Integration_Detail_Id"]);
                    curData.isMarketoActivity = TypeConvert.ToBoolean(dr["Is_Marketo_Activity"]);
                    
                    ApplicationLog.WriteToLog(curData.configurationId, "Integration started", System.Diagnostics.EventLogEntryType.Information,
                        "Marketo_Integration_Detail", curData.integrationDtlId.ToString(),
                        "MKTO.Server.ServiceTask.Integration", "ProcessRecords", null);

                    try
                    {
                        switch (TypeConvert.ToString(dr["Data_Direction"]).ToLower())
                        {
                            case "pivotal to marketo":
                                runResult1=UpdateMarketoRecord(curData.integrationDtlId);
                                finalRunResult = runResult1;
                                break;
                            case "marketo to pivotal":

                                if (curData.isMarketoActivity == false)
                                {
                                    runResult1 = UpdatePivotalRecordFromNew(curData.integrationDtlId);
                                }
                                runResult2=UpdatePivotalRecord(curData.integrationDtlId);
                                finalRunResult = runResult1 && runResult2;
                                break;
                            case "bidirectional":

                                if (curData.isMarketoActivity == false)
                                {
                                    runResult1 = UpdatePivotalRecordFromNew(curData.integrationDtlId);
                                }
                                runResult3 = UpdateMarketoRecord(curData.integrationDtlId);
                                runResult2 = UpdatePivotalRecord(curData.integrationDtlId);
                                finalRunResult = runResult1 && runResult2 && runResult3;
                                break;
                        }
                        //Update last time integration was run
                        if (finalRunResult == true)
                        {
                            //UpdateLastRunDate();
                            this.SystemServer.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdateLastRunDate", new Type[] { typeof(Id) }, new object[] { curData.integrationDtlId }, true);
                        }
                    }
                    catch (Exception exc)
                    {
                        ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                "Marketo_Integration_Detail", curData.integrationDtlId.ToString(),
                                "MKTO.Server.ServiceTask.Integration", "ProcessRecords", exc.Message);
                    }
                }
            }
        }
        /// <summary>
        ///     This method is used to update the Pivotal records from Marketo
        /// </summary>
        /// <history>
        /// #Revision   Date            Author  Description
        /// 6.0.0.57    2022-11-20      GC      Depracateed
        /// </history>
        //[TaskExecute]
        //public virtual void SSUpdatePivotalRecord()
        //{
        //    try
        //    {
        //        if (ApplicationLog == null)
        //            ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();
        //        Id integrationDetailId = this.DefaultDataAccess.SqlFind("Marketo_Integration_Detail", "Active", true);
        //        if (integrationDetailId == null)
        //            return;
        //        GetIntegrationDtlData(integrationDetailId);

        //        if (curData.mktoOAuthToken.Length > 0)
        //        {
        //            // Populate field mapping object. Records are retrieved from Marketo
        //            GetFieldMapping("Marketo");

        //            // Get list of fields to retrieve from Marketo
        //            string fieldListing = "";
        //            int i = 0;
        //            foreach (string fKey in curData.fieldMapping.Keys)
        //            {
        //                if (i > 0)
        //                    fieldListing += ",";
        //                fieldListing += fKey;
        //                i++;
        //            }

        //            DateTime sinceDateTime = curData.currentDateTime;
        //            string pagingToken = GetPagingToken(sinceDateTime);
        //            bool moreResults = false;

        //            do
        //            {
        //                //string mktoURL = curData.mktoRestURL + "/v1/activities/leadchanges.json?access_token=" + curData.mktoOAuthToken
        //                //    + "&sinceDatetime=" + sinceDateTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ") +
        //                //    "&nextPageToken=" + pagingToken + "&fields=" + fieldListing;
        //                string mktoURL = curData.mktoRestURL + "/v1/activities/leadchanges.json"; 
        //                string mktoURLParams = "&sinceDatetime=" + sinceDateTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ") + "&nextPageToken=" + pagingToken + "&fields=" + fieldListing;
        //                XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL, mktoURLParams,String.Empty, String.Empty);

        //                XmlNodeList xmlRecordList = xDoc.SelectNodes("root/result/item");

        //                //Get next paging token
        //                pagingToken = TypeConvert.ToString(xDoc.SelectSingleNode("root/nextPageToken").InnerText);
        //                //Check to see if there are more pages
        //                moreResults = TypeConvert.ToBoolean(xDoc.SelectSingleNode("root/moreResult").InnerText);

        //                //Get list of fields from Pivotal
        //                string[] pivFields = new string[curData.fieldMapping.Count];
        //                curData.fieldMapping.Values.CopyTo(pivFields, 0);

        //                //Cycle through all records and update corresponding Pivotal record
        //                foreach (XmlNode xmlRecord in xmlRecordList)
        //                {
        //                    //Get Marketo Id from XML
        //                    int marketoId = 0;
        //                    if (xmlRecord["leadId"] != null)
        //                        marketoId = TypeConvert.ToInt32(xmlRecord["leadId"].InnerText);

        //                    //Get date the record was updated
        //                    DateTime lastUpdated = curData.lastRunDateTime;
        //                    if (xmlRecord["activityDate"] != null)
        //                        lastUpdated = TypeConvert.ToDateTime(xmlRecord["activityDate"].InnerText);

        //                    //Only update if Id is present and it was changed between the date range
        //                    if (marketoId > 0 && lastUpdated.CompareTo(curData.lastRunDateTime) > 0 && lastUpdated.CompareTo(curData.currentDateTime) < 0)
        //                    {
        //                        //Get list of updated fields from Marketo
        //                        XmlNodeList xmlFields = xmlRecord.SelectNodes("fields/item");

        //                        if (xmlFields.Count > 0)
        //                        {
        //                            //Check to see if record exists in Contacts and Leads
        //                            DataTable pivRecord = this.DefaultDataAccess.GetDataTable("Marketo: Lead for Marketo Id ?",
        //                                new object[] { marketoId },
        //                                pivFields);

        //                            if (pivRecord.Rows.Count == 0)
        //                            {
        //                                pivRecord = this.DefaultDataAccess.GetDataTable("Marketo: Contact for Marketo Id ?",
        //                                new object[] { marketoId },
        //                                pivFields);
        //                            }

        //                            DataRow curRecord;

        //                            if (pivRecord.Rows.Count == 0)
        //                            {
        //                                //If record doesn't exist, create Lead in Pivotal
        //                                curRecord = this.DefaultDataAccess.GetNewDataRow(curData.pivObject, pivFields);
        //                            }
        //                            else
        //                            {
        //                                //Update existing record
        //                                curRecord = pivRecord.Rows[0];
        //                            }


        //                            //Cycle through fields in XML
        //                            foreach (XmlNode xmlFieldInfo in xmlFields)
        //                            {
        //                                //If column exists in mapping, update value
        //                                if (curData.fieldMapping.ContainsKey(xmlFieldInfo["name"].InnerText))
        //                                {
        //                                    if (pivRecord.Columns.Contains(curData.fieldMapping[xmlFieldInfo["name"].InnerText]))
        //                                    {
        //                                        curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = xmlFieldInfo["newValue"].InnerText;
        //                                    }
        //                                }
        //                            }
        //                            //Save changes to Pivotal
        //                            this.DefaultDataAccess.SaveDataRow(curRecord);
        //                        }
        //                        else
        //                        {
        //                            //Check to see if new record was added to Marketo
        //                            xmlFields = xmlRecord.SelectNodes("attributes/item");

        //                            //Cycle through fields in XML
        //                            foreach (XmlNode xmlFieldInfo in xmlFields)
        //                            {
        //                                //Check to see if it is a new record
        //                                if (xmlFieldInfo["name"].InnerText == "Source Type" && xmlFieldInfo["value"].InnerText == "New person")
        //                                {
        //                                    //Retrieve Lead record
        //                                    //mktoURL = curData.mktoRestURL + "/v1/lead/" + marketoId + ".json?access_token="
        //                                    //    + curData.mktoOAuthToken;
        //                                    mktoURL = curData.mktoRestURL + "/v1/lead/" + marketoId;

        //                                    XmlDocument xDoc2 = utility.CallMarketoRestAPI(mktoURL, String.Empty,String.Empty, String.Empty);

        //                                    //Create new Lead in Pivotal
        //                                    DataRow curRecord = this.DefaultDataAccess.GetNewDataRow(curData.pivObject, pivFields);

        //                                    //Get list of updated fields from Marketo
        //                                    XmlNode xmlNewFields = xDoc2.SelectSingleNode("root/result/item");

        //                                    //Cycle through fields in mapping
        //                                    foreach (KeyValuePair<string, string> kvp in curData.fieldMapping)
        //                                    {
        //                                        //If column exists in mapping, update value
        //                                        if (xmlNewFields[kvp.Key].InnerText != "")
        //                                        {
        //                                            if (curRecord.Table.Columns.Contains(kvp.Value))
        //                                            {
        //                                                curRecord[kvp.Value] = xmlNewFields[kvp.Key].InnerText;
        //                                            }
        //                                        }
        //                                    }

        //                                    //Save changes to Pivotal
        //                                    this.DefaultDataAccess.SaveDataRow(curRecord);
        //                                }
        //                            }
        //                        }
        //                    }
        //                }
        //            } while (moreResults);

        //            //Update last time integration was run
        //            UpdateLastRunDate();
        //        }
        //    }
        //    catch (Exception exc)
        //    {
        //        ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
        //            null, null, "MKTO.Server.ServiceTask.Integration", "UpdatePivotalRecord", exc.Message);
        //    }
        //}

        /// <summary>
        ///     This method is used to update the Pivotal records from Marketo
        /// </summary>
        /// <history>
        /// #Revision   Date            Author  Description
        /// 6.0.0.57    2022-11-20      GC      Depracateed
        /// </history>
        //[TaskExecute]
        //public virtual void SSUpdateMarketoRecord()
        //{
        //    try
        //    {
        //        if (ApplicationLog == null)
        //            ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();
        //        Id integrationDetailId = this.DefaultDataAccess.SqlFind("Marketo_Integration_Detail", "Active", true);
        //        if (integrationDetailId == null)
        //            return;
        //        GetIntegrationDtlData(integrationDetailId);

        //        if (curData.mktoOAuthToken.Length > 0)
        //        {
        //            // Populate field mapping object. Records are retrieved from Pivotal
        //            GetFieldMapping("Pivotal");

        //            //Retrieve Pivotal records
        //            DataTable dtPivotal = this.DefaultDataAccess.GetDataTable(curData.pivQuery);

        //            if (dtPivotal.Rows.Count > 0)
        //            {
        //                //Add record count to log
        //                //ApplicationLog.WriteToLog(curData.configurationId, dtPivotal.Rows.Count.ToString() + 
        //                //    " records will be transferred to Marketo", System.Diagnostics.EventLogEntryType.Information,
        //                //    null, null, "MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord", null);

        //                StringBuilder jsonRecord = new StringBuilder();
        //                StringBuilder jsonPrefix = new StringBuilder();

        //                //Cycle through all Pivotal records
        //                foreach (DataRow dr in dtPivotal.Rows)
        //                {
        //                    int i = 0;
        //                    curData.pivRecordId = Id.Create(dr[curData.pivObject + "_Id"]);

        //                    //Create JSON string for transfer to Marketo
        //                    jsonRecord.Clear();
        //                    jsonPrefix.Clear();
        //                    jsonRecord.AppendLine("\"input\":[{");
        //                    foreach (DataColumn col in dtPivotal.Columns)
        //                    {
        //                        //Ignore any fields that are not part of the mapping
        //                        if (curData.fieldMapping.ContainsKey(col.ColumnName))
        //                        {
        //                            //Do not include external record Id in JSON, but retrieve value for URL
        //                            if (curData.fieldMapping[col.ColumnName] == curData.mktoPKFieldName)
        //                            {
        //                                jsonPrefix.AppendLine("{");
        //                                jsonPrefix.AppendLine("\"lookupField\":\"id\",");
        //                                curData.mktoRecordId = TypeConvert.ToString(dr[col.ColumnName]);
        //                            }

        //                            if (i > 0)
        //                                jsonRecord.Append(",");
        //                            System.Type fldType = col.DataType;
        //                            if (fldType.Name == "DateTime")
        //                                //Format date field so Marketo will accept it
        //                                jsonRecord.AppendLine("\"" + curData.fieldMapping[col.ColumnName] + "\":\"" +
        //                                    DateTime.Parse(TypeConvert.ToString(dr[col.ColumnName])).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") + "\"");
        //                            else
        //                                jsonRecord.AppendLine("\"" + curData.fieldMapping[col.ColumnName] + "\":\"" + dr[col.ColumnName] + "\"");
        //                            i++;
        //                        }
        //                    }

        //                    if (jsonPrefix.ToString() == "")
        //                        jsonPrefix.AppendLine("{");

        //                    jsonRecord.AppendLine("}] }");

        //                    //Setup URL for record update
        //                    //string mktoURL = curData.mktoRestURL + "/v1/" + curData.mktoObject.ToLower() + ".json?access_token="
        //                    //        + curData.mktoOAuthToken;
        //                    string mktoURL = curData.mktoRestURL + "/v1/" + curData.mktoObject.ToLower() + ".json";

        //                    XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL,string.Empty, "POST",
        //                        jsonPrefix.ToString() + jsonRecord.ToString());

        //                    XmlNodeList nodeList = xDoc.GetElementsByTagName("success");

        //                    if (TypeConvert.ToBoolean(nodeList.Item(0).InnerText) == true)
        //                    {
        //                        //Get details of result
        //                        nodeList = xDoc.GetElementsByTagName("item");

        //                        //Check to see if updated or created
        //                        XmlNode intResult = nodeList.Item(0);

        //                        if (intResult["status"].InnerText == "created")
        //                        {
        //                            int mktoRecordId = TypeConvert.ToInt32(intResult["id"].InnerText);
        //                            //Update Pivotal record with Marketo Id
        //                            SetPivotalExternalSourceId(curData.pivObject, curData.pivRecordId, TypeConvert.ToString(mktoRecordId));
        //                        }
        //                        else if (intResult["status"].InnerText == "skipped")
        //                        {
        //                            XmlNodeList xmlReason = intResult.SelectNodes("reasons");
        //                            string errorList = "Marketo Error: ";
        //                            foreach (XmlNode xmlReasonItem in xmlReason)
        //                            {
        //                                if (xmlReasonItem.SelectSingleNode("item/code") != null)
        //                                    errorList += xmlReasonItem.SelectSingleNode("item/code").InnerText + ", "
        //                                        + xmlReasonItem.SelectSingleNode("item/message").InnerText;
        //                            }

        //                            // Report reason for skipping record
        //                            ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
        //                            null, null, "MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord", errorList);
        //                        }
        //                    }
        //                    else
        //                    {
        //                        XmlNode xmlErrors = xDoc.SelectSingleNode("root/errors");
        //                        XmlNodeList xmlErrorList = xmlErrors.SelectNodes("item");
        //                        string errorList = "Marketo Error: ";

        //                        foreach (XmlNode xmlError in xmlErrorList)
        //                        {
        //                            errorList += xmlError["code"].InnerText + ", " + xmlError["message"].InnerText + " ";
        //                        }
        //                        // Put error message into error Log
        //                        ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
        //                            null, null, "MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord", errorList);
        //                    }
        //                }
        //            }
        //            //Update last time integration was run
        //            UpdateLastRunDate();
        //        }
        //    }
        //    catch (Exception exc)
        //    {
        //        ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
        //            null, null, "MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord", exc.Message);
        //    }
        //}




        /// <summary>
        ///     This method is used to update the Pivotal records from Marketo
        /// </summary>
        /// <paramref name="integrationDetailId">Marketo_Detail_Integration_Id</paramref>
        /// <history>
        /// </history>
        [TaskExecute]
        public virtual bool UpdatePivotalRecord(Id integrationDetailId)
        {
            string Method = "UpdatePivotalRecord";
            try
            {
                OKToUpdateLastRunDate = true;
                if (ApplicationLog == null)
                    ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();

                GetIntegrationDtlData(integrationDetailId);
                string token = utility.GetMarketoToken();

                ApplicationLog.WriteToLog(curData.configurationId, "Running integration: " + curData.integrationName, System.Diagnostics.EventLogEntryType.Information,
"Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "Direction: " + curData.dataDirection + " Previous run: " + TypeConvert.ToString(curData.lastRunDateTime));

                if (LoggingLevel >= 2)
                {
                    if (token != null)
                    {
                        ApplicationLog.WriteToLog(curData.configurationId, "Here is the Auth Token: " + curData.mktoOAuthToken, System.Diagnostics.EventLogEntryType.Information,
            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    }
                }

                if (token != null && token.Length > 0)
                {
                    // Populate field mapping object. Records are retrieved from Marketo
                    GetFieldMapping("Marketo");

                    // Get list of fields to retrieve from Marketo
                    string fieldListing = "";
                    int i = 0;
                    foreach (string fKey in curData.fieldMapping.Keys)
                    {
                        if (i > 0)
                            fieldListing += ",";
                        fieldListing += fKey;
                        i++;
                    }

                    DateTime sinceDateTime = curData.lastRunDateTime;
                    string pagingToken = GetPagingToken(sinceDateTime);
                    bool moreResults = false;
                    bool isMarketoActivity = curData.isMarketoActivity;

                    ApplicationLog.WriteToLog(curData.configurationId, "Running integration: " + curData.integrationName , System.Diagnostics.EventLogEntryType.Information,
                        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "Direction: " + curData.dataDirection + " Previous run: " + TypeConvert.ToString(curData.lastRunDateTime));

                    if (LoggingLevel > 1)
                        ApplicationLog.WriteToLog(curData.configurationId, "Updating Pivotal records updated after:" + sinceDateTime, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");



                    // Keep looping as long as there are results to process from the JSON call to Marketo
                    // Each call to Marketo retrieves up to 300 results at a time
                    // If the JSON message returns a node named moreResult, that means we need to continue
                    // calling Marketo to get the next set of results

                    do // while (moreResults)
                    {
                        string mktoURL = "";
                        string mktoParams = "";
                        string strRecordId = "";

                        // Marketo has a limit where you can only make 100 API calls within 20 seconds.
                        // If you exceed that, you will get a 606 error.
                        // This while loop allows us to reprocess the record if we encounter a 606 error.  
                        // We pause for 3 seconds and then attempt to reprocess the record
                        bool proceedToNextRecord = false;

                        while (!proceedToNextRecord)
                        {
                            if (isMarketoActivity)
                            {
                                //mktoURL = curData.mktoRestURL + "/v1/activities.json?access_token=" + curData.mktoOAuthToken
                                //+ "&activityTypeIds=" + curData.marketoActivityId +
                                //"&nextPageToken=" + pagingToken;
                                mktoURL = curData.mktoRestURL + "/v1/activities.json";
                                mktoParams = "&activityTypeIds=" + curData.marketoActivityId + "&nextPageToken=" + pagingToken;

                                if (LoggingLevel >= 2)
                                    ApplicationLog.WriteToLog(curData.configurationId, "Calling Marketo using URL: " + mktoURL + mktoParams, System.Diagnostics.EventLogEntryType.Information,
                                        null, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                            }
                            else
                            {
                                //mktoURL = curData.mktoRestURL + "/v1/activities/leadchanges.json?access_token=" + curData.mktoOAuthToken
                                //    + "&sinceDatetime=" + sinceDateTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ") +
                                //    "&nextPageToken=" + pagingToken + "&fields=" + fieldListing;
                                //mktoURL = curData.mktoRestURL + "/v1/activities/leadchanges.json?access_token=" + curData.mktoOAuthToken
                                //    //                                + "&sinceDatetime=" + sinceDateTime.ToString("yyyy-MM-dd'T'HH:mm:ssZ") +
                                //    + "&nextPageToken=" + pagingToken + "&fields=" + fieldListing;
                                mktoURL = curData.mktoRestURL + "/v1/activities/leadchanges.json";
                                mktoParams = "&nextPageToken=" + pagingToken + "&fields=" + fieldListing;

                                if (LoggingLevel >= 2)
                                    ApplicationLog.WriteToLog(curData.configurationId, "Calling Marketo using URL: " + mktoURL + mktoParams, System.Diagnostics.EventLogEntryType.Information,
                                        null, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                            }

                            XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL, mktoParams, String.Empty, String.Empty);

                            if (LoggingLevel >= 2)
                            {
                                if (xDoc != null && xDoc.InnerXml != "" && xDoc.InnerXml != null)
                                {
                                    ApplicationLog.WriteToLog(curData.configurationId, "Response from Marketo using URL: " + mktoURL + mktoParams, System.Diagnostics.EventLogEntryType.Information,
                                        null, null, "MKTO.Server.ServiceTask.Integration", Method, xDoc.InnerXml);
                                }
                            }

                            XmlNodeList nodeList = xDoc.GetElementsByTagName("success");
                            bool error606 = false;
                            bool error602 = false;

                            XmlNodeList xmlRecordList = xDoc.SelectNodes("root/result/item");

                            if (TypeConvert.ToBoolean(nodeList.Item(0).InnerText) == true)
                            {
                                error606 = false;
                                error602 = false;
                                proceedToNextRecord = true;

                                if (LoggingLevel >= 1)
                                    ApplicationLog.WriteToLog(curData.configurationId, "Change count: " + xmlRecordList.Count.ToString(), System.Diagnostics.EventLogEntryType.Information,
                                                                null, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                                //Get next paging token
                                pagingToken = TypeConvert.ToString(xDoc.SelectSingleNode("root/nextPageToken").InnerText);
                                
                                //Check to see if there are more pages.  If so, then we will make another REST call using the next paging token
                                //from above
                                moreResults = TypeConvert.ToBoolean(xDoc.SelectSingleNode("root/moreResult").InnerText);

                                //Get list of fields from Pivotal

                                int fieldCount = 0;
                                if (isMarketoActivity)
                                {
                                    fieldCount = curData.fieldMapping.Count + 2;
                                }
                                else
                                {
                                    fieldCount = curData.fieldMapping.Count;
                                }
                                string[] pivFields = new string[fieldCount];

                                curData.fieldMapping.Values.CopyTo(pivFields, 0);

                                if (isMarketoActivity)
                                {
                                    pivFields[pivFields.Length - 1] = "Contact_Id";
                                    pivFields[pivFields.Length - 2] = "SFA_Lead_Id";
                                }

                                //Cycle through all records and update corresponding Pivotal record
                                foreach (XmlNode xmlRecord in xmlRecordList)
                                {
                                    //Get Marketo Id from XML
                                    int marketoId = 0;
                                    if (xmlRecord["leadId"] != null)
                                        marketoId = TypeConvert.ToInt32(xmlRecord["leadId"].InnerText);

                                    string marketoLeadId = "";
                                    if (xmlRecord["leadId"] != null)
                                        marketoLeadId = TypeConvert.ToString(xmlRecord["leadId"].InnerText);


                                    string marketoGUID = "";
                                    if (xmlRecord["marketoGUID"] != null)
                                        marketoGUID = TypeConvert.ToString(xmlRecord["marketoGUID"].InnerText);

                                    //Get date the record was updated
                                    DateTime lastUpdated = curData.lastRunDateTime;
                                    if (xmlRecord["activityDate"] != null)
                                        lastUpdated = TypeConvert.ToDateTime(xmlRecord["activityDate"].InnerText);

                                    string primaryAttributeValue = "";
                                    if (xmlRecord["primaryAttributeValue"] != null)
                                        primaryAttributeValue = TypeConvert.ToString(xmlRecord["primaryAttributeValue"].InnerText);

                                    //Only update if Id is present and it was changed between the date range
                                    if (marketoId > 0 && lastUpdated.CompareTo(curData.lastRunDateTime) > 0 && lastUpdated.CompareTo(curData.currentDateTime) < 0)
                                    {

                                        if (LoggingLevel >= 2)
                                            if (isMarketoActivity)
                                            {
                                                ApplicationLog.WriteToLog(curData.configurationId, "Attempting to update " + curData.pivObject + " record with Marketo Id: " + marketoGUID, System.Diagnostics.EventLogEntryType.Information,
                                                                        curData.mktoObject, marketoGUID, "MKTO.Server.ServiceTask.Integration", Method, "");
                                            }
                                            else
                                            {
                                                ApplicationLog.WriteToLog(curData.configurationId, "Attempting to update " + curData.pivObject + " record with Marketo Id: " + marketoLeadId, System.Diagnostics.EventLogEntryType.Information,
                                                                        curData.mktoObject, marketoLeadId, "MKTO.Server.ServiceTask.Integration", Method, "");
                                            }

                                        //Get list of updated fields from Marketo

                                        string xmlPath = "";

                                        if (curData.isMarketoActivity)
                                        {
                                            xmlPath = "attributes/item";
                                        }
                                        else
                                        {
                                            xmlPath = "fields/item";
                                        }
                                        XmlNodeList xmlFields = xmlRecord.SelectNodes(xmlPath);

                                        if (xmlFields.Count > 0)
                                        {
                                            DataTable pivRecord = null;

                                            if (isMarketoActivity)
                                            {
                                                pivRecord = this.DefaultDataAccess.GetDataTable(curData.queryName, new object[] { marketoGUID });
                                            }
                                            else
                                            {
                                                pivRecord = this.DefaultDataAccess.GetDataTable(curData.queryName, new object[] { marketoId }, pivFields);
                                            }

                                            DataRow curRecord = null;

                                            if (pivRecord.Rows.Count == 0)
                                            {
                                                //If record doesn't exist, create record in Pivotal
                                                curRecord = this.DefaultDataAccess.GetNewDataRow(curData.pivObject, pivFields);
                                                try
                                                {
                                                    curData.pivRecordId = Id.Create(curRecord[curData.pivObject + "_Id"]);
                                                }
                                                catch (Exception e)
                                                { }


                                            }
                                            else
                                            {
                                                //Update existing record
                                                curRecord = pivRecord.Rows[0];
                                                try
                                                {
                                                    curData.pivRecordId = Id.Create(curRecord[curData.pivObject + "_Id"]);
                                                }
                                                catch (Exception e)
                                                { }

                                            }


                                            //Cycle through fields in XML
                                            foreach (XmlNode xmlFieldInfo in xmlFields)
                                            {
                                                //If column exists in mapping, update value
                                                if (curData.fieldMapping.ContainsKey(xmlFieldInfo["name"].InnerText))
                                                {
                                                    if (pivRecord.Columns.Contains(curData.fieldMapping[xmlFieldInfo["name"].InnerText]))
                                                    {
                                                        System.Type fldType = curRecord.Table.Columns[curData.fieldMapping[xmlFieldInfo["name"].InnerText]].DataType;
                                                        if (curData.isMarketoActivity)
                                                        {
                                                            if (xmlFieldInfo["value"].InnerText != "")
                                                            {
                                                                if (LoggingLevel >= 2)
                                                                    ApplicationLog.WriteToLog(curData.configurationId,
                                                                        "Updating " + curData.pivObject + "." + curData.fieldMapping[xmlFieldInfo["name"].InnerText] + " = " + xmlFieldInfo["value"].InnerText, System.Diagnostics.EventLogEntryType.Information,
                                                                                            curData.mktoObject, marketoGUID, "MKTO.Server.ServiceTask.Integration", Method, null);

                                                                try
                                                                {
                                                                    if (fldType.Name == "DateTime")
                                                                    {
                                                                        curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = TypeConvert.ToDateTime(xmlFieldInfo["value"].InnerText);
                                                                    }
                                                                    else if (fldType.Name == "Int32" || fldType.Name == "Int64")
                                                                    {
                                                                        curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = xmlFieldInfo["value"].InnerText;
                                                                    }
                                                                    else if (fldType.Name == "Double")
                                                                    {
                                                                        curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = TypeConvert.ToDouble(xmlFieldInfo["value"].InnerText);
                                                                    }
                                                                    else if (fldType.Name == "Boolean")
                                                                    {
                                                                        curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = TypeConvert.ToBoolean(xmlFieldInfo["value"].InnerText);
                                                                    }
                                                                    else
                                                                    {

                                                                        // Compare the length of data coming in from Marketo with the Pivotal field data length.
                                                                        // If the data is longer than the Pivotal field, trim it to fit in the Pivotal field
                                                                        int pivFieldLength = curRecord.Table.Columns[curData.fieldMapping[xmlFieldInfo["name"].InnerText]].MaxLength;
                                                                        int marketoDataLength = xmlFieldInfo["value"].InnerText.Length;

                                                                        if (marketoDataLength > pivFieldLength)
                                                                        {
                                                                            curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = xmlFieldInfo["value"].InnerText.Substring(0,pivFieldLength-1);
                                                                        }
                                                                        else
                                                                        {
                                                                            curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = xmlFieldInfo["value"].InnerText;
                                                                        }
                                                                    }
                                                                }
                                                                catch (Exception exc)
                                                                {
                                                                    string errMsg = string.Format("Error processing {0}: Marketo GUID {1} Pivotal field {2} ", curData.pivObject,marketoGUID, curData.fieldMapping[xmlFieldInfo["name"].InnerText]);
                                                                    ApplicationLog.WriteToLog(curData.configurationId, errMsg, System.Diagnostics.EventLogEntryType.Error,
                                                                        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);

                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            if (LoggingLevel >= 2)
                                                                ApplicationLog.WriteToLog(curData.configurationId,
                                                                    "Updating " + curData.pivObject + "." + curData.fieldMapping[xmlFieldInfo["name"].InnerText] + " = " + xmlFieldInfo["newValue"].InnerText, System.Diagnostics.EventLogEntryType.Information,
                                                                                        curData.mktoObject, marketoLeadId, "MKTO.Server.ServiceTask.Integration", Method, "");

                                                            if (xmlFieldInfo["newValue"].InnerText != "")
                                                            {
                                                                if (fldType.Name == "DateTime")
                                                                {
                                                                    curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = TypeConvert.ToDateTime(xmlFieldInfo["newValue"].InnerText);
                                                                }
                                                                else if (fldType.Name == "Int32" || fldType.Name == "Int64")
                                                                {
                                                                    curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = xmlFieldInfo["newValue"].InnerText;
                                                                }
                                                                else if (fldType.Name == "Double")
                                                                {
                                                                    curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = TypeConvert.ToDouble(xmlFieldInfo["newValue"].InnerText);
                                                                }
                                                                else if (fldType.Name == "Boolean")
                                                                {
                                                                    curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = TypeConvert.ToBoolean(xmlFieldInfo["newValue"].InnerText);
                                                                }
                                                                else
                                                                {
                                                                    curRecord[curData.fieldMapping[xmlFieldInfo["name"].InnerText]] = xmlFieldInfo["newValue"].InnerText;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            if (curData.fieldMapping.ContainsKey("leadId"))
                                            {
                                                curRecord[curData.fieldMapping["leadId"]] = marketoLeadId;
                                                // Comment out for BlueShore
                                                //Id leadId = this.DefaultDataAccess.SqlFind("SFA_Lead", "Marketo_Id", marketoLeadId);
                                                //curRecord["SFA_Lead_Id"] = TypeConvert.ToDBValue(leadId);

                                                Id contactId = this.DefaultDataAccess.SqlFind("Contact", "Marketo_Id", marketoLeadId);
                                                curRecord["Contact_Id"] = TypeConvert.ToDBValue(contactId);
                                            }
                                            if (curData.fieldMapping.ContainsKey("id"))
                                            {
                                                curRecord[curData.fieldMapping["id"]] = marketoLeadId;
                                            }
                                            if (curData.fieldMapping.ContainsKey("marketoGUID"))
                                            {
                                                curRecord[curData.fieldMapping["marketoGUID"]] = marketoGUID;
                                            }
                                            if (curData.fieldMapping.ContainsKey("activityDate"))
                                            {
                                                curRecord[curData.fieldMapping["activityDate"]] = lastUpdated;
                                            }
                                            if (curData.fieldMapping.ContainsKey("primaryAttributeValue"))
                                            {
                                                curRecord[curData.fieldMapping["primaryAttributeValue"]] = primaryAttributeValue;
                                            }

                                            // Save changes to Pivotal
                                            // We want to save the record as a transaction, which is why instead of simply calling this.defaultDataAccess.SaveDataRow
                                            // we call SaveRecordInPivotal via ExecuteServerTask, even though this method is part of this class.
                                            // By doing it this way, we can save this record as a transaction
                                            // and not get the warning message in the Event Viewer saying this operation should be performed within a transaction

                                            try
                                            {
                                                strRecordId = (string)this.SystemServer.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "SaveRecordInPivotal", new Type[] { typeof(DataRow) }, new object[] { curRecord }, true);
                                            }
                                            catch (Exception exc)
                                            {
                                                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred while attempting to save Pivotal record", System.Diagnostics.EventLogEntryType.Error,
                                                    curData.pivObject, strRecordId, "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                                            }

                                            //try
                                            //{
                                            //    drSavedDataRow = this.DefaultDataAccess.SaveDataRow(curRecord);
                                            //}
                                            //catch (Exception exc)
                                            //{
                                            //    ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                            //        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);

                                            //}

                                            try
                                            {
                                                //curData.pivRecordId = Id.Create(drSavedDataRow[curData.pivObject + "_Id"]);
                                                curData.pivRecordId = Id.Create(strRecordId);
                                            }
                                            catch (Exception e)
                                            { }


                                            if (LoggingLevel >= 1)
                                            {
                                                if (curData.pivRecordId != null)
                                                {
                                                    if (isMarketoActivity)
                                                    {
                                                        ApplicationLog.WriteToLog(curData.configurationId, "Successfully updated Pivotal " + curData.pivObject + " record with Marketo Id: " + marketoGUID, System.Diagnostics.EventLogEntryType.Information,
                                                                                curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                                    }
                                                    else
                                                    {
                                                        ApplicationLog.WriteToLog(curData.configurationId, "Successfully updated Pivotal " + curData.pivObject + " record with Marketo Id: " + marketoLeadId, System.Diagnostics.EventLogEntryType.Information,
                                                                                curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                                    }
                                                }
                                                else
                                                {
                                                    if (isMarketoActivity)
                                                    {
                                                        ApplicationLog.WriteToLog(curData.configurationId, "Successfully updated Pivotal " + curData.pivObject + " record with Marketo Id: " + marketoGUID, System.Diagnostics.EventLogEntryType.Information,
                                                                                curData.pivObject, "", "MKTO.Server.ServiceTask.Integration", Method, "");
                                                    }
                                                    else
                                                    {
                                                        ApplicationLog.WriteToLog(curData.configurationId, "Successfully updated Pivotal " + curData.pivObject + " record with Marketo Id: " + marketoLeadId, System.Diagnostics.EventLogEntryType.Information,
                                                                                curData.pivObject, "", "MKTO.Server.ServiceTask.Integration", Method, "");
                                                    }
                                                }
                                            }
                                            proceedToNextRecord = true;
                                        }
                                        //else
                                        //{
                                        //    ApplicationLog.WriteToLog(curData.configurationId, "GC Else clause reached", System.Diagnostics.EventLogEntryType.Information,
                                        //        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);

                                        //    //Check to see if new record was added to Marketo
                                        //    xmlFields = xmlRecord.SelectNodes("attributes/item");

                                        //    //Cycle through fields in XML
                                        //    foreach (XmlNode xmlFieldInfo in xmlFields)
                                        //    {
                                        //        //Check to see if it is a new record
                                        //        if (xmlFieldInfo["name"].InnerText == "Source Type" && xmlFieldInfo["value"].InnerText == "New person")
                                        //        {
                                        //            //Retrieve Lead record
                                        //            //mktoURL = curData.mktoRestURL + "/v1/lead/" + marketoId + ".json?access_token="
                                        //            //    + curData.mktoOAuthToken;
                                        //            mktoURL = curData.mktoRestURL + "/v1/lead/" + marketoId + ".json";

                                        //            XmlDocument xDoc2 = utility.CallMarketoRestAPI(mktoURL, String.Empty, String.Empty, String.Empty);

                                        //            //Create new Lead in Pivotal
                                        //            DataRow curRecord = this.DefaultDataAccess.GetNewDataRow(curData.pivObject, pivFields);

                                        //            //Get list of updated fields from Marketo
                                        //            XmlNode xmlNewFields = xDoc2.SelectSingleNode("root/result/item");

                                        //            //Cycle through fields in mapping
                                        //            foreach (KeyValuePair<string, string> kvp in curData.fieldMapping)
                                        //            {
                                        //                //If column exists in mapping, update value
                                        //                if (xmlNewFields[kvp.Key].InnerText != "")
                                        //                {
                                        //                    if (curRecord.Table.Columns.Contains(kvp.Value))
                                        //                    {
                                        //                        System.Type fldType = curRecord.Table.Columns[kvp.Value].DataType;
                                        //                        if (fldType.Name == "DateTime")
                                        //                        {
                                        //                            curRecord[kvp.Value] = TypeConvert.ToDateTime(xmlNewFields[kvp.Key].InnerText);
                                        //                        }
                                        //                        else if (fldType.Name == "Int32" || fldType.Name == "Int64")
                                        //                        {
                                        //                            curRecord[kvp.Value] = xmlNewFields[kvp.Key].InnerText;
                                        //                        }
                                        //                        else if (fldType.Name == "Double")
                                        //                        {
                                        //                            curRecord[kvp.Value] = TypeConvert.ToDouble(xmlNewFields[kvp.Key].InnerText);
                                        //                        }
                                        //                        else if (fldType.Name == "Boolean")
                                        //                        {
                                        //                            curRecord[kvp.Value] = TypeConvert.ToBoolean(xmlNewFields[kvp.Key].InnerText);
                                        //                        }
                                        //                        else
                                        //                        {
                                        //                            curRecord[kvp.Value] = xmlNewFields[kvp.Key].InnerText;
                                        //                        }
                                        //                    }
                                        //                }
                                        //            }

                                        //            try
                                        //            {
                                        //                //Save changes to Pivotal
                                        //                //this.DefaultDataAccess.SaveDataRow(curRecord);

                                        //                this.SystemServer.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "SaveRecordInPivotal", new Type[] { typeof(DataRow) }, new object[] { curRecord }, true);
                                        //            }
                                        //            catch (Exception exc)
                                        //            {
                                        //                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                        //                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);

                                        //            }
                                        //        }
                                        //    }
                                        //}
                                    }
                                }
                            }
                            else
                            {
                                XmlNode xmlErrors = xDoc.SelectSingleNode("root/errors");
                                XmlNodeList xmlErrorList = xmlErrors.SelectNodes("item");
                                string errorList = "Marketo Error: ";

                                foreach (XmlNode xmlError in xmlErrorList)
                                {
                                    if (xmlError["code"].InnerText == "606")
                                    {
                                        error606 = true;
                                    }
                                    
                                    if (xmlError["code"].InnerText == "602")
                                    {
                                        error602 = true;
                                    }


                                    errorList += xmlError["code"].InnerText + ", " + xmlError["message"].InnerText + " ";
                                }
                                // Put error message into error Log
                                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, errorList);

                                // If we encounter either a 606 or 602 error, simply wait 3 seconds and attempt to reprocess again
                                if (error606 || error602)
                                {
                                    System.Threading.Thread.Sleep(3000);
                                    if (curData.isMarketoActivity || curData.marketoActivityId != null)
                                    {
                                        ApplicationLog.WriteToLog(curData.configurationId, "Attempting to reprocess Marketo activity id: " + curData.marketoActivityId,
                                            System.Diagnostics.EventLogEntryType.Information,
                                            curData.mktoObject, curData.marketoActivityId, "MKTO.Server.ServiceTask.Integration", Method, "");
                                    }
                                    else if (curData.pivRecordId !=null)
                                    {
                                        ApplicationLog.WriteToLog(curData.configurationId, "Attempting to reprocess record id: " + curData.pivRecordId.ToString(),
                                            System.Diagnostics.EventLogEntryType.Information,
                                            curData.mktoObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");

                                    }
                                    else
                                    {
                                        ApplicationLog.WriteToLog(curData.configurationId, "Attempting to reprocess",
                                            System.Diagnostics.EventLogEntryType.Information,
                                            curData.mktoObject, null, "MKTO.Server.ServiceTask.Integration", Method, "");
                                    }
                                    proceedToNextRecord = false;
                                }
                                // Otherwise skip to the next record.
                                else
                                {
                                    proceedToNextRecord = true;
                                }
                            }
                        }
                    } while (moreResults);
                }
                else
                {
                    OKToUpdateLastRunDate = false;
                    ApplicationLog.WriteToLog(curData.configurationId, "There was no Auth Token.  Integration did not execute", System.Diagnostics.EventLogEntryType.Warning,
        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }
                ApplicationLog.WriteToLog(curData.configurationId, "Completed running integration: " + curData.integrationName, System.Diagnostics.EventLogEntryType.Information,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);

                return OKToUpdateLastRunDate;

            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return false;
            }
        }

        /// <summary>
        ///     Return a list of newly created leads.  To see which leads are newly created,
        ///     we query Narketo for activity type 12, which is New Lead
        /// </summary>
        /// <paramref name="integrationDetailId">Marketo_Detail_Integration_Id</paramref>
        /// <history>
        /// </history>
        private List<string> NewlyCreatedLeads(Id integrationDetailId)
        {
            string Method = "NewlyCreatedLeads";
            List<string> leadLists = new List<string>();
            string leadList = "";

            try
            {
                int maxListSize = 30;
                SetApplicationLog();

                GetIntegrationDtlData(integrationDetailId);
                string token = utility.GetMarketoToken();

                if (LoggingLevel >= 2)
                {
                    if (token != null)
                    {
                        ApplicationLog.WriteToLog(curData.configurationId, "Here is the Auth Token: " + curData.mktoOAuthToken, System.Diagnostics.EventLogEntryType.Information,
            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    }
                }

//                if (curData.mktoOAuthToken !=null && curData.mktoOAuthToken.Length > 0)
                if (token !=null && token.Length > 0)
                {
                    DateTime sinceDateTime = curData.lastRunDateTime;
                    string pagingToken = GetPagingToken(sinceDateTime);
                    bool moreResults = false;
                    bool isMarketoActivity = curData.isMarketoActivity;

                    if (LoggingLevel > 1)
                        ApplicationLog.WriteToLog(curData.configurationId, "Updating Pivotal records updated after:" + sinceDateTime, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");

                    // Keep looping as long as there are results to process from the JSON call to Marketo
                    // Each call to Marketo retrieves up to 300 results at a time
                    // If the JSON message returns a node named moreResult, that means we need to continue
                    // calling Marketo to get the next set of results
                    do // while (moreResults)
                    {
                        string mktoURL = "";
                        string mktoParams = "";

                        // Get a list of leads that are newly created
                        //mktoURL = curData.mktoRestURL + "/v1/activities.json?access_token=" + curData.mktoOAuthToken
                        //    + "&activityTypeIds=12&nextPageToken=" + pagingToken;
                        mktoURL = curData.mktoRestURL + "/v1/activities.json";
                        mktoParams= "&activityTypeIds=12&nextPageToken=" + pagingToken;
                        if (LoggingLevel >= 2)
                            ApplicationLog.WriteToLog(curData.configurationId, "Calling Marketo using URL: " + mktoURL, System.Diagnostics.EventLogEntryType.Information,
                                null, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                        XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL,mktoParams, String.Empty, String.Empty);

                        if (LoggingLevel >= 2)
                        {
                            if (xDoc != null && xDoc.InnerXml != "" && xDoc.InnerXml != null)
                            {
                                ApplicationLog.WriteToLog(curData.configurationId, "Response from Marketo using URL: " + mktoURL, System.Diagnostics.EventLogEntryType.Information,
                                    null, null, "MKTO.Server.ServiceTask.Integration", Method, xDoc.InnerXml);
                            }
                        }

                        XmlNodeList xmlRecordList = xDoc.SelectNodes("root/result/item/leadId");

                        //Get next paging token
                        pagingToken = TypeConvert.ToString(xDoc.SelectSingleNode("root/nextPageToken").InnerText);
                        //Check to see if there are more pages
                        moreResults = TypeConvert.ToBoolean(xDoc.SelectSingleNode("root/moreResult").InnerText);

                        int i = 0;
                        foreach (XmlNode xmlRecord in xmlRecordList)
                        {
                            int leadId = 0;
                            leadId = TypeConvert.ToInt32(xmlRecord.InnerText);

                            if (leadId !=0)
                            {
                                leadList += TypeConvert.ToString(leadId) + ",";
                                i++;
                            }
                            if (i>=maxListSize)
                            {
                                leadLists.Add(leadList);
                                leadList = "";
                                i = 0;
                            }
                        }
                    } while (moreResults);
                }
                else
                {
                    OKToUpdateLastRunDate = false;
                    ApplicationLog.WriteToLog(curData.configurationId, "There was no Auth Token.", System.Diagnostics.EventLogEntryType.Warning,
        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                }

                if (leadList != "")
                {
                    leadLists.Add(leadList);
                }
                
                for (int i = 0; i < leadLists.Count();i++)
                {
                    if (leadLists[i].EndsWith(","))
                    {
                        leadLists[i] = leadLists[i].TrimEnd(new char[] { ',' });
                    }    
                }

                return leadLists;
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return leadLists;
            }
        }


        /// <summary>
        ///     This method is used to update the Pivotal records from Marketo for newly created records
        ///     
        /// </summary>
        /// <history>
        /// <paramref name="integrationDetailId">Marketo_Detail_Integration_Id</paramref>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual bool UpdatePivotalRecordFromNew(Id integrationDetailId)
        {
            string Method = "UpdatePivotalRecordFromNew";
            try
            {
                OKToUpdateLastRunDate = true;
                // Retrieve a list of newly created leads
                List<string> leadLists = NewlyCreatedLeads(integrationDetailId);

                if (ApplicationLog == null)
                    ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();

                GetIntegrationDtlData(integrationDetailId);
                string token = utility.GetMarketoToken();


                if (LoggingLevel >= 2)
                {
//                  if (curData.mktoOAuthToken != null)
                    if (token != null)
                    {
                            ApplicationLog.WriteToLog(curData.configurationId, "Here is the Auth Token: " + curData.mktoOAuthToken, System.Diagnostics.EventLogEntryType.Information,
            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    }
                }

//              if (curData.mktoOAuthToken!=null && curData.mktoOAuthToken.Length > 0)

                if (token!=null && token.Length > 0)
                {
                    // Populate field mapping object. Records are retrieved from Marketo
                    GetFieldMapping("Marketo");

                    // Get list of fields to retrieve from Marketo
                    string fieldListing = "";
                    int i = 0;
                    foreach (string fKey in curData.fieldMapping.Keys)
                    {
                        if (i > 0)
                            fieldListing += ",";
                        fieldListing += fKey;
                        i++;
                    }

                    DateTime sinceDateTime = curData.lastRunDateTime;
                    string pagingToken = GetPagingToken(sinceDateTime);
                    bool moreResults = false;
                    bool isMarketoActivity = curData.isMarketoActivity;

                    ApplicationLog.WriteToLog(curData.configurationId, "Running integration: " + curData.integrationName, System.Diagnostics.EventLogEntryType.Information,
                        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "Direction: " + curData.dataDirection + " Previous run: " + TypeConvert.ToString(curData.lastRunDateTime));

                    if (LoggingLevel > 1)
                        ApplicationLog.WriteToLog(curData.configurationId, "Updating Pivotal records updated after:" + sinceDateTime, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");


                    foreach (string leadList in leadLists)
                    {
                        string mktoURL = "";
                        string mktoParams = "";
                        string strRecordId = "";

                        //mktoURL = curData.mktoRestURL + "/v1/leads.json?access_token=" + curData.mktoOAuthToken
                        //    + "&filterType=id"
                        //    + "&filterValues=" + leadList
                        //    + "&fields=" + fieldListing;
                        mktoURL = curData.mktoRestURL + "/v1/leads.json";
                        mktoParams = "&filterType=id"
                            + "&filterValues=" + leadList
                            + "&fields=" + fieldListing;
                        
                        bool proceedToNextRecord = false;

                        // Marketo has a limit where you can only make 100 API calls within 20 seconds.
                        // If you exceed that, you will get a 606 error.
                        // This while loop allows us to reprocess the record if we encounter a 606 error.  
                        // We pause for 3 seconds and then attempt to reprocess the record

                        while (!proceedToNextRecord)
                        {


                            if (LoggingLevel >= 2)
                                ApplicationLog.WriteToLog(curData.configurationId, "Calling Marketo using URL: " + mktoURL + mktoParams, System.Diagnostics.EventLogEntryType.Information,
                                    null, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                            XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL, mktoParams, String.Empty, String.Empty);

                            if (LoggingLevel >= 2)
                            {
                                if (xDoc != null && xDoc.InnerXml != "" && xDoc.InnerXml != null)
                                {
                                    ApplicationLog.WriteToLog(curData.configurationId, "Response from Marketo using URL: " + mktoURL, System.Diagnostics.EventLogEntryType.Information,
                                        null, null, "MKTO.Server.ServiceTask.Integration", Method, xDoc.InnerXml);
                                }
                            }

                            XmlNodeList nodeList = xDoc.GetElementsByTagName("success");
                            bool error606 = false;
                            bool error602 = false;

                            if (TypeConvert.ToBoolean(nodeList.Item(0).InnerText) == true)
                            {

                                XmlNodeList xmlRecordList = xDoc.SelectNodes("root/result/item");

                                if (LoggingLevel >= 1)
                                    ApplicationLog.WriteToLog(curData.configurationId, "Change count: " + xmlRecordList.Count.ToString(), System.Diagnostics.EventLogEntryType.Information,
                                                                null, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                                //Get list of fields from Pivotal
                                string[] pivFields = new string[curData.fieldMapping.Count];

                                curData.fieldMapping.Values.CopyTo(pivFields, 0);

                                //Cycle through all records and update corresponding Pivotal record
                                foreach (XmlNode xmlRecord in xmlRecordList)
                                {
                                    //Get Marketo Id from XML
                                    int marketoId = 0;
                                    string marketoLeadId = "";
                                    if (xmlRecord["id"] != null)
                                    {
                                        marketoId = TypeConvert.ToInt32(xmlRecord["id"].InnerText);
                                        marketoLeadId = TypeConvert.ToString(xmlRecord["id"].InnerText);
                                    }

                                    if (marketoId > 0)
                                    {
                                        if (LoggingLevel >= 2)
                                            ApplicationLog.WriteToLog(curData.configurationId, "Attempting to update " + curData.pivObject + " record with Marketo Id: " + marketoLeadId, System.Diagnostics.EventLogEntryType.Information,
                                                                    curData.mktoObject, marketoLeadId, "MKTO.Server.ServiceTask.Integration", Method, "");

                                        //Get list of updated fields from Marketo
                                        XmlNodeList xmlFields = xmlRecord.ChildNodes;

                                        if (xmlFields.Count > 0)
                                        {
                                            DataTable pivRecord = null;

                                            pivRecord = this.DefaultDataAccess.GetDataTable(curData.queryName, new object[] { marketoId }, pivFields);

                                            DataRow curRecord = null;

                                            if (pivRecord.Rows.Count == 0)
                                            {
                                                //If record doesn't exist, create record in Pivotal
                                                curRecord = this.DefaultDataAccess.GetNewDataRow(curData.pivObject, pivFields);
                                                try
                                                {
                                                    curData.pivRecordId = Id.Create(curRecord[curData.pivObject + "_Id"]);
                                                }
                                                catch (Exception e)
                                                { }

                                            }
                                            else
                                            {
                                                //Update existing record
                                                curRecord = pivRecord.Rows[0];
                                                try
                                                {
                                                    curData.pivRecordId = Id.Create(curRecord[curData.pivObject + "_Id"]);
                                                }
                                                catch (Exception e)
                                                { }

                                            }

                                            //Cycle through fields in XML
                                            foreach (XmlNode xmlFieldInfo in xmlFields)
                                            {
                                                if (xmlFieldInfo.InnerText != "")
                                                {
                                                    //If column exists in mapping, update value
                                                    if (curData.fieldMapping.ContainsKey(xmlFieldInfo.Name))
                                                    {
                                                        if (pivRecord.Columns.Contains(curData.fieldMapping[xmlFieldInfo.Name]))
                                                        {
                                                            if (LoggingLevel >= 2)
                                                                ApplicationLog.WriteToLog(curData.configurationId,
                                                                    "Updating " + curData.pivObject + "." + curData.fieldMapping[xmlFieldInfo.Name] + " = " + xmlFieldInfo.InnerText, System.Diagnostics.EventLogEntryType.Information,
                                                                                        curData.mktoObject, marketoLeadId, "MKTO.Server.ServiceTask.Integration", Method, "");
                                                            System.Type fldType = curRecord.Table.Columns[curData.fieldMapping[xmlFieldInfo.Name]].DataType;
                                                            if (fldType.Name == "DateTime")
                                                            {
                                                                curRecord[curData.fieldMapping[xmlFieldInfo.Name]] = TypeConvert.ToDateTime(xmlFieldInfo.InnerText);
                                                            }
                                                            else if (fldType.Name == "Int32" || fldType.Name == "Int64")
                                                            {
                                                                curRecord[curData.fieldMapping[xmlFieldInfo.Name]] = xmlFieldInfo.InnerText;
                                                            }
                                                            else if (fldType.Name == "Double")
                                                            {
                                                                curRecord[curData.fieldMapping[xmlFieldInfo.Name]] = TypeConvert.ToDouble(xmlFieldInfo.InnerText);
                                                            }
                                                            else if (fldType.Name == "Boolean")
                                                            {
                                                                curRecord[curData.fieldMapping[xmlFieldInfo.Name]] = TypeConvert.ToBoolean(xmlFieldInfo.InnerText);
                                                            }
                                                            else
                                                            {
                                                                curRecord[curData.fieldMapping[xmlFieldInfo.Name]] = xmlFieldInfo.InnerText;
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            if (curData.fieldMapping.ContainsKey("id"))
                                            {
                                                curRecord[curData.fieldMapping["id"]] = marketoLeadId;
                                            }

                                            // Save changes to Pivotal
                                            // We want to save the record as a transaction, which is why instead of simply calling this.defaultDataAccess.SaveDataRow
                                            // we call SaveRecordInPivotal via ExecuteServerTask, even though this method is part of this class.
                                            // By doing it this way, we can save this record as a transaction
                                            // and not get the warning message in the Event Viewer saying this operation should be performed within a transaction

                                            //this.DefaultDataAccess.SaveDataRow(curRecord);

                                            try
                                            {
                                                this.SystemServer.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "SaveRecordInPivotal", new Type[] { typeof(DataRow) }, new object[] { curRecord }, true);
                                            }
                                            catch (Exception exc)
                                            {
                                                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred while attempting to save Pivotal record", System.Diagnostics.EventLogEntryType.Error,
                                                    curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                                            }

                                            if (LoggingLevel >= 1)
                                            {
                                                if (curData.pivRecordId != null)
                                                {
                                                    ApplicationLog.WriteToLog(curData.configurationId, "Successfully updated Pivotal " + curData.pivObject + " record with Marketo Id: " + marketoId, System.Diagnostics.EventLogEntryType.Information,
                                                                            curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                                }
                                                else
                                                {
                                                    ApplicationLog.WriteToLog(curData.configurationId, "Successfully updated Pivotal " + curData.pivObject + " record with Marketo Id: " + marketoId, System.Diagnostics.EventLogEntryType.Information,
                                                                            curData.pivObject, "", "MKTO.Server.ServiceTask.Integration", Method, "");
                                                }
                                            }

                                        }
                                        //else
                                        //{
                                        //    ApplicationLog.WriteToLog(curData.configurationId, "GC Else clause reached", System.Diagnostics.EventLogEntryType.Information,
                                        //        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);


                                        //    //Check to see if new record was added to Marketo
                                        //    xmlFields = xmlRecord.SelectNodes("attributes/item");

                                        //    //Cycle through fields in XML
                                        //    foreach (XmlNode xmlFieldInfo in xmlFields)
                                        //    {
                                        //        //Check to see if it is a new record
                                        //        if (xmlFieldInfo["name"].InnerText == "Source Type" && xmlFieldInfo["value"].InnerText == "New person")
                                        //        {
                                        //            //Retrieve Lead record
                                        //            //mktoURL = curData.mktoRestURL + "/v1/lead/" + marketoId + ".json?access_token="
                                        //            //    + curData.mktoOAuthToken;
                                        //            mktoURL = curData.mktoRestURL + "/v1/lead/" + marketoId + ".json";

                                        //            XmlDocument xDoc2 = utility.CallMarketoRestAPI(mktoURL, String.Empty, String.Empty, String.Empty);

                                        //            //Create new Lead in Pivotal
                                        //            DataRow curRecord = this.DefaultDataAccess.GetNewDataRow(curData.pivObject, pivFields);

                                        //            //Get list of updated fields from Marketo
                                        //            XmlNode xmlNewFields = xDoc2.SelectSingleNode("root/result/item");

                                        //            //Cycle through fields in mapping
                                        //            foreach (KeyValuePair<string, string> kvp in curData.fieldMapping)
                                        //            {
                                        //                //If column exists in mapping, update value
                                        //                if (xmlNewFields[kvp.Key].InnerText != "")
                                        //                {
                                        //                    if (curRecord.Table.Columns.Contains(kvp.Value))
                                        //                    {
                                        //                        System.Type fldType = curRecord.Table.Columns[kvp.Value].DataType;
                                        //                        if (fldType.Name == "DateTime")
                                        //                        {
                                        //                            curRecord[kvp.Value] = TypeConvert.ToDateTime(xmlNewFields[kvp.Key].InnerText);
                                        //                        }
                                        //                        else if (fldType.Name == "Int32" || fldType.Name == "Int64")
                                        //                        {
                                        //                            curRecord[kvp.Value] = xmlNewFields[kvp.Key].InnerText;
                                        //                        }
                                        //                        else if (fldType.Name == "Double")
                                        //                        {
                                        //                            curRecord[kvp.Value] = TypeConvert.ToDouble(xmlNewFields[kvp.Key].InnerText);
                                        //                        }
                                        //                        else if (fldType.Name == "Boolean")
                                        //                        {
                                        //                            curRecord[kvp.Value] = TypeConvert.ToBoolean(xmlNewFields[kvp.Key].InnerText);
                                        //                        }
                                        //                        else
                                        //                        {
                                        //                            curRecord[kvp.Value] = xmlNewFields[kvp.Key].InnerText;
                                        //                        }
                                        //                    }
                                        //                }
                                        //            }

                                        //            //Save changes to Pivotal
                                        //            //this.DefaultDataAccess.SaveDataRow(curRecord);
                                        //            this.SystemServer.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "SaveRecordInPivotal", new Type[] { typeof(DataRow) }, new object[] { curRecord }, true);
                                        //        }
                                        //    }
                                        //}
                                    }
                                }
                                proceedToNextRecord = true;
                            }
                            else // there was an error in the REST call
                            {
                                XmlNode xmlErrors = xDoc.SelectSingleNode("root/errors");
                                XmlNodeList xmlErrorList = xmlErrors.SelectNodes("item");
                                string errorList = "Marketo Error: ";

                                foreach (XmlNode xmlError in xmlErrorList)
                                {
                                    if (xmlError["code"].InnerText == "606")
                                    {
                                        error606 = true;
                                    }
                                    if (xmlError["code"].InnerText == "602")
                                    {
                                        error606 = true;
                                    }
                                    errorList += xmlError["code"].InnerText + ", " + xmlError["message"].InnerText + " ";
                                }
                                // Put error message into error Log
                                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, errorList);

                                // If we encounter a 606 or 602 error, simply wait 3 seconds and attempt to reprocess again
                                if (error606 || error602)
                                {
                                    System.Threading.Thread.Sleep(3000);
                                    if (curData.pivRecordId != null)
                                    {

                                        ApplicationLog.WriteToLog(curData.configurationId, "Attempting to reprocess: record id: " + curData.pivRecordId.ToString(),
                                        System.Diagnostics.EventLogEntryType.Information,
                                        curData.mktoObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                    }
                                    else
                                    {
                                        ApplicationLog.WriteToLog(curData.configurationId, "Attempting to reprocess: record id: " + curData.pivRecordId.ToString(),
                                        System.Diagnostics.EventLogEntryType.Information,
                                        curData.mktoObject, null, "MKTO.Server.ServiceTask.Integration", Method, "");

                                    }
                                    proceedToNextRecord = false;
                                }
                                else
                                // Otherwise skip to the next record.
                                {
                                    proceedToNextRecord = true;
                                }

                            }
                        } // while ok to proceed
                    } 

                }
                else
                {
                    OKToUpdateLastRunDate = false;
                    ApplicationLog.WriteToLog(curData.configurationId, "There was no Auth Token.  Integration did not execute", System.Diagnostics.EventLogEntryType.Warning,
        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);

                }
                //UpdateLastRunDate();
                ApplicationLog.WriteToLog(curData.configurationId, "Completed running integration: " + curData.integrationName, System.Diagnostics.EventLogEntryType.Information,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                return OKToUpdateLastRunDate;
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return false;
            }
        }

        /// <summary>
        ///     Save the datarow in Pivotal.
        ///     This method exists to be called via this.SystemServer.ExecuteServerTask
        ///     from UpdatePivotalRecord and UpdatePivotalRecordFromNew so that the row 
        ///     can be saved as a separate transaction and not trigger the warning 
        ///     message in the Event Viewer saying this operation should be performed within a transaction
        ///     
        ///     The record is returned as a string because Id cannot be returned when calling 
        ///     this.SystemServer.ExecuteServerTask
        /// </summary>
        /// <history>
        /// <paramref name="curRecord"/>
        /// #Revision   Date    Author  Description
        /// </history>

        [TaskExecute]
        public virtual string SaveRecordInPivotal(DataRow curRecord)
        {
            string tableName = "";
            string strRecordId = "";
            
            tableName = curRecord.Table.TableName;
            DataRow dr=this.DefaultDataAccess.SaveDataRow(curRecord);
            try
            {
                strRecordId=Id.Create(curRecord[tableName + "_Id"]).ToString();
            }
            catch (Exception exc)
            {
            }
            return strRecordId;
        }


        /// <summary>
        ///     This method is used to update the Marketo records from Pivotal
        /// </summary>
        /// <param name="integrationDetailId">Marketo_Integration_Detail_Id</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual bool UpdateMarketoRecord(Id integrationDetailId)
        {
            string Method = "UpdateMarketoRecord";
            try
            {
                bool isCustomObject = false;
                OKToUpdateLastRunDate = true;
                if(ApplicationLog == null)
                    ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();

                GetIntegrationDtlData(integrationDetailId);
                string token = utility.GetMarketoToken();

                ApplicationLog.WriteToLog(curData.configurationId, "Running integration: " + curData.integrationName, System.Diagnostics.EventLogEntryType.Information,
    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "Direction: " + curData.dataDirection + " Previous run: " + TypeConvert.ToString(curData.lastRunDateTime));

                if (LoggingLevel >= 2)
                {
//                    if (curData.mktoOAuthToken != null)
                    if (token!= null)
                    {
                        ApplicationLog.WriteToLog(curData.configurationId, "Here is the Auth Token: " + curData.mktoOAuthToken, System.Diagnostics.EventLogEntryType.Information,
            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                    }
                }

//                if (curData.mktoOAuthToken !=null && curData.mktoOAuthToken.Length > 0)
                if (token != null && token.Length > 0)
                {
                    // Populate field mapping object. Records are retrieved from Pivotal
                    GetFieldMapping("Pivotal");

                    // Custom objects end with _c
                    if (curData.mktoObject.ToLower().EndsWith("_c"))
                    {
                        isCustomObject = true;
                    }

                    //Retrieve Pivotal records
                    DataTable dtPivotal = this.DefaultDataAccess.GetDataTable(curData.pivQuery);

                    if (LoggingLevel >= 1)
                    {
                        ApplicationLog.WriteToLog(curData.configurationId, "Updating Marketo records updated after:" + curData.lastRunDateTime.ToString(), System.Diagnostics.EventLogEntryType.Information,
                                 "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                    }

                    if (LoggingLevel >= 2)
                    {
                        ApplicationLog.WriteToLog(curData.configurationId, "Pivotal query:" + curData.pivQuery, System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                        ApplicationLog.WriteToLog(curData.configurationId, curData.pivObject + " records found: " + dtPivotal.Rows.Count.ToString(), System.Diagnostics.EventLogEntryType.Information,
                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                    }

                    if (dtPivotal.Rows.Count > 0)
                    {

                        StringBuilder jsonRecord = new StringBuilder();
                        StringBuilder jsonPrefix = new StringBuilder();
                        int gcDebug = 0;

                        //Cycle through all Pivotal records
                        foreach (DataRow dr in dtPivotal.Rows)
                        {
                            try
                            {

                                int i = 0;

                                gcDebug++;

                                curData.pivRecordId = Id.Create(dr[curData.pivObject + "_Id"]);

                                if (LoggingLevel >= 1)
                                {
                                    ApplicationLog.WriteToLog(curData.configurationId, "Processing " + curData.pivObject + " records id: " + curData.pivRecordId.ToString(),
                                        System.Diagnostics.EventLogEntryType.Information,
                                        curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                }

                                //Create JSON string for transfer to Marketo
                                jsonRecord.Clear();
                                jsonPrefix.Clear();
                                jsonRecord.AppendLine("\"input\":[{");
                                foreach (DataColumn col in dtPivotal.Columns)
                                {
                                    //Ignore any fields that are not part of the mapping
                                    if (curData.fieldMapping.ContainsKey(col.ColumnName))
                                    {
                                        // If we are looking at the Unique Identifier
                                        if (curData.fieldMapping[col.ColumnName] == curData.mktoPKFieldName)
                                        {
                                            jsonPrefix.AppendLine("{");
                                            if (isCustomObject)
                                            {
                                                jsonPrefix.AppendLine("\"dedupeBy\":\"dedupeFields\",");
                                            }
                                            else
                                            {
                                                if (dr[col.ColumnName] != DBNull.Value)
                                                {
                                                    jsonPrefix.AppendLine("\"lookupField\":\"id\",");
                                                    if (i > 0)
                                                        jsonRecord.Append(",");
                                                    jsonRecord.AppendLine("\"" + curData.fieldMapping[col.ColumnName] + "\":\"" + dr[col.ColumnName] + "\"");
                                                    i++;

                                                }
                                            }
                                            curData.mktoRecordId = TypeConvert.ToString(dr[col.ColumnName]);
                                        }
                                        else
                                        {
                                            if (i > 0)
                                                jsonRecord.Append(",");
                                            System.Type fldType = col.DataType;
                                            if (fldType.Name == "DateTime" && TypeConvert.ToString(dr[col.ColumnName]) != "")
                                                //Format date field so Marketo will accept it
                                                jsonRecord.AppendLine("\"" + curData.fieldMapping[col.ColumnName] + "\":\"" +
                                                    DateTime.Parse(TypeConvert.ToString(dr[col.ColumnName])).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") + "\"");
                                            else
                                                jsonRecord.AppendLine("\"" + curData.fieldMapping[col.ColumnName] + "\":\"" + dr[col.ColumnName] + "\"");
                                            i++;
                                        }
                                    }
                                }

                                if (jsonPrefix.ToString() == "")
                                    jsonPrefix.AppendLine("{");

                                jsonRecord.AppendLine("}] }");

                                //Setup URL for record update
                                string mktoURL = "";

                                if (isCustomObject)
                                {
                                    //mktoURL = curData.mktoRestURL + "/v1/customobjects/" + curData.mktoObject.ToLower() + ".json?access_token="
                                    //        + curData.mktoOAuthToken;
                                    mktoURL = curData.mktoRestURL + "/v1/customobjects/" + curData.mktoObject.ToLower() + ".json";
                                }
                                else
                                {
                                    //mktoURL = curData.mktoRestURL + "/v1/" + curData.mktoObject.ToLower() + ".json?access_token="
                                    //        + curData.mktoOAuthToken;
                                    mktoURL = curData.mktoRestURL + "/v1/" + curData.mktoObject.ToLower() + ".json";
                                }

                                if (LoggingLevel >= 2)
                                {
                                    ApplicationLog.WriteToLog(curData.configurationId, "POST URL: " + mktoURL, System.Diagnostics.EventLogEntryType.Information,
                                        curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                    ApplicationLog.WriteToLog(curData.configurationId, "JSON Message: " + jsonPrefix.ToString() + jsonRecord.ToString(), System.Diagnostics.EventLogEntryType.Information,
                                        curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                }

                                bool proceedToNextRecord = false;


                                // Marketo has a limit where you can only make 100 API calls within 20 seconds.
                                // If you exceed that, you will get a 606 error.
                                // This while loop allows us to reprocess the record if we encounter a 606 error.  
                                // We pause for 3 seconds and then attempt to reprocess the record
                                //
                                // There is also a rare instance where the token expires just before we make a REST
                                // call but after we had done a check to see if the token had actually expired.  This 
                                // results in a 602 error
                                // So if we do encounter a 602 error, we will also just wait 3 seconds before attempting to reprocess
                                while (!proceedToNextRecord)
                                {
                                    XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL, String.Empty, "POST",
                                        jsonPrefix.ToString() + jsonRecord.ToString());

                                    XmlNodeList nodeList = xDoc.GetElementsByTagName("success");
                                    bool error606 = false;
                                    bool error602 = false;
                                    if (TypeConvert.ToBoolean(nodeList.Item(0).InnerText) == true)
                                    {
                                        //Get details of result
                                        nodeList = xDoc.GetElementsByTagName("item");

                                        //Check to see if updated or created
                                        XmlNode intResult = nodeList.Item(0);

                                        if (intResult["status"].InnerText == "created")
                                        {
                                            string mktoRecordId = "";
                                            if (isCustomObject)
                                            {
                                                mktoRecordId = intResult["marketoGUID"].InnerText;
                                            }
                                            else
                                            {
                                                mktoRecordId = intResult["id"].InnerText;
                                            }

                                            try
                                            {
                                                //Update Pivotal record with Marketo Id
                                                this.SystemServer.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "SetPivotalExternalSourceId",
                                                    new Type[] { typeof(string), typeof(Id), typeof(string), typeof(string), typeof(Id) },
                                                    new object[] { curData.pivObject, curData.pivRecordId, mktoRecordId, curData.pivPKFieldName, curData.configurationId }, true);

                                                if (LoggingLevel >= 2)
                                                {
                                                    ApplicationLog.WriteToLog(curData.configurationId, "Updated field " + curData.pivPKFieldName + " with value " + mktoRecordId,
                                                        System.Diagnostics.EventLogEntryType.Information,
                                                        curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);
                                                }

                                                if (LoggingLevel >= 1)
                                                {
                                                    ApplicationLog.WriteToLog(curData.configurationId, "Created Marketo Record: " + mktoRecordId + " from Pivotal " + curData.pivObject + " record id: " + curData.pivRecordId.ToString(), System.Diagnostics.EventLogEntryType.Information,
                                                        curData.mktoObject, mktoRecordId, "MKTO.Server.ServiceTask.Integration", Method, "");
                                                }
                                            }
                                            catch (Exception exc)
                                            {
                                                ApplicationLog.WriteToLog(ConfigurationId, "Error occurred attenmpting to Update field " + curData.pivPKFieldName + " with value " + mktoRecordId,
                                                    System.Diagnostics.EventLogEntryType.Information,
                                                    curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                                            }

                                            //SetPivotalExternalSourceId(curData.pivObject, curData.pivRecordId, mktoRecordId);

                                        }
                                        else if (intResult["status"].InnerText == "skipped")
                                        {
                                            ApplicationLog.WriteToLog(curData.configurationId, "Skipped processing Pivotal " + curData.pivObject + " record id: " + curData.pivRecordId.ToString(), System.Diagnostics.EventLogEntryType.Error,
                                                curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                            XmlNodeList xmlReason = intResult.SelectNodes("reasons");
                                            string errorList = "Marketo Error: ";
                                            foreach (XmlNode xmlReasonItem in xmlReason)
                                            {
                                                if (xmlReasonItem.SelectSingleNode("item/code") != null)
                                                    errorList += xmlReasonItem.SelectSingleNode("item/code").InnerText + ", "
                                                        + xmlReasonItem.SelectSingleNode("item/message").InnerText;
                                            }

                                            // Report reason for skipping record
                                            ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                            curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, errorList);
                                        }
                                        proceedToNextRecord = true;
                                    }
                                    else
                                    {
                                        XmlNode xmlErrors = xDoc.SelectSingleNode("root/errors");
                                        XmlNodeList xmlErrorList = xmlErrors.SelectNodes("item");
                                        string errorList = "Marketo Error: ";

                                        foreach (XmlNode xmlError in xmlErrorList)
                                        {
                                            if (xmlError["code"].InnerText == "606")
                                            {
                                                error606 = true;
                                            }
                                            else if (xmlError["code"].InnerText == "602")
                                            {
                                                error602 = true;
                                            }    

                                            errorList += xmlError["code"].InnerText + ", " + xmlError["message"].InnerText + " ";
                                        }
                                        // Put error message into error Log
                                        ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                                            "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, errorList);

                                        // If we encounter a 606 or 602 error, simply wait 3 seconds and attempt to reprocess again
                                        if (error606 || error602 )
                                        {
                                            System.Threading.Thread.Sleep(3000);
                                            ApplicationLog.WriteToLog(curData.configurationId, "Attempting to reprocess: record id: " + curData.pivRecordId.ToString(),
                                                System.Diagnostics.EventLogEntryType.Information,
                                                curData.mktoObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, "");
                                            proceedToNextRecord = false;
                                        }
                                        else
                                        // Otherwise skip to the next record.
                                        {
                                            proceedToNextRecord = true;
                                        }
                                    }
                                }
                            }
                            catch (Exception exc)
                            {
                                ApplicationLog.WriteToLog(curData.configurationId, "Error Processing " + curData.pivObject + " records id: " + curData.pivRecordId.ToString(),
                                    System.Diagnostics.EventLogEntryType.Error,
                                    curData.pivObject, curData.pivRecordId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);

                            }
                        }
                    }
                }
                else
                {
                    OKToUpdateLastRunDate= false;
                    ApplicationLog.WriteToLog(curData.configurationId, "There was no Auth Token.  Integration did not execute" , System.Diagnostics.EventLogEntryType.Warning,
        "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);

                }
                //UpdateLastRunDate();
                ApplicationLog.WriteToLog(curData.configurationId, "Completed integration: " + curData.integrationName, System.Diagnostics.EventLogEntryType.Information,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, null);

                return OKToUpdateLastRunDate;
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    "Marketo_Integration_Detail", integrationDetailId.ToString(), "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return false;
            }
        }

        /// <summary>
        ///     This method is used to retrieve Marketo fields from specified object
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual string[] GetMarketoObjectFields(Id configurationId, string mktoObject)
        {
            const string Method = "GetMarketoObjectFields";
            try
            {
                SetApplicationLog();
                //Retrieve connection info for Marketo
                GetConfigurationData(configurationId);

                List<string> mktoFields = new List<string>();
                string mktoURL = "";
                bool customObject = false;

                if (mktoObject.ToLower().EndsWith("_c"))
                {
                    customObject = true;
                }

                if (customObject)
                {
                    //mktoURL = curData.mktoRestURL + "/v1/customobjects/" + mktoObject.ToLower() + "/describe.json?access_token=" + curData.mktoOAuthToken;
                    mktoURL = curData.mktoRestURL + "/v1/customobjects/" + mktoObject.ToLower() + "/describe.json";
                }
                else
                {
                    //else if (mktoObject.ToLower() == "leads")
                    //mktoURL = curData.mktoRestURL + "/v1/" + mktoObject.ToLower() + "/describe.json?access_token=" + curData.mktoOAuthToken;
                    mktoURL = curData.mktoRestURL + "/v1/" + mktoObject.ToLower() + "/describe.json";
                }
                //else
                //    mktoURL = curData.mktoRestURL + "/v1/" + mktoObject.ToLower() + "/roles/describe.json?access_token=" + curData.mktoOAuthToken;

                XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL, String.Empty,String.Empty, String.Empty);
                XmlNodeList nodeList = null;

                if (TypeConvert.ToBoolean(xDoc.SelectSingleNode("/root/success").InnerText)==false)
                {
                    string marketoError = parseMarketoErrors(xDoc);
                    throw new Exception(marketoError);
                }

                if (customObject)
                {
                    nodeList = xDoc.SelectNodes("/root/result/item/fields/item");
                }
                else
                {
                    nodeList = xDoc.GetElementsByTagName("item");
                }

                foreach (XmlNode fldNode in nodeList)
                {
                    if (customObject)
                    {
                        mktoFields.Add(fldNode["name"].InnerText);

                    }
                    else
                    {
                        mktoFields.Add(fldNode["rest"]["name"].InnerText);
                    }
                }
                mktoFields.Sort((x, y) => string.Compare(x, y));
                return mktoFields.ToArray();
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(configurationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return null;
            }
        }

        /// <summary>
        ///     This method is used to retrieve Marketo fields from specified object
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        [TaskExecute]
        public virtual string[] GetMarketoActivityFields(Id configurationId, string mktoActivity)
        {
            SetApplicationLog();

            //Retrieve connection info for Marketo
            GetConfigurationData(configurationId);

            List<string> mktoActivityFields = new List<string>();
            string mktoURL = "";
            //mktoURL = curData.mktoRestURL + "/v1/activities/types.json?access_token=" + curData.mktoOAuthToken;
            mktoURL = curData.mktoRestURL + "/v1/activities/types.json";

            XmlDocument xDoc = utility.CallMarketoRestAPI(mktoURL, String.Empty,String.Empty, String.Empty);
            XmlNodeList nodeList = xDoc.SelectNodes("//root/result/item");

            List<string> fieldLists = new List<string>();

            foreach (XmlNode n in nodeList)
            {
                string activityType = n["name"].InnerText;
                if (activityType == mktoActivity)
                {
                    //string primaryAttributeName = n["primaryAttribute"]["name"].InnerText;
                    //mktoActivityFields.Add(primaryAttributeName);

                    XmlNodeList attributesList = n["attributes"].ChildNodes;
                    foreach (XmlNode attributeNode in attributesList)
                    {
                        mktoActivityFields.Add(attributeNode["name"].InnerText);
                    }
                }
            }
            mktoActivityFields.Add("primaryAttributeValue");
            mktoActivityFields.Add("marketoGUID");
            mktoActivityFields.Add("leadId");
            mktoActivityFields.Add("activityDate");
            mktoActivityFields.Sort((x, y) => string.Compare(x, y));
            return mktoActivityFields.ToArray();
        }

        /// <summary>
        ///     Instantiate the ApplicationLog object if it hasn't already been instantiated
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        private void SetApplicationLog()
        {
            if (ApplicationLog==null) ApplicationLog = (Logging)this.SystemServer.GetMetaItem<ServerTask>("MKTO.Server.ServiceTask.Logging").CreateInstance();
        }

        /// <summary>
        ///     Parse the message returned by Marketo to return
        ///     any errors returned by Marketo
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        private string parseMarketoErrors (XmlDocument doc)
        {
            const string Method = "GetMarketoObjectFields";
            try
            {
                string errorMessages = "";
                XmlNodeList errorNodes = doc.SelectNodes("/root/errors/item");
                int i = 1;
                foreach (XmlNode errorNode in errorNodes)
                {
                    string code = errorNode["code"].InnerText;
                    string message = errorNode["message"].InnerText;

                    errorMessages += "(" + i.ToString() + ") Code: " + code + " Message: " + message + " ";
                }
                return errorMessages;
            }
            catch (Exception exc)
            {
                ApplicationLog.WriteToLog(curData.integrationDtlId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                    null, null, "MKTO.Server.ServiceTask.Integration", Method, exc.Message);
                return "";
            }
        }

        #endregion
    }
}


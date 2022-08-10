using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using CdcSoftware.Pivotal.Applications.Core.Client;
using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.Types.Security;
using CdcSoftware.Pivotal.Engine.UI.Forms;
using CdcSoftware.Pivotal.Applications.Core.Common;

namespace MKTO.Client.FormTasks
{
    public partial class MarketoConfiguration: FormClientTask
    {
        #region Constructor
        public MarketoConfiguration()
        {
        }

        #endregion

        #region Private Fields
        private string tableLabel;
        private string tableId;
        private string tableName;
        private bool tableNameChanged;
        #endregion

        #region Protected Properties
        /// <summary>
        /// tableCollection Dictionary
        /// </summary>
        protected Dictionary<string, string> tableCollection;
        /// <summary>
        /// Table Name
        /// </summary>
        protected string TableName
        { get; set; }
        /// <summary>
        /// Table lable
        /// </summary>
        protected string TableLabel
        { get; set; }
        /// <summary>
        /// Table Id
        /// </summary>
        protected string TableId
        { get; set; }
        /// <summary>
        /// Integration Object
        /// </summary>
        protected virtual PivotalDropDown tableNameDropDown
        {
            get
            {
                return this.FormControl.GetControlByName(MarketoConfigurationData.FormControl.TableNameDropDown) as PivotalDropDown;
            }
        }
        protected virtual PivotalGroupBox resultsGroupBox
        {
            get
            {
                return this.FormControl.GetControlByName(MarketoConfigurationData.FormControl.ResultsGroupBox) as PivotalGroupBox;
            }
        }
        protected virtual PivotalGroupBox recordsGroupBox
        {
            get
            {
                return this.FormControl.GetControlByName(MarketoConfigurationData.FormControl.RecordsGroupBox) as PivotalGroupBox;
            }
        }
        #endregion

        #region Protected Interface




        #endregion

        #region Public Methods
        public override void OnFormInitialized()
        {
            base.OnFormInitialized();

            this.SharedLoad();
        }

        public override void OnFormReloaded()
        {
            base.OnFormReloaded();
           // this.InitializeForm();
        }
        #endregion

        #region Private Methods
        /// <summary>
        /// Adds Columns to the secondary for configuration of the search.
        /// </summary>
        protected virtual void AddColumnNames()
        {
            try
            {
                if (string.IsNullOrEmpty(tableNameDropDown.Text))
                {
                    return;
                }
                Table selectedTable = this.SystemClient.UserProfile.GetMetaItem<Table>(Id.Create(this.tableId));
                DataTable dataTableCriteria = this.DataSet.Tables[MarketoConfigurationData.FormControl.ChooseColumnDataSegment];
                var columns = selectedTable.Columns;

                //add columns not present in secondary but belong to table
                var newColumns = from newColumn in columns.AsEnumerable()
                                 where !((from temp in dataTableCriteria.AsEnumerable()
                                          where temp.RowState != DataRowState.Deleted
                                          select temp["Pivotal_Field"] as string).Contains(newColumn.Name))
                                 select newColumn;
                if (dataTableCriteria == null)
                { return; }
                foreach (Column column in newColumns)
                {
                    if (!(column.IsPrimaryKey || column.IsSystemDefined || column.DataType.BaseType == BaseType.Memo || column.DataType.BaseType == BaseType.Binary
                        || column.DataType.BaseType == BaseType.Graphic || column.Name.Contains("Delta")))
                    {
                        this.NewSecondaryRecord(MarketoConfigurationData.FormControl.ChooseColumnDataSegment);

                       // dataTableCriteria.Rows[dataTableCriteria.Rows.Count - 1][MarketoConfigurationData.Field.MatchType] = MarketoConfigurationData.MatchType.Equal;
                        dataTableCriteria.Rows[dataTableCriteria.Rows.Count - 1]["Pivotal_Field"] = column.Name;
                       // dataTableCriteria.Rows[dataTableCriteria.Rows.Count - 1][MarketoConfigurationData.Field.FieldLabel] = column.DisplayName;
                       // dataTableCriteria.Rows[dataTableCriteria.Rows.Count - 1][MarketoConfigurationData.Field.FieldFKValue] = column.Id.ToString();
                    }
                }
                //so that deleted rows get committed
                dataTableCriteria.AcceptChanges();

            }
            catch (Exception ex)
            {
                //show message
                Globals.HandleException(ex, true);
            }
        }

        /// <summary>
        /// Updates hidden fields for Duplicate_Check primary table
        /// </summary>
        /// <param name="tablePluralName">table plural name</param>
        protected virtual void SetTableValues(string tablePluralName)
        {
            //get table name
            TableCollection tableCollection = this.SystemClient.UserProfile.GetMetaCollection<TableCollection>();

            var matchingTables = from selectedTable in tableCollection.AsEnumerable()
                                 where selectedTable.DisplayName == tablePluralName || selectedTable.Name == tablePluralName
                                 select selectedTable;
            Table targetTable = matchingTables.First<Table>();
            //update the value in primary 
 //           this.PrimaryDataRow[MarketoConfigurationData.Field.TableName] = targetTable.Name;
 //           this.PrimaryDataRow[MarketoConfigurationData.Field.TableFKValue] = targetTable.Id.ToString();
 //           this.PrimaryDataRow[MarketoConfigurationData.Field.TableNameLabel] = string.IsNullOrEmpty(targetTable.DisplayName) ? targetTable.Name : targetTable.DisplayName;
            this.tableId = targetTable.Id.ToString();
            this.tableLabel = string.IsNullOrEmpty(targetTable.DisplayName) ? targetTable.Name : targetTable.DisplayName;
            this.tableName = targetTable.Name;
        }

        private void SharedLoad()
        {
            PivotalDropDown logType = (PivotalDropDown)this.FormControl.GetControlByDataName("Log_Type");

            //Enable/Disable fields based on Enable_Logging value
            if (TypeConvert.ToBoolean(this.PrimaryDataRow["Log_Enable"]) == true)
            {
                logType.Required = true;

                if (TypeConvert.ToString(logType.Value) == "File")
                    EnableDisableFields(false, true);
                else
                    EnableDisableFields(true, false);
            }
            else
            {
                EnableDisableFields(false, false);
            }
        }
        #endregion

        #region Protected Methods
        protected void InitializeForm()
        {
            /*
            try
            {
                //clear the collection 
                tableNameDropDown.Items.Clear();
                TableCollection tableCollection = this.SystemClient.UserProfile.GetMetaCollection<TableCollection>();
                
                this.tableNameChanged = false;
                DataTable dataTableExistingCriteria = Globals.GetDataTable(MarketoConfigurationData.Table.TableName, new string[] { MarketoConfigurationData.Field.TableFKValue });

                //filter existing and excluded tables, tables sorted by plural name, those with empty will be on top 
                var addTables = (from newTables in tableCollection.AsEnumerable()
                                 where !((from temp in dataTableExistingCriteria.AsEnumerable()
                                          select temp[MarketoConfigurationData.Field.TableFKValue] as string).Contains(newTables.Id.ToString()))
                                 select newTables).OrderBy(table => table.DisplayName);

                //add items in the dropdown
                foreach (Table table in addTables)
                {
                    TableSettings tableSettings = this.SystemClient.UserProfile.GetMetaSettings<TableSettings>(table);

                    if (tableSettings.CanRead && tableSettings.CanInsert && !table.Name.StartsWith("Rn_")
                        && !table.Name.StartsWith("Prox"))
                    {
                        tableNameDropDown.Items.Add(!String.IsNullOrEmpty(table.DisplayName) ? table.DisplayName : table.Name);
                    }
                }
                string tempName = this.PrimaryDataRow[MarketoConfigurationData.Field.TableName].ToString();
                if (this.RecordId != null && !string.IsNullOrEmpty(tempName))
                {
                   
                    this.tableId = this.PrimaryDataRow[MarketoConfigurationData.Field.TableFKValue].ToString();
                    this.tableName = this.PrimaryDataRow[MarketoConfigurationData.Field.TableName].ToString();
                    this.tableLabel = this.PrimaryDataRow[MarketoConfigurationData.Field.TableNameLabel].ToString();
                    tableNameDropDown.Value = this.tableLabel;

                    //add target table to the dropdown
                    if (!tableNameDropDown.Items.Contains(this.tableLabel))
                    {
                        tableNameDropDown.Items.Insert(0, this.tableLabel);
                    }

                }
                else
                {
                    //resultsGroupBox.Visible = true;
                    //recordsContainer.Visible = true;
                    this.TaskPad.Visible = false;
                    Id empId = this.SystemClient.UserProfile.EmployeeId;
                    if (empId != null)
                    {
                        this.PrimaryDataRow[MarketoConfigurationData.Field.EmployeeId] = empId.ToByteArray();
                    }
                }

                //add column names
                this.AddColumnNames();

                this.tableNameChanged = true;
                //setting dirty flag to false
                this.FormView.Dirty = false;

            }
            catch (Exception ex)
            {
                Globals.HandleException(ex, true);
            }
            */
        }

        #endregion
    }
}

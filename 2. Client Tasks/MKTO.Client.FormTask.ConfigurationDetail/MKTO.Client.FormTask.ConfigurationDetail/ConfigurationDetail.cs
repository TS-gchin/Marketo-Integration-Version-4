using System;
using System.Windows.Forms;
using CdcSoftware.Pivotal.Applications.Core.Client;
using CdcSoftware.Pivotal.Applications.Core.Common;
using CdcSoftware.Pivotal.Applications.Core.Data.Element;
using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Client.ClientTasks.Attributes;
using CdcSoftware.Pivotal.Engine.Client.Services.Interfaces;
using CdcSoftware.Pivotal.Engine.Types.Localization;

using CdcSoftware.Pivotal.Engine.Client.Services.Actions;
using CdcSoftware.Pivotal.Engine.Client.Services.DataAccess;
using CdcSoftware.Pivotal.Engine.Types.DataTemplates;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.UI;
using CdcSoftware.Pivotal.Engine.UI.Forms;

using CdcSoftware.Pivotal.Engine.Types.ServerTasks;
using System.Linq.Expressions;

namespace MKTO.Client.FormTasks
{
    /// <summary>
    /// The Form Client Task class to undertake some specfic tasks for one business object associated to a specified form. It will call
    /// Form Server Task and Service Server Task.
    /// </summary>
    /// <history>
    /// #Revision   Date    Author  Description
    /// </history>
    public partial class ConfigurationDetail : FormClientTask
    {
        #region Private fields
        // TODO (PIV) Confirm with the correct ServerTaskProxy type for this class. You can choose the ServerTaskProxy
        // pointing to the ServerTask that is associated to the current Smart Client Form.
        // private CompanyProxy m_defaultServerTaskProxy;
        #endregion

        #region Constructor
        /// <summary>
        /// Initialize the instance of AppFormClientTask class and set the default resource bundle to 'xxxxxx' LD Group.
        /// </summary>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public ConfigurationDetail()
        {
            // TODO (PIV) Confirm with the correct Resource Bundle name for this class.
            //base.DefaultResourceBundleName = "xxxxxxx";
        }
        #endregion

        #region Protected properties
        /// <summary>
        /// Gets or sets default ServerTaskProxy reference for the current Client Task.  
        /// </summary>
        // TODO (PIV) Confirm with the correct ServerTaskProxy type for this class.
        //protected CompanyProxy DefaultServerTaskProxy
        //{
        //    get
        //    {
        //        if (m_defaultServerTaskProxy == null)
        //        {
        //            m_defaultServerTaskProxy = new CompanyProxy(this.DataTemplate);
        //        }
        //        return m_defaultServerTaskProxy;
        //    }
        //    set
        //    {
        //        m_defaultServerTaskProxy = value;
        //    }
        //}
        #endregion

        #region Public methods
        /// <summary>
        /// Save a new record into database
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public virtual bool AddRecord()
        {
            // TODO (PIV) Add your business logic here. 
            return base.SaveRecord();
        }

        /// <summary>
        /// This function deletes the current Form
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public override bool DeleteRecord()
        {
            // TODO (PIV) Add your business logic here. If you do not have any business logic,
            // you can remove this override method.
            return base.DeleteRecord();
        }

        /// <summary>
        /// This function opens an existing form
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public override bool LoadRecord()
        {
            // TODO (PIV) If you do not have different business logic for NewRecord, you can remove the if - else block.
            if (this.FormData.RecordId == null)
            {
                return this.NewRecord();
            }
            else
            {
                // TODO (PIV) Add your business logic here. If you do not have any business logic,
                // you can remove this override method.
                return base.LoadRecord();
            }
        }

        /// <summary>
        /// This method gets called when the form meta data and data have both been loaded,
        /// and the form UI has finished drawing. Override this method to perform custom
        /// form initialization.
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public override void OnFormInitialized()
        {
            base.OnFormInitialized();

            //Populate Pivotal Table dropdown
            PivotalDropDown pivTables = (PivotalDropDown)this.FormControl.GetControlByName("PivotalTable");
            PivotalDropDown parentTable = (PivotalDropDown)this.FormControl.GetControlByName("ParentTable");
            PivotalDropDown childTable = (PivotalDropDown)this.FormControl.GetControlByName("ChildTable");
            PivotalDropDown relOperator = (PivotalDropDown)this.FormControl.GetControlByName("RelOperator");

            pivTables.Items.Clear();
            parentTable.Items.Clear();
            childTable.Items.Clear();
            relOperator.Items.Clear();

            //Populate Table dropdown fields with list of tables
            TableCollection tableCollection = this.SystemClient.UserProfile.GetMetaCollection<TableCollection>();

            foreach (Table curTable in tableCollection)
            {
                pivTables.Items.Add(curTable.Name);
                parentTable.Items.Add(curTable.Name);
                childTable.Items.Add(curTable.Name);
            }

            //Populate field type dropdowns with available values
            relOperator.Items.Add("AND");
            relOperator.Items.Add("OR");

            ToggleMarketoObjectActivity();
            bool isMarketoActvity = TypeConvert.ToBoolean(this.PrimaryDataRow["Is_Marketo_Activity"]);
            if (isMarketoActvity)
            {
                UpdateMarketoActivityFieldNameDropdown();
            }
            else
            {
                UpdateMarketoFieldNameDropdown();
            }
            DataDirectionUpdated();
        }

        /// <summary>
        /// This function opens an new form
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public virtual bool NewRecord()
        {
            // TODO (PIV) Add your business logic here. If you do not have any business logic,
            // you can remove this override method.
            return base.LoadRecord();
        }


        /// <summary>
        /// This function updates the Form
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public override bool SaveRecord()
        {
            //Reset Marketo Object value since field is actually on table
            //this.PrimaryDataRow["Marketo_Object"] = null;
            // TODO (PIV) If you do not have different business logic for AddRecord, you can remove the if - else block.
            if (this.FormData.RecordId == null)
            {
                return this.AddRecord();
            }
            else
            {
                // TODO (PIV) Add your business logic here. If you do not have any business logic,
                // you can remove this override method.
                return base.SaveRecord();
            }
        }

        public void ToggleMarketoObjectActivity()
        {
            PivotalCheckBox chkIsMarketo = (PivotalCheckBox)this.FormControl.GetControlByDataName("Is_Marketo_Activity");
            PivotalLabel lblMarketoObject = (PivotalLabel)this.FormControl.GetControlByName("lblMarketoObject");
            PivotalLabel lblMarketActivity = (PivotalLabel)this.FormControl.GetControlByName("lblMarketoActivityTypesId");
            PivotalDropDown ddMarketoObject = (PivotalDropDown)this.FormControl.GetControlByName("MarketoObject");
            PivotalForeignKey ddMarketoActivity = (PivotalForeignKey)this.FormControl.GetControlByDataName("Marketo_Activity_Types_Id");
            string dataDirection = TypeConvert.ToString(this.PrimaryDataRow["Data_Direction"]);
            PivotalDropDown ddDirection = (PivotalDropDown)this.FormControl.GetControlByDataName("Data_Direction");
            PivotalTextBox txtMappingQuery = (PivotalTextBox)this.FormControl.GetControlByDataName("Mapping_Query");

            if (chkIsMarketo.Checked)
            {
                lblMarketoObject.Visible = false;
                ddMarketoObject.Visible = false;
                lblMarketActivity.Visible = true;
                ddMarketoActivity.Visible = true;
                lblMarketActivity.Location = new System.Drawing.Point(382, 50);
                ddMarketoActivity.Location = new System.Drawing.Point(481, 46);
                txtMappingQuery.Required = false;

                if (dataDirection != "Marketo To Pivotal")
                {
                    this.PrimaryDataRow["Data_Direction"] = "Marketo To Pivotal";
                    PivotalMessageBox.Show("Marketo Activities can only flow from Marketo to Pivotal", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                ddDirection.ReadOnly = true;
            }
            else
            {
                lblMarketoObject.Visible = true;
                ddMarketoObject.Visible = true;
                lblMarketoObject.Location = new System.Drawing.Point(382, 50);
                ddMarketoObject.Location = new System.Drawing.Point(481, 46);
                lblMarketActivity.Visible = false;
                ddMarketoActivity.Visible = false;
                ddDirection.ReadOnly = false;
                if (dataDirection == "Marketo To Pivotal")
                {
                    txtMappingQuery.Required = true;
                }
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public virtual void UpdateMarketoActivityFieldNameDropdown()
        {
            PivotalDropDown mktoFields = (PivotalDropDown)this.FormControl.GetControlByName("MarketoField");

            mktoFields.Items.Clear();
            mktoFields.Value = "";
            Id marketoActivityTypesId = Id.Create(this.PrimaryDataRow["Marketo_Activity_Types_Id"]);
            if (marketoActivityTypesId != null)
            {
                string marketoActivityName = TypeConvert.ToString(Globals.SqlIndex("Marketo_Activity_Types", "Activity_Type", marketoActivityTypesId));
                if (marketoActivityName !="")
                {
                    //Retrieve Marketo fields from Marketo
                    object result = Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "GetMarketoActivityFields",
                        new Type[] { typeof(Id), typeof(string) },
                        new object[] { Id.Create(this.PrimaryDataRow["Marketo_Configuration_Id"]), marketoActivityName });

                    //Populate Marketo field dropdown
                    string[] fieldArray = (string[])result;

                    for (int i = 0; i < fieldArray.Length; i++)
                    {
                        mktoFields.Items.Add(fieldArray[i]);
                    }
                }
            }
        }

        public virtual void DataDirectionUpdated()
        {
            PivotalDropDown ddDataDirection = (PivotalDropDown)this.FormControl.GetControlByName("DataDirection");
            PivotalTextBox txtMappingQuery = (PivotalTextBox)this.FormControl.GetControlByName("MappingQuery");
            string dataDirection = TypeConvert.ToString(this.PrimaryDataRow["Data_Direction"]);
            bool isMarketoActivity = TypeConvert.ToBoolean(this.PrimaryDataRow["Is_Marketo_Activity"]);

            if (isMarketoActivity == false)
            {
                if (dataDirection == "Marketo To Pivotal" || dataDirection == "Bidirectional")
                {
                    txtMappingQuery.Required = true;
                }
                else
                {
                    txtMappingQuery.Required = false;
                }
            }
            else
            {
                txtMappingQuery.Required = false;
            }
        }

        #endregion
    }
}

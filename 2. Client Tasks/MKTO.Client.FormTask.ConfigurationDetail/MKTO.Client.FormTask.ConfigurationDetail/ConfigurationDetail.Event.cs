using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Windows.Forms;

using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Client.Services.Interfaces;
using CdcSoftware.Pivotal.Engine.Types.DataTemplates;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.Types.Localization;
using CdcSoftware.Pivotal.Engine.UI;
using CdcSoftware.Pivotal.Engine.UI.Forms;

using CdcSoftware.Pivotal.Applications.Core.Client;
using CdcSoftware.Pivotal.Applications.Core.Common;

namespace MKTO.Client.FormTasks
{
    /// <summary>
    /// The Form Client Task class inludes a number of form control event handlers in this file.
    /// The event can call Form Server Task and Service Server Task.
    /// </summary>
    /// <history>
    /// #Revision   Date    Author  Description
    /// </history>
    public partial class ConfigurationDetail : FormClientTask
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnTableNameChange(PivotalControl sender, EventArgs args)
        {
            this.UpdateFieldNameDropdown("PivotalTable", "PivotalField");
            PivotalTextBox pivTableAlias = (PivotalTextBox)this.FormControl.GetControlByName("PivotalTableAlias");
            pivTableAlias.Text = "";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnMarketoObjectChange(PivotalControl sender, EventArgs args)
        {
            this.UpdateMarketoFieldNameDropdown();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnParentTableNameChange(PivotalControl sender, EventArgs args)
        {
            this.UpdateFieldNameDropdown("ParentTable", "ParentField");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnChildTableNameChange(PivotalControl sender, EventArgs args)
        {
            this.UpdateFieldNameDropdown("ChildTable", "ChildField");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnAddMappingButtonClick(PivotalControl sender, EventArgs args)
        {
            if (MappingFieldsEntered())
                this.AddMappingToSecondary();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnAddRelationshipButtonClick(PivotalControl sender, EventArgs args)
        {
            if (RelationshipFieldsEntered())
                this.AddRelationshipToSecondary();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnUpdateMarketoButtonClick(PivotalControl sender, EventArgs args)
        {
              Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord",
                  new Type[] { typeof(Id) },
                  new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });
            
           // Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "ProcessRecords", null );
            PivotalMessageBox.Show("Process Complete", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }


        public virtual void OnReplaySyncButtonClick(PivotalControl sender, EventArgs args)
        {
            string direction = TypeConvert.ToString(this.PrimaryDataRow["Data_Direction"]);
            string marketoObject = TypeConvert.ToString(this.PrimaryDataRow["Marketo_Object"]);
            bool runResult1 = true;
            bool runResult2 = true;
            bool runResult3 = true;
            bool finalRunResult = true;

            if (direction =="Pivotal To Marketo" )
            {
                runResult1=(bool)Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord",
                    new Type[] { typeof(Id) },
                    new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });

                finalRunResult = runResult1;
                if (finalRunResult)
                {
                    PivotalMessageBox.Show("Process Complete", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                else
                {
                    PivotalMessageBox.Show("Process Complete but with errors.  Please check the log", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }

            }
            else if (direction == "Marketo To Pivotal")
            {
                // We need to make 2 calls here.
                // Although Marketo has an API call to retrieve leads that have been updated since Last_Run_Date_Time
                // this does not include newly created leads
                // 
                // The first call, UpdatePivotalRecordFromNew checks to see if there are any newly created leads Last_Run_Date_Time
                // The second call, UpdatePivotalRecord checks to see what updates have been made since Last_Run_Date_Time

                if (marketoObject.ToUpper() == "LEAD")
                {
                    runResult1 = (bool)Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdatePivotalRecordFromNew",
                        new Type[] { typeof(Id) },
                        new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });
                }
                else
                {
                    runResult1 = true;
                }
                runResult2=(bool)Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdatePivotalRecord",
                    new Type[] { typeof(Id) },
                    new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });

                finalRunResult = runResult1 && runResult2;
                if (finalRunResult)
                {
                    PivotalMessageBox.Show("Process Complete", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                else
                {
                    PivotalMessageBox.Show("Process Complete but with errors.  Please check the log", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }

            }
            else if (direction == "Bidirectional")
            {
                runResult1=(bool)Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdateMarketoRecord",
                    new Type[] { typeof(Id) },
                    new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });

                PivotalMessageBox.Show("Process Complete", MessageBoxButtons.OK, MessageBoxIcon.Information);

                // We need to make 2 calls here.
                // Although Marketo has an API call to retrieve leads that have been updated since Last_Run_Date_Time
                // this does not include newly created leads
                // 
                // The first call, UpdatePivotalRecordFromNew checks to see if there are any newly created leads Last_Run_Date_Time
                // The second call, UpdatePivotalRecord checks to see what updates have been made since Last_Run_Date_Time

                runResult2=(bool)Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdatePivotalRecordFromNew",
                    new Type[] { typeof(Id) },
                    new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });
                runResult3=(bool)Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdatePivotalRecord",
                    new Type[] { typeof(Id) },
                    new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });

                finalRunResult = runResult1 && runResult2 && runResult3;
                if (finalRunResult)
                {
                    PivotalMessageBox.Show("Process Complete", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                else
                {
                    PivotalMessageBox.Show("Process Complete but with errors.  Please check the log", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }

            if (finalRunResult == true)
            {
                Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdateLastRunDate",
                    new Type[] { typeof(Id) }, new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });
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
        public virtual void OnUpdatePivotalButtonClick(PivotalControl sender, EventArgs args)
        {
            Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "UpdatePivotalRecord",
                new Type[] { typeof(Id) },
                new object[] { Id.Create(this.PrimaryDataRow["Marketo_Integration_Detail_Id"]) });

            PivotalMessageBox.Show("Process Complete", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public virtual void UpdateFieldNameDropdown(string tableFieldName, string fieldFieldName)
        {
            PivotalDropDown pivFields = (PivotalDropDown)this.FormControl.GetControlByName(fieldFieldName);
            PivotalDropDown pivTable = (PivotalDropDown)this.FormControl.GetControlByName(tableFieldName);

            pivFields.Items.Clear();
            pivFields.Value = "";

            TableCollection tableCollection = this.SystemClient.UserProfile.GetMetaCollection<TableCollection>();
            List<string> fldList = new List<string>();

            foreach (Table curTable in tableCollection)
            {
                if (curTable.Name == TypeConvert.ToString(pivTable.Value))
                {
                    foreach (Column curColumn in curTable.Columns)
                    {
                        //pivFields.Items.Add(curColumn.Name);
                        fldList.Add(curColumn.Name);
                    }
                }
            }
            fldList.Sort();

            for (int i = 0; i < fldList.Count; i++)
            {
                pivFields.Items.Add(fldList[i]);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        public virtual void UpdateMarketoFieldNameDropdown()
        {
            PivotalDropDown mktoFields = (PivotalDropDown)this.FormControl.GetControlByName("MarketoField");

            mktoFields.Items.Clear();
            mktoFields.Value = "";

            if (TypeConvert.ToString(this.PrimaryDataRow["Marketo_Object"]).Length > 0)
            {
                //Retrieve Marketo fields from Marketo
                object result = Globals.SystemClient.ExecuteServerTask("MKTO.Server.ServiceTask.Integration", "GetMarketoObjectFields",
                    new Type[] { typeof(Id), typeof(string) },
                    new object[] { Id.Create(this.PrimaryDataRow["Marketo_Configuration_Id"]), TypeConvert.ToString(this.PrimaryDataRow["Marketo_Object"]) });
                
                //Populate Marketo field dropdown
                string[] fieldArray = (string[])result;

                for (int i = 0; i < fieldArray.Length; i++)
                {
                    mktoFields.Items.Add(fieldArray[i]);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <history>
        /// Revision# Date Author Description
        /// </history>
        protected virtual void AddMappingToSecondary()
        {
            this.NewSecondaryRecord("Marketo_Field_Mapping");
            DataTable dtMapping = this.DataSet.Tables["Marketo_Field_Mapping"];
            PivotalDropDown pivTable = (PivotalDropDown)this.FormControl.GetControlByName("PivotalTable");
            PivotalDropDown pivField = (PivotalDropDown)this.FormControl.GetControlByName("PivotalField");
            PivotalTextBox pivFieldFormula = (PivotalTextBox)this.FormControl.GetControlByName("PivotalFieldFormula");
            PivotalDropDown mktoTable = (PivotalDropDown)this.FormControl.GetControlByName("MarketoObject");
            PivotalForeignKey mktoActivity = (PivotalForeignKey)this.FormControl.GetControlByDataName("Marketo_Activity_Types_Id");
            PivotalDropDown mktoField = (PivotalDropDown)this.FormControl.GetControlByName("MarketoField");
            PivotalCheckBox uniqueIdentifier = (PivotalCheckBox)this.FormControl.GetControlByName("UniqueIdentifier");
            PivotalCheckBox chkIsMarketoActivity = (PivotalCheckBox)this.FormControl.GetControlByName("IsMarketoActivity");

            bool isMarketoActivity = TypeConvert.ToBoolean(chkIsMarketoActivity.Checked);
            //Add new record to secondary            
            dtMapping.Rows[dtMapping.Rows.Count - 1]["Pivotal_Table"] = pivTable.Value;
            dtMapping.Rows[dtMapping.Rows.Count - 1]["Pivotal_Field"] = pivField.Value;
            dtMapping.Rows[dtMapping.Rows.Count - 1]["Pivotal_Field_Formula"] = pivFieldFormula.Text;

            if (isMarketoActivity)
            {
                string activityTypeName = TypeConvert.ToString(Globals.SqlIndex("Marketo_Activity_Types", "Activity_Type", Id.Create(this.PrimaryDataRow["Marketo_Activity_Types_Id"])));
                dtMapping.Rows[dtMapping.Rows.Count - 1]["External_Table"] = activityTypeName;
            }
            else
            {
                dtMapping.Rows[dtMapping.Rows.Count - 1]["External_Table"] = mktoTable.Value;

            }
            dtMapping.Rows[dtMapping.Rows.Count - 1]["External_Field"] = mktoField.Value;
            dtMapping.Rows[dtMapping.Rows.Count - 1]["Unique_Identifier"] = TypeConvert.ToBoolean(uniqueIdentifier.Checked);
            dtMapping.AcceptChanges();

            //Reset field values, except for Table Names and Alias
            pivField.Value = "";
            pivFieldFormula.Text = "";
            mktoField.Value = "";
            uniqueIdentifier.Checked = false;
        }

        protected virtual void AddRelationshipToSecondary()
        {
            PivotalTextBox formulaField = (PivotalTextBox)this.FormControl.GetControlByName("RelationshipFormula");
            PivotalDropDown parentTable = (PivotalDropDown)this.FormControl.GetControlByName("ParentTable");
            PivotalDropDown childTable = (PivotalDropDown)this.FormControl.GetControlByName("ChildTable");
            PivotalDropDown parentField = (PivotalDropDown)this.FormControl.GetControlByName("ParentField");
            PivotalDropDown childField = (PivotalDropDown)this.FormControl.GetControlByName("ChildField");
            PivotalDropDown relOperator = (PivotalDropDown)this.FormControl.GetControlByName("RelOperator");

            this.NewSecondaryRecord("Marketo_Table_Relationship");
            DataTable dtRelationship = this.DataSet.Tables["Marketo_Table_Relationship"];

            if (formulaField.Text.Length == 0)
            {
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Parent_Table"] = parentTable.Value;
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Parent_Field"] = parentField.Value;
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Child_Table"] = childTable.Value;
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Child_Field"] = childField.Value;
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Operator"] = relOperator.Value;
            }
            else
            {
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Operator"] = relOperator.Value;
                dtRelationship.Rows[dtRelationship.Rows.Count - 1]["Formula"] = formulaField.Text;
            }
            dtRelationship.AcceptChanges();

            //Reset field values
            parentTable.Value = "";
            parentField.Value = "";
            childTable.Value = "";
            childField.Value = "";
            formulaField.Text = "";
            relOperator.Value = "";
        }

        private bool MappingFieldsEntered()
        {
            PivotalDropDown pivTable = (PivotalDropDown)this.FormControl.GetControlByName("PivotalTable");
            PivotalDropDown pivField = (PivotalDropDown)this.FormControl.GetControlByName("PivotalField");
            PivotalTextBox pivFldFormula = (PivotalTextBox)this.FormControl.GetControlByName("PivotalFieldFormula");
            PivotalDropDown mktoTable = (PivotalDropDown)this.FormControl.GetControlByName("MarketoObject");
            PivotalDropDown mktoField = (PivotalDropDown)this.FormControl.GetControlByName("MarketoField");
            PivotalCheckBox chkIsMarketoActivity = (PivotalCheckBox)this.FormControl.GetControlByName("IsMarketoActivity");
            PivotalForeignKey mktoActivity = (PivotalForeignKey)this.FormControl.GetControlByName("MarketoActivityTypesId");

            bool isMarketoActivity = chkIsMarketoActivity.Checked;

            if (isMarketoActivity)
            {
                if ((pivTable.Value.ToString() == "" || pivField.Value.ToString() == "" ||
                    mktoActivity.Value.ToString() == "" || mktoField.Value.ToString() == "")
                    && pivFldFormula.Text.ToString() == "")
                {
                    PivotalMessageBox.Show("All mapping fields must be populated", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return false;
                }
            }
            else
            {
                if ((pivTable.Value.ToString() == "" || pivField.Value.ToString() == "" ||
                    mktoTable.Value.ToString() == "" || mktoField.Value.ToString() == "")
                    && pivFldFormula.Text.ToString() == "")
                {
                    PivotalMessageBox.Show("All mapping fields must be populated", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return false;
                }
            }
            return true;
        }

        private bool RelationshipFieldsEntered()
        {
            PivotalTextBox formulaField = (PivotalTextBox)this.FormControl.GetControlByName("RelationshipFormula");

            if (formulaField.Text.Length == 0)
            {
                PivotalDropDown parentTable = (PivotalDropDown)this.FormControl.GetControlByName("ParentTable");
                PivotalDropDown childTable = (PivotalDropDown)this.FormControl.GetControlByName("ChildTable");
                PivotalDropDown parentField = (PivotalDropDown)this.FormControl.GetControlByName("ParentField");
                PivotalDropDown childField = (PivotalDropDown)this.FormControl.GetControlByName("ChildField");
                PivotalDropDown relOperator = (PivotalDropDown)this.FormControl.GetControlByName("RelOperator");

                if (parentTable.Value.ToString() == "" || parentField.Value.ToString() == "" ||
                    childTable.Value.ToString() == "" || childField.Value.ToString() == "" || relOperator.Value.ToString() == "")
                {
                    PivotalMessageBox.Show("All relationship fields must be populated", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return false;
                }

            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void IsMarketoCheckedChanged(PivotalControl sender, EventArgs args)
        {
            
            ToggleMarketoObjectActivity();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender">The control associated with the event triggered</param>
        /// <param name="args">The argument for the event</param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual void OnMarketoActivityChange(PivotalControl sender, EventArgs args)
        {
            this.UpdateMarketoActivityFieldNameDropdown();
        }

        public virtual void OnDataDirectionChanged(PivotalControl sender, EventArgs args)
        {
            this.DataDirectionUpdated();
        }



    }
}

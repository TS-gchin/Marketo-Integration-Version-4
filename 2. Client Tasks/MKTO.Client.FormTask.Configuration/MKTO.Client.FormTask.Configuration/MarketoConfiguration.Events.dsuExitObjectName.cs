using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;

using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.UI.Forms;
using CdcSoftware.Pivotal.Applications.Core.Client;
using CdcSoftware.Pivotal.Engine.Types.Security;

namespace MKTO.Client.FormTasks
{
    public partial class MarketoConfiguration : FormClientTask
    {
        public virtual void dsuTableName(PivotalControl sender, EventArgs args)
        {
            try
            {
                if (tableNameChanged == false)
                {
                    return; // No need to update.
                }
                PivotalDropDown tableNameDropDown = sender as PivotalDropDown;
                if (this.tableLabel == tableNameDropDown.Text)
                {
                    return; // Nothing has changed so return to the caller.
                }
                if (tableNameDropDown.Text != tableLabel && !string.IsNullOrEmpty(tableLabel))
                {
                    DataTable chooseColumnsDataTable = this.DataSet.Tables[MarketoConfigurationData.FormControl.ChooseColumnDataSegment];
                    //clear the criteria
                    chooseColumnsDataTable.RejectChanges();
                    // Clear out the current records.
                    for (int i = 0; i < chooseColumnsDataTable.Rows.Count; i++)
                    {
                        chooseColumnsDataTable.Rows[i].Delete();
                    }
                    // Gets the table specifics.
                    this.SetTableValues(tableNameDropDown.Text);
                    //this.PopulateTargetTable(tableNameDropDown);

                }
                else
                {
                    this.SetTableValues(tableNameDropDown.Text);
                }
               // this.AddColumnNames();
            }
            catch (Exception ex)
            {
                Globals.HandleException(ex, true);
            }
        }
    }
}

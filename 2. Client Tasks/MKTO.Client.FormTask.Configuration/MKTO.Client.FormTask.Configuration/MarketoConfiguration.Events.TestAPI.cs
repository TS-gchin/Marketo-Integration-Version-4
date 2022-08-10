using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Net;
using System.Xml;
using System.Xml.Linq;
using System.IO;
using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.UI.Forms;
using CdcSoftware.Pivotal.Applications.Core.Client;
using CdcSoftware.Pivotal.Engine.Client.Services.Interfaces;
using CdcSoftware.Pivotal.Engine.Types.Security;
using MKTO.Common;

namespace MKTO.Client.FormTasks
{
    public partial class MarketoConfiguration : FormClientTask
    {
        #region Class Objects
        Utility utility = new Utility();
        #endregion
        public virtual void ValidateRESTConfig(PivotalControl sender, EventArgs args)
        {
            try
            {

                bool success = false;
                success = this.TestURL();
                if (!success)
                {
                    
                }
                else
                {
                    PivotalMessageBox.Show("Success");
                }
                this.PrimaryDataRow[MarketoConfigurationData.Field.APIIsValid] = success;
            }
            catch (Exception e)
            {
                Globals.HandleException(e, true);
            }
        }


        protected virtual bool TestURL()
        {
            HttpWebRequest webRequest = null;
            string url = this.BuildRequest();
            try
            {
                webRequest = System.Net.WebRequest.Create(url) as HttpWebRequest;

            }
            catch (UriFormatException)
            {
                return false;
            }
            bool isApiUrlVerified = false;
            try
            {
                HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse();

                XmlDocument responseDoc = utility.ConvertJSONToXML(response.GetResponseStream());

                XmlNodeList nodeList = responseDoc.GetElementsByTagName("errors");

                if (nodeList.Count > 0)
                {
                    nodeList = responseDoc.GetElementsByTagName("message");
                    string errMsg = nodeList.Count > 0 ? nodeList[0].InnerText : string.Empty;
                    isApiUrlVerified = false;
                    PivotalMessageBox.Show(errMsg, System.Windows.Forms.MessageBoxButtons.OK);
                }
                else
                {
                    isApiUrlVerified = true;
                }
                responseDoc = null;
            }
            catch (WebException webexception)
            {
                return false;
            }
            webRequest = null;
            return isApiUrlVerified;
        }
        private string BuildRequest()
        {
            string identity = this.PrimaryDataRow[MarketoConfigurationData.Field.RESTIdentity].ToString();
            string endPoint = this.PrimaryDataRow[MarketoConfigurationData.Field.RESTEndpoint].ToString();
            string secret = this.PrimaryDataRow[MarketoConfigurationData.Field.RESTSecret].ToString();
            string clientId = this.PrimaryDataRow[MarketoConfigurationData.Field.RESTClientId].ToString();
            string userEmail = this.PrimaryDataRow[MarketoConfigurationData.Field.APIUserEmail].ToString();
            string token = utility.GetMarketoToken(identity, clientId, secret); 

            string filterType = "email";
            string urlCallType = "/v1/leads.json?access_token=";
            string builtUrl = string.Empty;
            if (token != this.PrimaryDataRow[MarketoConfigurationData.Field.RESTToken].ToString())
            {
                this.PrimaryDataRow[MarketoConfigurationData.Field.RESTToken] = token;
            }
            builtUrl = endPoint + urlCallType + token + "&filterType=" + filterType + "&" + "filterValues=" + userEmail;

            return builtUrl;
        }
  
    }
}

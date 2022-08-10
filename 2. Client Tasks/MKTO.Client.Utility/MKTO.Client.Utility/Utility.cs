using System;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Xml;
using System.Net;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.Web.Configuration;
using System.Text;



namespace MKTO.Common
{
    public class Utility
    {
        private string identityURL;
        private string clientId;
        private string clientSecret;
        private string token;
        private int remainingSeconds = 0;
        private DateTime tokenExpiry;

        #region Constructor
        public Utility(string mktoIdentityURL, string mktoClientId, string mktoClientSecret)
        {
            identityURL = mktoIdentityURL;
            clientId = mktoClientId;
            clientSecret = mktoClientSecret;

            getMarketoToken(identityURL, clientId, clientSecret);
        }

        public Utility()
        {

        }

        #endregion

        #region Public Methods

        public string GetMarketoToken()
        {
            if (tokenExpiry < DateTime.Now)
            {
                //Refresh the token
                if (!getMarketoToken(identityURL, clientId, clientSecret))
                {
                    throw new Exception("Failure to refresh the token");
                }
            }
            return token;
        }

        /// <summary>
        /// Returns a CSV of strings from a String[]
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public String csvString(String[] args)
        {
            StringBuilder sb = new StringBuilder();
            int i = 1;
            foreach (String s in args)
            {
                if (i < args.Length)
                {
                    sb.Append(s + ",");
                }
                else
                {
                    sb.Append(s);
                }
                i++;
            }
            return sb.ToString();
        }
        /// <summary>
        /// Returns a CSV of strings from an int[]
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public String csvString(int[] args)
        {
            StringBuilder sb = new StringBuilder();
            int i = 1;
            foreach (int s in args)
            {
                if (i < args.Length)
                {
                    sb.Append(s + ",");
                }
                else
                {
                    sb.Append(s);
                }
                i++;
            }
            return sb.ToString();
        }
        /// <summary>
        /// Converts the JSON objects to XML.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns></returns>
        public virtual XmlDocument ConvertJSONToXML(Stream stream)
        {
            XmlDocument document = new XmlDocument();
            using (XmlDictionaryReader xmlReader = JsonReaderWriterFactory.CreateJsonReader(stream, XmlDictionaryReaderQuotas.Max))
            {
                xmlReader.Read();
                document.LoadXml(xmlReader.ReadOuterXml());
            }
            return document;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mktoClientId"></param>
        /// <param name="mktoClientSecret"></param>
        /// <param name="mktoIdentityURL"></param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual string GetMarketoToken(string mktoIdentityURL, string mktoClientId, string mktoClientSecret)
        {
            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

                string sURL = mktoIdentityURL + "?grant_type=client_credentials&client_id=" + mktoClientId + "&client_secret="
                    + mktoClientSecret;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sURL);
                request.ContentType = "application/json";
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream resStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(resStream);
                String json = reader.ReadToEnd();
                Dictionary<string, string> obj = new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(json);

                return obj["access_token"];
            }
            catch (Exception exc)
            {
                throw new Exception(exc.Message + "|| mktoIdentityURL: " + mktoIdentityURL + ", mktoClientId:" + mktoClientId + ", mktoSecret: " + mktoClientSecret + " || Stacktrace: " + exc.StackTrace);
                //return string.Empty;
                //ApplicationLog.WriteToLog(curData.integrationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                //        null, null, "TS.Server.ServiceTasks.TSIntegration", "SFAuthenticate", exc.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mktoClientId"></param>
        /// <param name="mktoClientSecret"></param>
        /// <param name="mktoIdentityURL"></param>
        /// <history>
        /// #Revision   Date    Author  Description
        /// </history>
        public virtual XmlDocument CallMarketoRestAPI(string mktoURL, string mktoURLParams, string methodType, string mktoContent)
        {
            string mktoURLToUse;
            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

                if (tokenExpiry < DateTime.Now)
                {
                    //Refresh the token
                    if (!getMarketoToken(identityURL, clientId, clientSecret))
                    {
                        throw new Exception("Failure to refresh the token");
                    }
                }

                mktoURLToUse = mktoURL + "?access_token=" + token + mktoURLParams;

//                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(mktoURL);
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(mktoURLToUse);

                request.ContentType = "application/json";
                request.Accept = "application/json";


                //Set method type if value exists
                if (methodType.Length > 0)
                    request.Method = methodType;

                //Send content to Marketo if present
                if (mktoContent.Length > 0)
                {
                    StreamWriter sWriter = new StreamWriter(request.GetRequestStream());
                    sWriter.Write(mktoContent);
                    sWriter.Flush();
                }

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream resStream = response.GetResponseStream();

                XmlDocument xDoc = this.ConvertJSONToXML(resStream);

                return xDoc;
            }
            catch (Exception exc)
            {
                throw new Exception(exc.Message + "|| mktoURL: " + mktoURL + ", methodType:" + methodType+ ", mktoContent: " + mktoContent+ " || Stacktrace: " + exc.StackTrace);
                //ApplicationLog.WriteToLog(curData.integrationId, "Error occurred", System.Diagnostics.EventLogEntryType.Error,
                //        null, null, "TS.Server.ServiceTasks.TSIntegration", "SFAuthenticate", exc.Message);
            }
        }
        #endregion

        #region Private Methods
        private bool getMarketoToken(string mktoIdentityURL, string mktoClientId, string mktoClientSecret)
        {
            bool retVal = false;
            try
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

                string sURL = mktoIdentityURL + "?grant_type=client_credentials&client_id=" + mktoClientId + "&client_secret="
                    + mktoClientSecret;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(sURL);
                request.ContentType = "application/json";
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream resStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(resStream);
                String json = reader.ReadToEnd();
                Dictionary<string, string> obj = new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(json);
                if (int.TryParse(obj["expires_in"], out remainingSeconds))
                {
                    tokenExpiry = DateTime.Now.AddSeconds(remainingSeconds - 3);
                    token = obj["access_token"];
                    retVal = true;
                }
                else
                {
                    tokenExpiry = DateTime.Now;
                }

                return retVal;
            }
            catch (Exception exc)
            {
                throw new Exception(exc.Message + "|| mktoIdentityURL: " + mktoIdentityURL + ", mktoClientId:" + mktoClientId + ", mktoSecret: " + mktoClientSecret + " || Stacktrace: " + exc.StackTrace);
            }
        }

        #endregion
    }
}

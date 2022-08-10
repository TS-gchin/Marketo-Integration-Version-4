using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;
using System.Web.Configuration;
using System.Web.Script.Serialization;
using System.Xml;

using CdcSoftware.Pivotal.Applications.Core.Server;
using CdcSoftware.Pivotal.Applications.Core.Common;
using CdcSoftware.Pivotal.Applications.Core.Data.Element;
using CdcSoftware.Pivotal.Engine;
using CdcSoftware.Pivotal.Engine.Types.Database;
using CdcSoftware.Pivotal.Engine.Types.ServerTasks;

using MKTO.Common;
using MKTO.Server.ServiceTask;


namespace MKTO.Server.ServiceTask
{
    /// <summary>
    /// The service server task class to undertake some specfic business tasks for the Specified Business object group. All methods
    /// can be directly called by other Form Server Tasks or remotely called by Client Tasks through client proxy classes.
    /// </summary>
    /// <history>
    /// #Revision   Date    Author  Description
    /// </history>
    public class MKTOActivity : AbstractApplicationServerTask
    {
        private String host = "CHANGE ME"; //host of your marketo instance, https://AAA-BBB-CCC.mktorest.com
        private String clientId = "CHANGE ME"; //clientId from admin > Launchpoint
        private String clientSecret = "CHANGE ME"; //clientSecret from admin > Launchpoint
        public String nextPageToken;//paging token returned from getPaging token, required
        public int batchSize;//max 300 default 300
        public int listId;//optional static list to search for activities
        public String[] fields;//array of fields names to retrieve changes for, required
        //private Utility utility = new Utility();

        /// <summary>
        /// Get Activity Data based on types
        /// </summary>
        /// <param name="mktoUrl"></param>
        /// <param name="pagingToken"></param>
        /// <param name="getFromDateTime"></param>
        /// <param name="activityTypeIds"></param>
        /// <returns></returns>
        public String getActivityData(string mktoUrl, string pagingToken, DateTime getFromDateTime, string[] activityTypeIds)
        {
            String url = mktoUrl + "/rest/v1/activities.json?access_token=" + pagingToken + "&activityTypeIds=";
            //String url = mktoUrl + "/rest/v1/activities.json?access_token=" + pagingToken + "&activityTypeIds=" + utility.csvString(activityTypeIds)
            //    + "&nextPageToken=" + nextPageToken;
            if (batchSize > 0 && batchSize < 300)
            {
                url += "&batchSize=" + batchSize;
            }
            if (listId > 0)
            {
                url += "&listId=" + listId;
            }
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.ContentType = "application/json";
            request.Accept = "application/json";
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream resStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(resStream);
            return reader.ReadToEnd();
        }
        /// <summary>
        /// Returns an array of lead activity
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="mktoIdentityURL"></param>
        /// <param name="mktoClientId"></param>
        /// <param name="mktoSecret"></param>
        /// <returns></returns>
        public String GetLeadActivities(string[] fields, string mktoIdentityURL, string mktoClientId, string mktoSecret)
        {
            String url = host + "/rest/v1/activities/leadchanges.json?access_token=";
            //String url = host + "/rest/v1/activities/leadchanges.json?access_token=" + utility.GetMarketoToken(mktoIdentityURL, mktoClientId, mktoSecret) + "&fields=" + utility.csvString(fields)
            //    + "&nextPageToken=" + nextPageToken;
            if (batchSize > 0 && batchSize < 300)
            {
                url += "&batchSize=" + batchSize;
            }
            if (listId > 0)
            {
                url += "&listId=" + listId;
            }
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.ContentType = "application/json";
            request.Accept = "application/json";
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            Stream resStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(resStream);
            return reader.ReadToEnd();
        }
    }
}

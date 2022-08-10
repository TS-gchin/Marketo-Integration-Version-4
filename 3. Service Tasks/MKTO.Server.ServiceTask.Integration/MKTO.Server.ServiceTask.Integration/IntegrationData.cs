using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CdcSoftware.Pivotal.Engine;

namespace MKTO.Server.ServiceTask
{
    public class IntegrationData
    {
        public string mktoIdentityURL { get; set; }
        public string mktoRestURL { get; set; }
        public string mktoClientId { get; set; }
        public string mktoClientSecret { get; set; }
        public string mktoOAuthToken { get; set; }
        public Id integrationDtlId { get; set; }
        public Id configurationId { get; set; }
        public string dataDirection { get; set; }
        public DateTime lastRunDateTime { get; set; }
        public string mktoPKFieldName { get; set; }
        public string pivPKFieldName { get; set; }
        public DateTime currentDateTime { get; set; }
        public Dictionary<string, string> fieldMapping { get; set; }
        public string pivObject { get; set; }
        public string mktoObject { get; set; }
        public string pivQuery { get; set; }
        public Id pivRecordId { get; set; }
        public string mktoRecordId { get; set; }
        public bool isMarketoActivity { get; set; }
        public string marketoActivityId { get; set; }
        public string queryName { get; set; }
        public string integrationName {get;set;}
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MKTO.Client.FormTasks
{
    class MarketoConfigurationData
    {
        public struct Table
        {
            public const string TableName = "Marketo_Integration";
        }
        public struct Field
        {
            public const string APIIsValid = "API_Is_Valid";
            public const string APIUserEmail = "API_User_Email";
            public const string APIUserFirstName = "API_User_First_Name";
            public const string APIUserLastName = "API_User_Last_Name";
            public const string MarketoConfigurationId = "Marketo_Configuration_Id";
            public const string PBSServerName = "PBS_Server_Name";
            public const string RESTClientId = "REST_Client_Id";
            public const string RESTEndpoint = "REST_Endpoint";
            public const string RESTIdentity = "REST_Identity";
            public const string RESTSecret = "REST_Secret";
            public const string RESTToken = "REST_Token";
            public const string RnCreateDate = "Rn_Create_Date";
            public const string RnCreateUser = "Rn_Create_User";
            public const string RnDescriptor = "Rn_Descriptor";
            public const string RnEditDate = "Rn_Edit_Date";
            public const string RnEditUser = "Rn_Edit_User";
        }
        public struct FormControl
        {
            public const string TableNameDropDown = "IntegrationObject";
            public const string ChooseColumnDataSegment = "Marketo_Field_Mapping_Marketo_";
            public const string ChooseColumnSegmentName = "MarketoFieldMappingMarketoIntegrationId";
            public const string ContinueButton = "ContinueButton";
            public const string ResultsGroupBox = "ResultsGB";
            public const string RecordsGroupBox = "RecordsGB";
            public const string ResultsContainer = "ResultsContainer";
            public const string RecordsContainer = "RecordsContainer";
        }

        public struct Queries
        {
            public const string GetIntegrationTables = "MKTO: Get Integration Tables";
        }
        public struct MatchType
        {
            public const string Equal = "Equals";
            public const string BeginsWith = "Begins With";
            public const string Contains = "Contains";
        }

        public struct APIURL
        {
            public const string Value = "https://091-enj-691.mktorest.com/rest/v1/";
        }
    }   
}

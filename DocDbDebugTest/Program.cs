using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace DocDbDebugTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var endpointUrl = "https://sjk1-nosql.documents.azure.com:443/";
            var key = "K5OSYWAbNm+NkWD2X2Gd/vW3naqnLFxUctvFGQ0jhmU0gb7FsVgqR6uVnya7rCawqVyB9KzRS829rowvcGzp7w==";

            var test = new TestClass(new DocumentDbInitializer(endpointUrl, key));
            var list = test.GetAllTrafficAsync(Guid.Parse("D3289594-DE02-4C76-8FB1-5C125707C395")).Result;
        }


    }

    public class TestClass
    {
        private readonly IDocumentDbInitializer _initializer;
        public TestClass(IDocumentDbInitializer initializer)
        {
            _initializer = initializer;
        }

        public async Task<List<Traffic>> GetAllTrafficAsync(Guid applicationId)
        {
            var results = new List<Traffic>();
            using (var client = new DocumentDbRepository<Traffic>(_initializer, "Traffic", "App1Traffic"))
            {
                var documentQuery = client.QueryAsync(GetHeaderQuery("application-id", applicationId.ToString())).AsDocumentQuery();
                //var queryAsReadableString = documentQuery.ToString();
                while (documentQuery.HasMoreResults)
                {
                    var pageOfResults = await documentQuery.ExecuteNextAsync<Traffic>();
                    //var rus = pageOfResults.RequestCharge;
                    results.AddRange(pageOfResults);
                }
            }
            return results;
        }

        private string GetHeaderQuery(string headerName, string headerValue)
        {
            return $"select * from c where " + GetHeaderClause(headerName, headerValue);
        }

        private string GetIdByAppIdQuery(string headerName, string headerValue)
        {
            return $"SELECT value c.id FROM c where " + GetHeaderClause(headerName, headerValue);
        }

        private string GetHeaderClause(string headerName, string headerValue)
        {
            return $"c.Headers[\"{headerName}\"] = '{headerValue}'";

        }

    }

    public class Traffic
    {
        public Guid RequestId { get; set; }
        public long ElapsedMs { get; set; }
        public string RequestPath { get; set; }
        public string Method { get; set; }
        public string Ip { get; set; }
        public HttpStatusCode StatusCode { get; set; }
        public int Epoch { get; set; }
        public ExpandoObject Headers { get; set; }

    }
}

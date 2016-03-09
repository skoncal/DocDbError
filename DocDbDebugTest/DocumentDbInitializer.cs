using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

namespace DocDbDebugTest
{
    public class DocumentDbInitializer : IDocumentDbInitializer
    {
        private readonly string _endpointUrl;
        private readonly string _authorizationKey;
        private DocumentClient _client;

        public DocumentDbInitializer(string endpointUrl, string authorizationKey)
        {
            _endpointUrl = endpointUrl;
            _authorizationKey = authorizationKey;
            _client = GetClient(_endpointUrl, _authorizationKey);

        }

        public DocumentClient GetClient()
        {
            if (_client == null)
            {
                _client = GetClient(_endpointUrl, _authorizationKey);
            }
            return _client;
        }

        public DocumentClient GetClient(string endpointUrl, string authorizationKey, ConnectionPolicy connectionPolicy = null)
        {
            if (string.IsNullOrWhiteSpace(endpointUrl))
                throw new ArgumentNullException(nameof(endpointUrl));

            if (string.IsNullOrWhiteSpace(authorizationKey))
                throw new ArgumentNullException(nameof(authorizationKey));

            return new DocumentClient(new Uri(endpointUrl), authorizationKey, connectionPolicy ?? new ConnectionPolicy());
        }
    }
}

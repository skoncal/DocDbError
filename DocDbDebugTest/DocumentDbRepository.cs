using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace DocDbDebugTest
{
    public interface IDocumentDbInitializer
    {
        DocumentClient GetClient(string endpointUrl, string authorizationKey, ConnectionPolicy connectionPolicy = null);
        DocumentClient GetClient();
    }

    public class UnknownEndpointException : Exception
    {
        public UnknownEndpointException(string dbName, string colName) : base(String.Format("Invalid Document DB Endpoint. Db: {0} Col: {1}", dbName, colName)) { }

    }

    public class DocumentDbRepository<T> : IDisposable where T : class
    {
        private readonly DocumentClient _client;
        private readonly string _dbId;
        private readonly string _colllectionId;
        private Database _database;
        private DocumentCollection _collection;

        public DocumentDbRepository(IDocumentDbInitializer initializer, string dbId, string collectionId)
        {
            _dbId = dbId;
            _colllectionId = collectionId;
            _client = initializer.GetClient();
        }

        public async Task<bool> RemoveDocumentAsync(Guid id)
        {
            var result = await _client.DeleteDocumentAsync(GetDocumentUri(id.ToString()));
            var isSuccess = result.StatusCode == HttpStatusCode.NoContent;

            return isSuccess;
        }

        public async Task<T> AddDocumentAsync(T entity)
        {
            var insertedDoc = await _client.CreateDocumentAsync(GetCollectionUri(), entity);
            if (insertedDoc.StatusCode == HttpStatusCode.Created)
            {
                return DeserializeDocument<T>(insertedDoc.Resource);
            }
            throw new Exception("Create failure");
        }

        public async Task<T> ReplaceDocumentAsync(Guid id, T entity)
        {
            var replacedDoc = await _client.ReplaceDocumentAsync(GetDocumentUri(id.ToString()), entity);
            return DeserializeDocument<T>(replacedDoc.Resource);
        }

        public class NoSqlNotExistException : Exception { }

        private T DeserializeDocument<T>(Document doc)
        {
            var entity = JsonConvert.DeserializeObject<T>(doc.ToString());
            return entity;
        }

        private Uri GetCollectionUri()
        {
            var uri = UriFactory.CreateDocumentCollectionUri(_dbId, _colllectionId);
            if (uri.ToString() == "dbs//colls/")
            {
                throw new UnknownEndpointException(_dbId, _colllectionId);
            }
            return uri;
        }

        private Uri GetDocumentUri(string id)
        {
            var uri = UriFactory.CreateDocumentUri(_dbId, _colllectionId, id);
            if (uri.ToString().StartsWith("dbs//colls/"))
            {
                throw new UnknownEndpointException(_dbId, _colllectionId);
            }
            return uri;
        }

        public T FirstOrDefault(Expression<Func<T, bool>> predicate)
        {
            return _client.CreateDocumentQuery<T>(GetCollectionUri())
                .Where(predicate)
                .AsEnumerable()
                .FirstOrDefault();
        }

//        public IQueryable<T> Where(Expression<Func<T, bool>> predicate, PagingOptions pagingOptions = null)
//        {
//            return _client.CreateDocumentQuery<T>(GetCollectionUri(), MapPagingOptions(pagingOptions))
//                .Where(predicate);
//        }
//
//        private FeedOptions MapPagingOptions(PagingOptions pagingOptions)
//        {
//            if (pagingOptions == null)
//            {
//                return null;
//            }
//            else
//            {
//                FeedOptions feedOptions = new FeedOptions()
//                {
//                    MaxItemCount = pagingOptions.MaxItemCount,
//                    RequestContinuation = pagingOptions.RequestContinuation
//                };
//                return feedOptions;
//            }
//        }

        public IQueryable<T> QueryAsync()
        {
            return _client.CreateDocumentQuery<T>(GetCollectionUri());
        }

        public IQueryable<T> QueryAsync(string query)
        {
            return _client.CreateDocumentQuery<T>(GetCollectionUri(), query);
        }

        private async Task<DocumentCollection> GetOrCreateCollectionAsync()
        {
            var collection = _client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(_dbId))
                              .Where(c => c.Id == _colllectionId)
                              .AsEnumerable()
                              .FirstOrDefault();

            if (collection == null)
            {
                var collectionSpec = new DocumentCollection { Id = _colllectionId };
                var requestOptions = new RequestOptions { OfferType = "S1" };

                collection = await _client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(_dbId), collectionSpec, requestOptions);
            }
            return collection;
        }

        public async Task<DocumentCollection> GetCollectionAsync()
        {
            if (_collection == null)
            {
                _collection = await GetOrCreateCollectionAsync();
            }
            return _collection;
        }

        private async Task<Database> GetOrCreateDatabaseAsync()
        {
            Database database = _client.CreateDatabaseQuery()
                .Where(db => db.Id == _dbId).ToArray().FirstOrDefault();

            if (database == null)
            {
                database = await _client.CreateDatabaseAsync(new Database { Id = _dbId });
            }

            return database;
        }

        public async Task<Database> GetDatabaseAsync()
        {
            if (_database == null)
            {
                _database = await GetOrCreateDatabaseAsync();
            }
            return _database;
        }

        public async Task<bool> RemoveDatabaseAsync()
        {
            var database = await GetDatabaseAsync();
            var result = await _client.DeleteDatabaseAsync(database.SelfLink);
            var isSuccess = result.StatusCode == HttpStatusCode.NoContent;
            return isSuccess;
        }

        public async Task<bool> RemoveCollectionAsync()
        {
            var collection = await GetCollectionAsync();
            var result = await _client.DeleteDocumentCollectionAsync(collection.SelfLink);
            var isSuccess = result.StatusCode == HttpStatusCode.NoContent;
            return isSuccess;
        }

        public void Dispose()
        {
            _database = null;
            _collection = null;
        }
    }
}

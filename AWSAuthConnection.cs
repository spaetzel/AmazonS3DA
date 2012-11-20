// This software code is made available "AS IS" without warranties of any        
// kind.  You may copy, display, modify and redistribute the software            
// code either by itself or as incorporated into your code; provided that        
// you do not remove any proprietary notices.  Your use of this software         
// code is at your own risk and you waive any claim against Amazon               
// Digital Services, Inc. or its affiliates with respect to your use of          
// this software code. (c) 2006 Amazon Digital Services, Inc. or its             
// affiliates.          


using System;
using System.Collections;
using System.Net;
using System.Text;
using System.Web;
using System.IO;

namespace com.CastRoller.AmazonS3DA
{
    /// An interface into the S3 system.  It is initially configured with
    /// authentication and connection parameters and exposes methods to access and
    /// manipulate S3 data.
    public class AWSAuthConnection
    {
        private string awsAccessKeyId;
        private string awsSecretAccessKey;
        private bool isSecure;
        private string server;
        private int port;

        public AWSAuthConnection( string awsAccessKeyId, string awsSecretAccessKey ):
            this( awsAccessKeyId, awsSecretAccessKey, true )
        {
        }

        public AWSAuthConnection( string awsAccessKeyId, string awsSecretAccessKey, bool isSecure ):
            this( awsAccessKeyId, awsSecretAccessKey, isSecure, Utils.Host )
        {
        }

        public AWSAuthConnection( string awsAccessKeyId, string awsSecretAccessKey, bool isSecure,
                                  string server ) :
            this( awsAccessKeyId, awsSecretAccessKey, isSecure, server,
                  isSecure ? Utils.SecurePort : Utils.InsecurePort )
        {
        }

        public AWSAuthConnection( string awsAccessKeyId, string awsSecretAccessKey, bool isSecure,
                                  string server, int port )
        {
            this.awsAccessKeyId = awsAccessKeyId;
            this.awsSecretAccessKey = awsSecretAccessKey;
            this.isSecure = isSecure;
            this.server = server;
            this.port = port;
        }

        /// <summary>
        /// Creates a new bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to create</param>
        /// <param name="headers">A Map of string to string representing the headers to pass (can be null)</param>
        public Response createBucket( string bucket, SortedList headers )
        {
            S3Object obj = new S3Object("", null);
            WebRequest request = makeRequest("PUT", bucket, headers, obj);
            request.ContentLength = 0;
            request.GetRequestStream().Close();
            return new Response(request);
        }

        /// <summary>
        /// Lists the contents of a bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to list</param>
        /// <param name="prefix">All returned keys will start with this string (can be null)</param>
        /// <param name="marker">All returned keys will be lexographically greater than this string (can be null)</param>
        /// <param name="maxKeys">The maximum number of keys to return (can be 0)</param>
        /// <param name="headers">A Map of string to string representing HTTP headers to pass.</param>
        public ListBucketResponse listBucket( string bucket, string prefix, string marker,
                                              int maxKeys, SortedList headers ) {
            return listBucket( bucket, prefix, marker, maxKeys, null, headers );
        }

        /// <summary>
        /// Lists the contents of a bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to list</param>
        /// <param name="prefix">All returned keys will start with this string (can be null)</param>
        /// <param name="marker">All returned keys will be lexographically greater than this string (can be null)</param>
        /// <param name="maxKeys">The maximum number of keys to return (can be 0)</param>
        /// <param name="headers">A Map of string to string representing HTTP headers to pass.</param>
        /// <param name="delimiter">Keys that contain a string between the prefix and the first
        /// occurrence of the delimiter will be rolled up into a single element.</param>
        public ListBucketResponse listBucket( string bucket, string prefix, string marker,
                                              int maxKeys, string delimiter, SortedList headers ) {
            StringBuilder path = new StringBuilder( bucket );
            path.Append( "?" );
            if (prefix != null) path.Append("prefix=").Append(HttpUtility.UrlEncode(prefix)).Append("&");
            if ( marker != null ) path.Append( "marker=" ).Append(HttpUtility.UrlEncode(marker)).Append( "&" );
            if ( maxKeys != 0 ) path.Append( "max-keys=" ).Append(maxKeys).Append( "&" );
            if (delimiter != null) path.Append("delimiter=").Append(HttpUtility.UrlEncode(delimiter)).Append("&");
            // we've always added exactly one too many chars.
            path.Remove( path.Length - 1, 1 );

            return new ListBucketResponse( makeRequest( "GET", path.ToString(), headers ) );
        }

        /// <summary>
        /// Deletes an empty Bucket.
        /// </summary>
        /// <param name="bucket">The name of the bucket to delete</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        /// <returns></returns>
        public Response deleteBucket( string bucket, SortedList headers )
        {
            return new Response( makeRequest( "DELETE", bucket, headers ) );
        }

        /// <summary>
        /// Writes an object to S3.
        /// </summary>
        /// <param name="bucket">The name of the bucket to which the object will be added.</param>
        /// <param name="key">The name of the key to use</param>
        /// <param name="obj">An S3Object containing the data to write.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public Response put( string bucket, string key, S3Object obj, SortedList headers )
        {
            WebRequest request = makeRequest("PUT", bucket + "/" + encodeKeyForSignature(key), headers, obj);
            request.ContentLength = obj.Bytes.Length;

            request.GetRequestStream().Write(obj.Bytes, 0, obj.Bytes.Length);
            request.GetRequestStream().Close();

            return new Response( request );
        }



		/// <summary>
		/// Writes an object to S3 using streaming.
		/// </summary>
		/// <param name="bucket">The name of the bucket to which the object will be added.</param>
		/// <param name="key">The name of the key to use</param>
		/// <param name="obj">An S3Object containing the data to write.</param>
		/// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
		public Response putStream(string bucket, string key, S3StreamObject obj, SortedList headers) 
		{

			Boolean isEmptyKey = (key == null) || (key.Length == 0)
				|| (key.Trim().Length == 0);
			string pathSep = isEmptyKey ? "" : "/";

			if (key == null)
				key = "";

			/*WebRequest request = makeStreamRequest("PUT", bucket + pathSep
				+ HttpUtility.UrlEncode(key), headers, obj);
            */

            WebRequest request = makeStreamRequest("PUT", bucket + pathSep
                + key, headers, obj);

			// cast WebRequest to a HttpWebRequest to allow for direct streaming of data
			HttpWebRequest hwr = (HttpWebRequest) request;
			hwr.AllowWriteStreamBuffering = false;
			hwr.SendChunked = false;
			hwr.ContentLength = obj.Stream.Length;

			ASCIIEncoding encoding = new ASCIIEncoding();

			byte[] buf = new byte[1024];
			BufferedStream bufferedInput = new BufferedStream(obj.Stream);
			int contentLength = 0;
			int bytesRead = 0;
			while ((bytesRead = bufferedInput.Read(buf, 0, 1024)) > 0)
			{
				contentLength += bytesRead;
				hwr.GetRequestStream().Write( buf, 0, bytesRead );
			}
			hwr.GetRequestStream().Close();

			return new Response( request );
		}

		/// <summary>
		/// Make a new WebRequest
		/// </summary>
		/// <param name="method">The HTTP method to use (GET, PUT, DELETE)</param>
		/// <param name="resource">The resource name (bucket + "/" + key)</param>
		/// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
		/// <param name="obj">S3StreamObject that is to be written (can be null).</param>
		private WebRequest makeStreamRequest( string method, string resource, SortedList headers, S3StreamObject obj )
		{
			string url = makeURL( resource );
			WebRequest req = WebRequest.Create( url );
			req.Timeout = 3600000; // 1 hr
			req.Method = method;

			addHeaders( req, headers );
			if ( obj != null ) addMetadataHeaders( req, obj.Metadata );
			addAuthHeader( req, resource );

			return req;
		}

        // NOTE: The Syste.Net.Uri class does modifications to the URL.
        // For example, if you have two consecutive slashes, it will
        // convert these to a single slash.  This could lead to invalid
        // signatures as best and at worst keys with names you do not
        // care for.
        private static string encodeKeyForSignature(string key)
        {
            
            string output = HttpUtility.UrlEncode(key).Replace("%2f", "/");

            return key;
        }

        /// <summary>
        /// Reads an object from S3
        /// </summary>
        /// <param name="bucket">The name of the bucket where the object lives</param>
        /// <param name="key">The name of the key to use</param>
        /// <param name="headers">A Map of string to string representing the HTTP headers to pass (can be null)</param>
        public GetResponse get( string bucket, string key, SortedList headers )
        {
            return new GetResponse(makeRequest("GET", bucket + "/" + encodeKeyForSignature(key), headers));
        }

        /// <summary>
        /// Delete an object from S3.
        /// </summary>
        /// <param name="bucket">The name of the bucket where the object lives.</param>
        /// <param name="key">The name of the key to use.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        /// <returns></returns>
        public Response delete( string bucket, string key, SortedList headers )
        {
            return new Response(makeRequest("DELETE", bucket + "/" + encodeKeyForSignature(key), headers));
        }

        /// <summary>
        /// Get the logging xml document for a given bucket
        /// </summary>
        /// <param name="bucket">The name of the bucket</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public GetResponse getBucketLogging(string bucket, SortedList headers)
        {
            return new GetResponse(makeRequest("GET", bucket + "?logging", headers));
        }

        /// <summary>
        /// Write a new logging xml document for a given bucket
        /// </summary>
        /// <param name="bucket">The name of the bucket to enable/disable logging on</param>
        /// <param name="loggingXMLDoc">The xml representation of the logging configuration as a String.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public Response putBucketLogging(string bucket, string loggingXMLDoc, SortedList headers)
        {
            S3Object obj = new S3Object(loggingXMLDoc, null);

            WebRequest request = makeRequest("PUT", bucket + "?logging", headers, obj);
            request.ContentLength = loggingXMLDoc.Length;

            request.GetRequestStream().Write(obj.Bytes, 0, obj.Bytes.Length);
            request.GetRequestStream().Close();

            return new Response(request);
        }

        /// <summary>
        /// Get the ACL for a given bucket.
        /// </summary>
        /// <param name="bucket">The the bucket to get the ACL from.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public GetResponse getBucketACL(string bucket, SortedList headers)
        {
            return getACL(bucket, null, headers);
        }

        /// <summary>
        /// Get the ACL for a given object
        /// </summary>
        /// <param name="bucket">The name of the bucket where the object lives</param>
        /// <param name="key">The name of the key to use.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public GetResponse getACL( string bucket, string key, SortedList headers )
        {
            if ( key == null ) key = "";
            return new GetResponse(makeRequest("GET", bucket + "/" + encodeKeyForSignature(key) + "?acl", headers));
        }

        /// <summary>
        /// Write a new ACL for a given bucket
        /// </summary>
        /// <param name="bucket">The name of the bucket to change the ACL.</param>
        /// <param name="aclXMLDoc">An XML representation of the ACL as a string.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public Response putBucketACL(string bucket, string aclXMLDoc, SortedList headers)
        {
            return putACL(bucket, null, aclXMLDoc, headers);
        }

        /// <summary>
        /// Write a new ACL for a given object
        /// </summary>
        /// <param name="bucket">The name of the bucket where the object lives or the
        /// name of the bucket to change the ACL if key is null.</param>
        /// <param name="key">The name of the key to use; can be null.</param>
        /// <param name="aclXMLDoc">An XML representation of the ACL as a string.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public Response putACL(string bucket, string key, string aclXMLDoc, SortedList headers)
        {
            S3Object obj = new S3Object( aclXMLDoc, null );
            if ( key == null ) key = "";

            WebRequest request = makeRequest("PUT", bucket + "/" + encodeKeyForSignature(key) + "?acl", headers, obj);
            request.ContentLength = aclXMLDoc.Length;

            request.GetRequestStream().Write(obj.Bytes, 0, obj.Bytes.Length);
            request.GetRequestStream().Close();

            return new Response(request);
        }

        /// <summary>
        /// List all the buckets created by this account.
        /// </summary>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        public ListAllMyBucketsResponse listAllMyBuckets( SortedList headers )
        {
            return new ListAllMyBucketsResponse(makeRequest("GET", "", headers));
        }

        /// <summary>
        /// Make a new WebRequest without an S3Object.
        /// </summary>
        private WebRequest makeRequest( string method, string resource, SortedList headers )
        {
            return makeRequest( method, resource, headers, null );
        }

        /// <summary>
        /// Make a new WebRequest
        /// </summary>
        /// <param name="method">The HTTP method to use (GET, PUT, DELETE)</param>
        /// <param name="resource">The resource name (bucket + "/" + key)</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        /// <param name="obj">S3Object that is to be written (can be null).</param>
        private WebRequest makeRequest( string method, string resource, SortedList headers, S3Object obj )
        {
            string url = makeURL( resource );
            WebRequest req = WebRequest.Create( url );
            if (req is HttpWebRequest)
            {
                HttpWebRequest httpReq = req as HttpWebRequest;
                httpReq.AllowWriteStreamBuffering = false;
            }
            req.Method = method;

            addHeaders( req, headers );
            if ( obj != null ) addMetadataHeaders( req, obj.Metadata );
            addAuthHeader( req, resource );

            return req;
        }

        /// <summary>
        /// Add the given headers to the WebRequest
        /// </summary>
        /// <param name="req">Web request to add the headers to.</param>
        /// <param name="headers">A map of string to string representing the HTTP headers to pass (can be null)</param>
        private void addHeaders( WebRequest req, SortedList headers )
        {
            addHeaders( req, headers, "" );
        }

        /// <summary>
        /// Add the given metadata fields to the WebRequest.
        /// </summary>
        /// <param name="req">Web request to add the headers to.</param>
        /// <param name="metadata">A map of string to string representing the S3 metadata for this resource.</param>
        private void addMetadataHeaders( WebRequest req, SortedList metadata )
        {
            addHeaders( req, metadata, Utils.METADATA_PREFIX );
        }

        /// <summary>
        /// Add the given headers to the WebRequest with a prefix before the keys.
        /// </summary>
        /// <param name="req">WebRequest to add the headers to.</param>
        /// <param name="headers">Headers to add.</param>
        /// <param name="prefix">String to prepend to each before ebfore adding it to the WebRequest</param>
        private void addHeaders( WebRequest req, SortedList headers, string prefix )
        {
            if ( headers != null )
            {
                foreach ( string key in headers.Keys )
                {
                    if (prefix.Length == 0 && key.Equals("Content-Type"))
                    {
                        req.ContentType = headers[key] as string;
                    }
                    else
                    {
                        req.Headers.Add(prefix + key, headers[key] as string);
                    }
                }
            }
        }

        /// <summary>
        /// Add the appropriate Authorization header to the WebRequest
        /// </summary>
        /// <param name="request">Request to add the header to</param>
        /// <param name="resource">The resource name (bucketName + "/" + key)</param>
        private void addAuthHeader( WebRequest request, string resource )
        {
            if ( request.Headers[Utils.ALTERNATIVE_DATE_HEADER] == null )
            {
                request.Headers.Add(Utils.ALTERNATIVE_DATE_HEADER, Utils.getHttpDate());
            }

            string canonicalString = Utils.makeCanonicalString( resource, request );
            string encodedCanonical = Utils.encode( awsSecretAccessKey, canonicalString, false );
            request.Headers.Add( "Authorization", "AWS " + awsAccessKeyId + ":" + encodedCanonical );
        }

        /// <summary>
        /// Create a new URL object for the given resource.
        /// </summary>
        /// <param name="resource">The resource name (bucketName + "/" + key)</param>
        private string makeURL( string resource )
        {
            StringBuilder url = new StringBuilder();
            url.Append( isSecure ? "https" : "http" ).Append( "://" );
            url.Append( server ).Append( ":" ).Append( port ).Append( "/" );
            url.Append( resource );
            return url.ToString();
        }
    }
}
